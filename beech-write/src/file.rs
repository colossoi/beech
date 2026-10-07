use crate::{ObjectSink, Writer};
use beech_core::Id;
use beech_disk::{FileOutput, FileOutputOptions, ThreadPoolFileOutput, Workspace, atomic_replace};
use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

/// Stages immutable objects on disk. Commit installs objects without overwriting
/// them, then atomically replaces root. Failure can leave unreferenced objects;
/// a sync failure after root replacement has an ambiguous publication outcome.
pub struct FileWriter {
    output: Option<Box<dyn FileOutput>>,
    directory: PathBuf,
    staging: Workspace,
    objects: usize,
    root: Option<Id>,
    // Held until staging cleanup has finished. Never unlink the lock file.
    _lock: File,
}
impl FileWriter {
    pub fn new(directory: impl AsRef<Path>) -> io::Result<Self> {
        Self::with_output_options(directory, FileOutputOptions::default())
    }
    pub fn with_output_options(
        directory: impl AsRef<Path>,
        options: FileOutputOptions,
    ) -> io::Result<Self> {
        Self::with_output(directory, |workspace| {
            Ok(Box::new(ThreadPoolFileOutput::new(workspace, options)?))
        })
    }
    /// The backend must target the supplied private workspace and obey FileOutput
    /// completion/drop guarantees. Final durability remains owned by this writer.
    pub fn with_output(
        directory: impl AsRef<Path>,
        make_output: impl FnOnce(&Workspace) -> io::Result<Box<dyn FileOutput>>,
    ) -> io::Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        #[cfg(not(unix))]
        beech_disk::sync_directory(&directory)?;
        let lock = beech_disk::lock(&directory.join(".beech-write.lock"))?;
        let staging = Workspace::in_directory(&directory)?;
        let output = make_output(&staging)?;
        Ok(Self {
            output: Some(output),
            directory,
            staging,
            objects: 0,
            root: None,
            _lock: lock,
        })
    }
}
impl ObjectSink for FileWriter {
    fn put(&mut self, id: Id, bytes: &[u8]) -> io::Result<()> {
        if object_file_exists(&self.directory.join(id.to_string()))? {
            return Ok(());
        }
        if self.output.as_mut().unwrap().submit(&id.to_string(), bytes)? {
            self.objects += 1;
        }
        Ok(())
    }
}
impl Writer for FileWriter {
    fn stage_root(&mut self, root_id: Id) -> io::Result<()> {
        self.output.as_mut().unwrap().finish()?;
        if !object_file_exists(&self.staging.path().join(root_id.to_string()))?
            && !object_file_exists(&self.directory.join(root_id.to_string()))?
        {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "root object is not available",
            ));
        }
        self.root = Some(root_id);
        Ok(())
    }
    fn commit(mut self) -> io::Result<()> {
        self.output.as_mut().unwrap().finish()?;
        drop(self.output.take());
        beech_disk::install_files(self.staging.path(), &self.directory)?;
        if let Some(root) = self.root {
            atomic_replace(&self.directory.join("root"), root.to_string().as_bytes())?;
        }
        Ok(())
    }
    fn abort(mut self) -> io::Result<()> {
        drop(self.output.take());
        self.staging.close()
    }
    fn num_to_commit(&self) -> usize {
        self.objects + usize::from(self.root.is_some())
    }
}

fn object_file_exists(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(m) if m.is_file() => Ok(true),
        Ok(_) => Err(io::Error::other(format!(
            "object path is not a file: {}",
            path.display()
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}
