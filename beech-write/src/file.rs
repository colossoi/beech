use crate::{ObjectSink, Writer};
use beech_core::Id;
use beech_disk::{atomic_replace, sync_directory, Workspace};
use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

/// Stages immutable objects on disk. Commit installs objects without overwriting
/// them, then atomically replaces root. Failure can leave unreferenced objects;
/// a sync failure after root replacement has an ambiguous publication outcome.
pub struct FileWriter {
    directory: PathBuf,
    staging: Workspace,
    objects: usize,
    root: Option<Id>,
    // Held until staging cleanup has finished. Never unlink the lock file.
    _lock: File,
}
impl FileWriter {
    pub fn new(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        #[cfg(not(unix))]
        sync_directory(&directory)?;
        let lock = beech_disk::lock(&directory.join(".beech-write.lock"))?;
        let staging = Workspace::in_directory(&directory)?;
        Ok(Self {
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
        let staged = self.staging.path().join(id.to_string());
        if object_file_exists(&staged)? || object_file_exists(&self.directory.join(id.to_string()))? {
            return Ok(());
        }
        self.staging.stage_file(&id.to_string(), |file| file.write_all(bytes))?;
        self.objects += 1;
        Ok(())
    }
}
impl Writer for FileWriter {
    fn stage_root(&mut self, root_id: Id) -> io::Result<()> {
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
    fn commit(self) -> io::Result<()> {
        // Sync and install completed objects only at publication time.
        // The directory is the object journal; no growing ID set.
        for entry in fs::read_dir(self.staging.path())? {
            let entry = entry?;
            let destination = self.directory.join(entry.file_name());
            match beech_disk::install_file(&entry.path(), &destination) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists && object_file_exists(&destination)? => {
                }
                Err(e) => return Err(e),
            }
        }
        sync_directory(&self.directory)?;
        if let Some(root) = self.root {
            atomic_replace(&self.directory.join("root"), root.to_string().as_bytes())?;
        }
        Ok(())
    }
    fn abort(self) -> io::Result<()> {
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
