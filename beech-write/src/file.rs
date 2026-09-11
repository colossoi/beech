use crate::{ObjectSink, Writer};
use beech_core::Id;
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};
use tempfile::TempDir;

/// A directory publication transaction. Acquire this before reading the current
/// snapshot to prevent lost updates between cooperating writers. Readers need no lock.
///
/// Objects are staged on the same filesystem, synced, then linked without
/// overwriting existing objects. The root is replaced atomically last. A failed
/// commit can leave unreferenced objects, but never deletes published objects.
/// After a root rename, a directory-sync failure is an ambiguous commit outcome:
/// reopen the root to determine whether publication occurred.
pub struct FileWriter {
    directory: PathBuf,
    staging: TempDir,
    objects: BTreeSet<Id>,
    root: Option<Id>,
    // Held until staging cleanup has finished. Never unlink the lock file.
    _lock: File,
}
impl FileWriter {
    pub fn new(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join(".beech-write.lock"))?;
        lock.try_lock().map_err(|e| io::Error::other(format!("cannot acquire Beech writer lock: {e}")))?;
        let staging = tempfile::Builder::new().prefix(".beech-stage-").tempdir_in(&directory)?;
        Ok(Self {
            directory,
            staging,
            objects: BTreeSet::new(),
            root: None,
            _lock: lock,
        })
    }
}
impl ObjectSink for FileWriter {
    fn put(&mut self, id: Id, bytes: &[u8]) -> io::Result<()> {
        // Codec-produced IDs identify immutable contents; reuse without reading bytes.
        if self.objects.contains(&id) || object_exists(&self.directory.join(id.to_string()))? {
            return Ok(());
        }
        let path = self.staging.path().join(id.to_string());
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        self.objects.insert(id);
        Ok(())
    }
}
impl Writer for FileWriter {
    fn stage_root(&mut self, root_id: Id) -> io::Result<()> {
        // The root object must be staged or already committed before publication.
        if !self.objects.contains(&root_id) && !self.directory.join(root_id.to_string()).is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "root object is not available",
            ));
        }
        let mut file = File::create(self.staging.path().join("root"))?;
        file.write_all(root_id.to_string().as_bytes())?;
        file.sync_all()?;
        self.root = Some(root_id);
        Ok(())
    }
    fn commit(self) -> io::Result<()> {
        for id in &self.objects {
            let staged = self.staging.path().join(id.to_string());
            let destination = self.directory.join(id.to_string());
            match fs::hard_link(&staged, &destination) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    if !object_exists(&destination)? {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "existing object disappeared",
                        ));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        sync_directory(&self.directory)?;
        if self.root.is_some() {
            fs::rename(self.staging.path().join("root"), self.directory.join("root"))?;
            sync_directory(&self.directory)?;
        }
        Ok(())
    }
    fn abort(self) -> io::Result<()> {
        self.staging.close()
    }
    fn num_to_commit(&self) -> usize {
        self.objects.len() + usize::from(self.root.is_some())
    }
}
fn object_exists(path: &Path) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(io::Error::other(format!(
            "object path is not a file: {}",
            path.display()
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path; // Root rename remains atomic; directory durability is OS-specific.
    Ok(())
}
