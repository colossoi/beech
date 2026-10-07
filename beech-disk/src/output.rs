use crate::{WorkerPool, Workspace};
use std::{
    collections::HashSet,
    io::{self, Write},
    path::{Component, Path},
    sync::{Arc, Mutex},
};

/// Private immutable-file output. Submission copies completed bytes and may
/// block for capacity. A successful submission is not completion or durability.
/// `finish` waits for all accepted files to be written and closed, and reports
/// deferred errors. No operation syncs data. Implementations must drain/cancel
/// and join outstanding work on drop before releasing the destination workspace.
pub trait FileOutput: Send {
    /// Returns false when this filename is already pending or staged.
    /// Reusing names trusts the first contents; existing files are never replaced.
    fn submit(&mut self, name: &str, bytes: &[u8]) -> io::Result<bool>;
    fn finish(&mut self) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug)]
pub struct FileOutputOptions {
    pub workers: usize,
    /// Maximum queued plus active files.
    pub max_files: usize,
    /// Maximum queued plus active payload bytes. One oversized file is allowed
    /// only when no other files are outstanding, so submission can make progress.
    pub max_bytes: usize,
}
impl Default for FileOutputOptions {
    fn default() -> Self {
        Self {
            workers: 4,
            max_files: 20,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}
/// Bounded portable backend sharing task scheduling with external sorting.
pub struct ThreadPoolFileOutput {
    pool: WorkerPool,
    pending: Arc<Mutex<HashSet<String>>>,
    workspace: Workspace,
}
impl ThreadPoolFileOutput {
    pub fn new(workspace: &Workspace, options: FileOutputOptions) -> io::Result<Self> {
        Ok(Self {
            pool: WorkerPool::new(
                "beech-output",
                options.workers,
                options.max_files,
                options.max_bytes,
            )?,
            pending: Arc::new(Mutex::new(HashSet::new())),
            workspace: workspace.clone(),
        })
    }
}
impl FileOutput for ThreadPoolFileOutput {
    fn submit(&mut self, name: &str, bytes: &[u8]) -> io::Result<bool> {
        let mut parts = Path::new(name).components();
        if !matches!(parts.next(), Some(Component::Normal(_))) || parts.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output requires a single filename",
            ));
        }
        self.pool.check()?;
        if self.pending.lock().unwrap().contains(name) {
            return Ok(false);
        }
        match std::fs::metadata(self.workspace.path().join(name)) {
            Ok(meta) if meta.is_file() => return Ok(false),
            Ok(_) => return Err(io::Error::other("output path is not a regular file")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
        let pending = self.pending.clone();
        let workspace = self.workspace.clone();
        self.pool.submit_with(bytes.len(), || {
            let name = name.to_owned();
            let bytes = bytes.to_vec();
            pending.lock().unwrap().insert(name.clone());
            move || {
                let result = workspace.stage_file(&name, |file| file.write_all(&bytes));
                drop(bytes);
                pending.lock().unwrap().remove(&name);
                result.map_err(|e| io::Error::new(e.kind(), format!("output {name}: {e}")))
            }
        })?;
        Ok(true)
    }
    fn finish(&mut self) -> io::Result<()> {
        let result = self.pool.finish();
        self.pending.lock().unwrap().clear();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn batches_deduplicate_bound_and_finish_before_cleanup() {
        let workspace = Workspace::new().unwrap();
        let path = workspace.path().to_owned();
        let mut pool = ThreadPoolFileOutput::new(
            &workspace,
            FileOutputOptions {
                workers: 2,
                max_files: 2,
                max_bytes: 8,
            },
        )
        .unwrap();
        for n in 0..100 {
            let name = n.to_string();
            assert!(pool.submit(&name, &[n; 4]).unwrap());
            assert!(!pool.submit(&name, &[9; 4]).unwrap());
            let (jobs, bytes) = pool.pool.outstanding();
            assert!(jobs <= 2);
            assert!(bytes <= 8);
        }
        assert!(pool.submit("large", &[7; 100]).unwrap());
        pool.finish().unwrap();
        for n in 0..100 {
            assert_eq!(std::fs::read(path.join(n.to_string())).unwrap(), vec![n; 4]);
        }
        assert_eq!(std::fs::read(path.join("large")).unwrap(), vec![7; 100]);
        assert!(pool.submit("empty", &[]).unwrap());
        drop(workspace);
        drop(pool); // joins remaining output before deleting workspace
        assert!(!path.exists());
    }
    #[test]
    fn deferred_error_is_sticky_and_finish_drains() {
        let workspace = Workspace::new().unwrap();
        let mut pool = ThreadPoolFileOutput::new(&workspace, FileOutputOptions::default()).unwrap();
        let target = workspace.clone();
        pool.pool
            .submit_with(1, || {
                move || {
                    std::fs::create_dir(target.path().join("bad"))?;
                    target.stage_file("bad", |file| file.write_all(&[0]))
                }
            })
            .unwrap();
        assert!(pool.finish().is_err());
        assert!(pool.submit("next", &[1]).is_err());
        assert!(pool.finish().is_err());
        assert!(pool.pending.lock().unwrap().is_empty());
    }
    #[test]
    fn rejects_invalid_limits_and_names() {
        let workspace = Workspace::new().unwrap();
        assert!(ThreadPoolFileOutput::new(
            &workspace,
            FileOutputOptions {
                workers: 0,
                ..Default::default()
            }
        )
        .is_err());
        let mut pool = ThreadPoolFileOutput::new(&workspace, FileOutputOptions::default()).unwrap();
        for name in ["", "..", "a/b", "/absolute"] {
            assert!(pool.submit(name, &[]).is_err());
        }
        pool.finish().unwrap();
    }
}
