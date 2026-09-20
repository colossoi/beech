//! Portable transaction scratch space, atomic publication, and external sorting.
mod merge;
pub use merge::IterMerger;
mod records;
mod sort;
pub use records::Spool;
pub use sort::{ExternalSort, SortLimits, SortedRuns};
use std::{
    fs::{self, File},
    io::{self, Write},
    path::Path,
    sync::Arc,
};
pub use tempfile::{NamedTempFile, TempDir};

/// Scratch files are removed when their last owner is dropped, including on error.
#[derive(Clone)]
pub struct Workspace(Arc<TempDir>);
impl Workspace {
    pub fn new() -> io::Result<Self> {
        Ok(Self(Arc::new(tempfile::tempdir()?)))
    }
    pub fn in_directory(directory: &Path) -> io::Result<Self> {
        Ok(Self(Arc::new(
            tempfile::Builder::new().prefix(".beech-stage-").tempdir_in(directory)?,
        )))
    }
    pub fn close(self) -> io::Result<()> {
        Arc::try_unwrap(self.0).map_err(|_| io::Error::other("scratch workspace still in use"))?.close()
    }
    pub fn path(&self) -> &Path {
        self.0.path()
    }
    pub fn file(&self) -> io::Result<NamedTempFile> {
        NamedTempFile::new_in(self.path())
    }
}

/// Write and sync a temporary file, then atomically install it with a hard link.
/// Returns AlreadyExists without touching the destination. Both names are on
/// the same filesystem. The temporary name is removed before the directory sync.
/// Platforms without directory-sync support fail before writing.
/// Cleanup or sync errors after linking can mean the destination was installed;
/// inspect it before retrying. Failed publication retains automatic cleanup.
pub fn atomic_write(path: &Path, write: impl FnOnce(&mut File) -> io::Result<()>) -> io::Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    #[cfg(not(unix))]
    sync_directory(parent)?;
    let mut file = NamedTempFile::new_in(parent)?;
    write(file.as_file_mut())?;
    sync_for_publication(&file)?;
    fs::hard_link(file.path(), path)?;
    let cleanup = file.close();
    sync_directory(parent)?;
    cleanup
}

/// Install an already synced file without replacing an existing destination.
/// Both paths must be on the same filesystem; sync the destination directory
/// after installing a batch and before publishing its pointer.
pub fn install_file(staged: &Path, destination: &Path) -> io::Result<()> {
    fs::hard_link(staged, destination)
}

/// Atomically replace a pointer file after its referenced objects are installed.
/// Platforms without directory-sync support fail before writing.
/// A sync error after replacement is an ambiguous commit: inspect the pointer.
pub fn atomic_replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    #[cfg(not(unix))]
    sync_directory(parent)?;
    let mut file = NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    sync_for_publication(&file)?;
    #[cfg(not(windows))]
    file.persist(path).map_err(|e| e.error)?;
    #[cfg(windows)]
    replace_windows(file.path(), path)?;
    sync_directory(parent)
}

fn sync_for_publication(file: &NamedTempFile) -> io::Result<()> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_NORMAL, SetFileAttributesW};
        let path = windows_path(file.path())?;
        // SAFETY: path is a live NUL-terminated buffer. Clear tempfile's temporary
        // attribute before syncing and publishing, as tempfile::persist does.
        if unsafe { SetFileAttributesW(path.as_ptr(), FILE_ATTRIBUTE_NORMAL) } == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    file.as_file().sync_all()
}

#[cfg(windows)]
fn windows_path(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    let mut text: Vec<_> = path.as_os_str().encode_wide().collect();
    if text.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"));
    }
    text.push(0);
    Ok(text)
}

#[cfg(windows)]
fn replace_windows(source: &Path, destination: &Path) -> io::Result<()> {
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let source = windows_path(source)?;
    let destination = windows_path(destination)?;
    // SAFETY: Both buffers are NUL-terminated and live throughout the call.
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

/// Sync directory entries. Returns `Unsupported` on platforms without an
/// implementation; success always means the directory sync completed.
pub fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "directory sync is not implemented on this platform",
        ))
    }
}

/// An exclusive advisory lock; the file must never be unlinked while in use.
pub fn lock(path: &Path) -> io::Result<File> {
    let file = fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
    file.try_lock().map_err(io::Error::other)?;
    Ok(file)
}
/// Read at an explicit offset without sharing an iterator's logical cursor.
/// Supports Unix and Windows; other platforms return Unsupported.
pub fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_at(file, bytes, offset)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::FileExt::seek_read(file, bytes, offset)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, bytes, offset);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "positional file reads require Unix or Windows",
        ))
    }
}

#[cfg(test)]
mod tests;
