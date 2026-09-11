use crate::{BeechError, Id, Result};
use bytes::Bytes;
use std::{
    fs::File,
    io::{self, Read},
    path::PathBuf,
    sync::Arc,
};

#[derive(Clone, Debug)]
enum Data {
    Bytes(Bytes),
    File(Arc<File>, u64),
}

/// An immutable object with byte-range access, independent of its stored format.
/// Clones share bytes or a File; they do not duplicate OS handles.
#[derive(Clone, Debug)]
pub struct ObjectFile {
    data: Data,
}
impl ObjectFile {
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self {
            data: Data::Bytes(bytes),
        }
    }
    pub fn from_file(file: File) -> Result<Self> {
        let size = file.metadata()?.len();
        Ok(Self {
            data: Data::File(Arc::new(file), size),
        })
    }
    pub fn len(&self) -> u64 {
        match &self.data {
            Data::Bytes(b) => b.len() as u64,
            Data::File(_, n) => *n,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn read_range(&self, start: u64, length: usize) -> io::Result<Bytes> {
        if start.checked_add(length as u64).is_none_or(|end| end > self.len()) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("object range {start} + {length} exceeds length {}", self.len()),
            ));
        }
        match &self.data {
            Data::Bytes(b) => Ok(b.slice(start as usize..start as usize + length)),
            Data::File(_, _) => {
                let mut bytes = vec![0; length];
                self.reader(start)?.read_exact(&mut bytes)?;
                Ok(bytes.into())
            }
        }
    }
    pub(crate) fn read_all(&self) -> Result<Bytes> {
        let len = usize::try_from(self.len())
            .map_err(|_| io::Error::other("object length exceeds addressable memory"))?;
        Ok(self.read_range(0, len)?)
    }
    pub(crate) fn reader(&self, start: u64) -> io::Result<ObjectReader> {
        if start > self.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read beyond object"));
        }
        Ok(ObjectReader {
            object: self.clone(),
            position: start,
        })
    }
}

/// Only an iterator's logical offset is mutable. File reads always supply that
/// offset, so shared files need neither a cursor mutex nor duplicated OS handles.
pub(crate) struct ObjectReader {
    object: ObjectFile,
    position: u64,
}
impl Read for ObjectReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = match &self.object.data {
            Data::Bytes(data) => {
                let remaining = &data[self.position as usize..];
                let n = remaining.len().min(bytes.len());
                bytes[..n].copy_from_slice(&remaining[..n]);
                n
            }
            Data::File(file, _) => read_at(file, bytes, self.position)?,
        };
        self.position =
            self.position.checked_add(n as u64).ok_or_else(|| io::Error::other("file offset overflow"))?;
        Ok(n)
    }
}
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
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

pub trait BackingStore: Send + Sync {
    /// Return an immutable object by ID. No format interpretation is required.
    fn get(&self, id: &Id) -> Result<ObjectFile>;
}
impl<T: BackingStore + ?Sized> BackingStore for Arc<T> {
    fn get(&self, id: &Id) -> Result<ObjectFile> {
        (**self).get(id)
    }
}

/// Read-only directory of immutable objects. Root publication belongs to the writer.
pub struct FileStore {
    directory: PathBuf,
}
impl FileStore {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }
    pub fn object_path(&self, id: &Id) -> PathBuf {
        self.directory.join(id.to_string())
    }
}
impl BackingStore for FileStore {
    fn get(&self, id: &Id) -> Result<ObjectFile> {
        let file = File::open(self.object_path(id)).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound { BeechError::NotFound(*id) } else { e.into() }
        })?;
        ObjectFile::from_file(file)
    }
}
