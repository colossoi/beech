use crate::Workspace;
use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Write},
};

/// Temporary append-only byte storage. Callers define the encoding; each successful
/// append counts as one entry. Opening a reader seals the file.
pub struct Spool {
    writer: Option<BufWriter<File>>,
    file: tempfile::TempPath,
    count: u64,
    failed: bool,
    _workspace: Workspace,
}
impl Spool {
    pub fn new(workspace: &Workspace) -> io::Result<Self> {
        let file = workspace.file()?;
        Ok(Self {
            writer: Some(BufWriter::new(file.reopen()?)),
            file: file.into_temp_path(),
            count: 0,
            failed: false,
            _workspace: workspace.clone(),
        })
    }
    pub fn append(&mut self, write: impl FnOnce(&mut dyn Write) -> io::Result<()>) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("spool failed"));
        }
        let writer = self.writer.as_mut().ok_or_else(|| io::Error::other("spool is sealed"))?;
        let result = write(writer);
        if result.is_err() {
            self.failed = true;
        }
        result?;
        self.count += 1;
        Ok(())
    }
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn seal(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("spool failed"));
        }
        if let Some(mut writer) = self.writer.take()
            && let Err(error) = writer.flush()
        {
            self.failed = true;
            return Err(error);
        }
        Ok(())
    }
    /// Open an independent buffered byte reader. Keep this spool or its workspace
    /// alive while using the reader if the temporary directory must remain present.
    pub fn reader(&mut self) -> io::Result<BufReader<File>> {
        self.seal()?;
        Ok(BufReader::new(File::open(&self.file)?))
    }
    pub(crate) fn workspace(&self) -> Workspace {
        self._workspace.clone()
    }
}
