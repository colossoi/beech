use crate::{IterMerger, Spool, Workspace};
use std::{
    cmp::Ordering,
    io::{self, BufRead, Write},
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
pub struct SortLimits {
    /// Accounted record memory per sorted chunk. One larger record is allowed.
    chunk_bytes: usize,
    /// Maximum input files/record heads in each merge; at least two.
    max_merge_inputs: usize,
}
impl SortLimits {
    /// Limit accounted chunk memory and the number of input runs per merge.
    /// This is not a process-wide memory cap; one larger record is allowed.
    pub fn new(chunk_bytes: usize, max_merge_inputs: usize) -> io::Result<Self> {
        if chunk_bytes == 0 || max_merge_inputs < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sort requires positive chunk bytes and at least two merge inputs",
            ));
        }
        Ok(Self {
            chunk_bytes,
            max_merge_inputs,
        })
    }
    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }
    pub fn max_merge_inputs(&self) -> usize {
        self.max_merge_inputs
    }
}
impl Default for SortLimits {
    fn default() -> Self {
        Self {
            chunk_bytes: 8 * 1024 * 1024,
            max_merge_inputs: 32,
        }
    }
}

/// Sort bounded chunks and merge their heads. Each merge opens at most fan_in
/// inputs; cascading levels bound run bookkeeping by O(fan_in * log(chunks)).
pub struct ExternalSort<T, C> {
    workspace: Workspace,
    options: SortLimits,
    compare: C,
    encode: fn(&T, &mut dyn Write) -> io::Result<()>,
    decode: fn(&mut dyn BufRead) -> io::Result<Option<T>>,
    memory_size: fn(&T) -> usize,
    buffer: Vec<T>,
    bytes: usize,
    levels: Vec<Vec<Spool>>,
}
impl<T, C: Fn(&T, &T) -> Ordering> ExternalSort<T, C> {
    /// Supply matching encoding and decoding functions for temporary runs.
    /// Decode one value per call, return `None` only at clean EOF, and report
    /// incomplete values as errors. Memory accounting should include owned allocations.
    pub fn new(
        workspace: &Workspace,
        options: SortLimits,
        compare: C,
        encode: fn(&T, &mut dyn Write) -> io::Result<()>,
        decode: fn(&mut dyn BufRead) -> io::Result<Option<T>>,
        memory_size: fn(&T) -> usize,
    ) -> Self {
        Self {
            workspace: workspace.clone(),
            options,
            compare,
            encode,
            decode,
            memory_size,
            buffer: vec![],
            bytes: 0,
            levels: vec![],
        }
    }
    pub fn push(&mut self, record: T) -> io::Result<()> {
        let size = (self.memory_size)(&record).max(std::mem::size_of::<T>());
        if !self.buffer.is_empty() && size > self.options.chunk_bytes.saturating_sub(self.bytes) {
            self.flush()?;
        }
        self.bytes = self.bytes.saturating_add(size);
        self.buffer.push(record);
        if self.bytes >= self.options.chunk_bytes {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_unstable_by(&self.compare);
        let mut run = Spool::new(&self.workspace)?;
        for record in self.buffer.drain(..) {
            run.append(|writer| (self.encode)(&record, writer))?;
        }
        run.seal()?;
        self.bytes = 0;
        let mut level = 0;
        loop {
            if self.levels.len() == level {
                self.levels.push(vec![]);
            }
            self.levels[level].push(run);
            if self.levels[level].len() < self.options.max_merge_inputs {
                break;
            }
            let runs = std::mem::take(&mut self.levels[level]);
            run = merge(&self.workspace, runs, &self.compare, self.encode, self.decode)?;
            level += 1;
        }
        Ok(())
    }
    pub fn finish(mut self) -> io::Result<SortedRuns<T, C>> {
        self.flush()?;
        let mut runs: Vec<_> = self.levels.into_iter().rev().flatten().collect();
        while runs.len() > self.options.max_merge_inputs {
            let count = runs.len().min(self.options.max_merge_inputs);
            let inputs = runs.split_off(runs.len() - count);
            runs.push(merge(
                &self.workspace,
                inputs,
                &self.compare,
                self.encode,
                self.decode,
            )?);
        }
        Ok(SortedRuns {
            runs,
            compare: Arc::new(self.compare),
            decode: self.decode,
        })
    }
}

/// Completed runs whose final merge is read lazily, without a final output file.
/// Readers can be reopened for validation before processing. At most fan_in
/// files are retained; each reader opens those files with independent offsets.
pub struct SortedRuns<T, C> {
    runs: Vec<Spool>,
    compare: Arc<C>,
    decode: fn(&mut dyn BufRead) -> io::Result<Option<T>>,
}
impl<T, C: Fn(&T, &T) -> Ordering> SortedRuns<T, C> {
    pub fn len(&self) -> u64 {
        self.runs.iter().map(Spool::len).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.runs.iter().all(Spool::is_empty)
    }
    pub fn reader(
        &mut self,
    ) -> io::Result<IterMerger<impl Iterator<Item = io::Result<T>> + use<T, C>, T, C>> {
        let readers =
            self.runs.iter_mut().map(|run| decoded(run, self.decode)).collect::<io::Result<Vec<_>>>()?;
        IterMerger::with_compare(readers, self.compare.clone())
    }
}

fn decoded<T>(
    run: &mut Spool,
    decode: fn(&mut dyn BufRead) -> io::Result<Option<T>>,
) -> io::Result<impl Iterator<Item = io::Result<T>> + use<T>> {
    let mut reader = run.reader()?;
    let workspace = run.workspace();
    let mut ended = false;
    Ok(std::iter::from_fn(move || {
        let _keep_alive = &workspace;
        if ended {
            return None;
        }
        match decode(&mut reader) {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => {
                ended = true;
                None
            }
            Err(error) => {
                ended = true;
                Some(Err(error))
            }
        }
    }))
}

fn merge<T>(
    workspace: &Workspace,
    mut inputs: Vec<Spool>,
    compare: &impl Fn(&T, &T) -> Ordering,
    encode: fn(&T, &mut dyn Write) -> io::Result<()>,
    decode: fn(&mut dyn BufRead) -> io::Result<Option<T>>,
) -> io::Result<Spool> {
    let readers = inputs.iter_mut().map(|run| decoded(run, decode)).collect::<io::Result<Vec<_>>>()?;
    let mut output = Spool::new(workspace)?;
    for record in IterMerger::new(readers, compare)? {
        output.append(|writer| encode(&record?, writer))?;
    }
    output.seal()?;
    Ok(output)
}
