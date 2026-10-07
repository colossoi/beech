use crate::{IterMerger, Spool, WorkerPool, Workspace};
use std::{
    cmp::Ordering,
    collections::VecDeque,
    io::{self, BufRead, Write},
    sync::{
        mpsc::{self, Receiver},
        Arc,
    },
};

#[derive(Clone, Copy, Debug)]
pub struct SortLimits {
    /// Accounted record memory per sorted chunk. One larger record is allowed.
    chunk_bytes: usize,
    /// Maximum input files/record heads in each merge; at least two.
    max_merge_inputs: usize,
    workers: usize,
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
            workers: 4,
        })
    }
    /// Parallel chunk sort/write workers; one selects the inline serial path.
    /// At most this many submitted chunks plus one producer chunk are retained.
    pub fn with_workers(mut self, workers: usize) -> io::Result<Self> {
        if workers == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sort workers must be positive",
            ));
        }
        self.workers = workers;
        Ok(self)
    }
    pub fn workers(&self) -> usize {
        self.workers
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
            workers: 4,
        }
    }
}

/// Sort bounded chunks and merge their heads. Each merge opens at most fan_in
/// inputs; cascading levels bound run bookkeeping by O(fan_in * log(chunks)).
pub struct ExternalSort<T, C> {
    // Join workers before releasing outstanding run receivers and workspace.
    pool: Option<WorkerPool>,
    pending: VecDeque<Receiver<Spool>>,
    workspace: Workspace,
    options: SortLimits,
    compare: Arc<C>,
    encode: fn(&T, &mut dyn Write) -> io::Result<()>,
    decode: fn(&mut dyn BufRead) -> io::Result<Option<T>>,
    memory_size: fn(&T) -> usize,
    buffer: Vec<T>,
    bytes: usize,
    levels: Vec<Vec<Spool>>,
}
impl<T: Send + 'static, C: Fn(&T, &T) -> Ordering + Send + Sync + 'static> ExternalSort<T, C> {
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
            pool: None,
            pending: VecDeque::new(),
            workspace: workspace.clone(),
            options,
            compare: Arc::new(compare),
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
        let buffer = std::mem::take(&mut self.buffer);
        let bytes = std::mem::take(&mut self.bytes);
        if self.options.workers == 1 {
            let run = sorted_chunk(&self.workspace, buffer, self.compare.as_ref(), self.encode)?;
            return self.add_run(run);
        }
        if self.pool.is_none() {
            self.pool = Some(WorkerPool::new(
                "beech-sort",
                self.options.workers,
                self.options.workers,
                self.options.chunk_bytes.saturating_mul(self.options.workers),
            )?);
        }
        // Bound completed results too; integrate in submission order, independent
        // of worker completion order. Merges remain on the coordinator.
        if self.pending.len() >= self.options.workers {
            self.collect_run()?;
        }
        let compare = self.compare.clone();
        let workspace = self.workspace.clone();
        let encode = self.encode;
        let (send, receive) = mpsc::sync_channel(1);
        self.pool.as_mut().unwrap().submit_with(bytes, || {
            move || {
                let run = sorted_chunk(&workspace, buffer, compare.as_ref(), encode)?;
                send.send(run).map_err(|_| io::Error::other("sort result receiver dropped"))
            }
        })?;
        self.pending.push_back(receive);
        Ok(())
    }
    fn collect_run(&mut self) -> io::Result<()> {
        let receive = self.pending.pop_front().unwrap();
        let run = match receive.recv() {
            Ok(run) => run,
            Err(_) => {
                self.pool.as_ref().unwrap().check()?;
                return Err(io::Error::other("sort worker did not produce a run"));
            }
        };
        self.add_run(run)
    }
    fn add_run(&mut self, mut run: Spool) -> io::Result<()> {
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
            run = merge(
                &self.workspace,
                runs,
                self.compare.as_ref(),
                self.encode,
                self.decode,
            )?;
            level += 1;
        }
        Ok(())
    }
    pub fn finish(mut self) -> io::Result<SortedRuns<T, C>> {
        self.flush()?;
        while !self.pending.is_empty() {
            self.collect_run()?;
        }
        if let Some(mut pool) = self.pool.take() {
            pool.finish()?;
        }
        let mut runs: Vec<_> = self.levels.into_iter().rev().flatten().collect();
        while runs.len() > self.options.max_merge_inputs {
            let count = runs.len().min(self.options.max_merge_inputs);
            let inputs = runs.split_off(runs.len() - count);
            runs.push(merge(
                &self.workspace,
                inputs,
                self.compare.as_ref(),
                self.encode,
                self.decode,
            )?);
        }
        Ok(SortedRuns {
            runs,
            compare: self.compare,
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

fn sorted_chunk<T>(
    workspace: &Workspace,
    mut buffer: Vec<T>,
    compare: &impl Fn(&T, &T) -> Ordering,
    encode: fn(&T, &mut dyn Write) -> io::Result<()>,
) -> io::Result<Spool> {
    buffer.sort_unstable_by(compare);
    let mut run = Spool::new(workspace)?;
    for record in buffer {
        run.append(|writer| encode(&record, writer))?;
    }
    run.seal()?;
    Ok(run)
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

#[cfg(test)]
mod tests {
    use super::*;
    fn read(reader: &mut dyn BufRead) -> io::Result<Option<u64>> {
        let mut bytes = [0; 8];
        if reader.fill_buf()?.is_empty() {
            return Ok(None);
        }
        reader.read_exact(&mut bytes)?;
        Ok(Some(u64::from_le_bytes(bytes)))
    }
    #[test]
    fn encoder_failure_and_worker_panic_cleanup() {
        for panic in [false, true] {
            let workspace = Workspace::new().unwrap();
            let path = workspace.path().to_owned();
            let mut sort = ExternalSort::new(
                &workspace,
                SortLimits::new(8, 2).unwrap(),
                move |a: &u64, b: &u64| {
                    if panic {
                        panic!("injected comparator panic");
                    }
                    a.cmp(b)
                },
                |_, _| Err(io::Error::other("injected run encoding failure")),
                read,
                |_| 8,
            );
            // Two records per chunk exercises comparator panic as well as codec failure.
            sort.options.chunk_bytes = 16;
            let result = sort.push(2).and_then(|_| sort.push(1)).and_then(|_| sort.finish().map(|_| ()));
            assert!(result.is_err());
            drop(workspace);
            assert!(!path.exists());
        }
    }
}
