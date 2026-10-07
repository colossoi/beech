use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

type Task = Box<dyn FnOnce() -> io::Result<()> + Send + 'static>;
struct Job {
    bytes: usize,
    task: Task,
}
#[derive(Default)]
struct State {
    jobs: VecDeque<Job>,
    outstanding: usize,
    bytes: usize,
    stopped: bool,
    error: Option<(io::ErrorKind, String)>,
}
impl State {
    fn check(&self) -> io::Result<()> {
        match &self.error {
            Some((kind, message)) => Err(io::Error::new(*kind, message.clone())),
            None => Ok(()),
        }
    }
}
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

/// Portable bounded task queue. Limits include queued and executing jobs.
/// One oversized job is admitted when empty. Errors and panics are sticky;
/// remaining queued tasks are discarded after failure. Drop drains and joins.
/// Completed results retained by callers are outside this queue's budget.
pub struct WorkerPool {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
    max_jobs: usize,
    max_bytes: usize,
}
impl WorkerPool {
    pub fn new(name: &str, workers: usize, max_jobs: usize, max_bytes: usize) -> io::Result<Self> {
        if workers == 0 || max_jobs == 0 || max_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker limits must be positive",
            ));
        }
        let mut pool = Self {
            shared: Arc::new(Shared::default()),
            workers: vec![],
            max_jobs,
            max_bytes,
        };
        for index in 0..workers {
            let shared = pool.shared.clone();
            pool.workers.push(
                thread::Builder::new().name(format!("{name}-{index}")).spawn(move || worker(shared))?,
            );
        }
        Ok(pool)
    }
    /// Wait for capacity before constructing the job, allowing callers to defer copies.
    /// The factory runs under the queue lock; it must not reenter this pool.
    pub fn submit_with<F, J>(&mut self, bytes: usize, factory: F) -> io::Result<()>
    where
        F: FnOnce() -> J,
        J: FnOnce() -> io::Result<()> + Send + 'static,
    {
        let mut state = self.shared.state.lock().unwrap();
        loop {
            state.check()?;
            if state.outstanding < self.max_jobs
                && (state.outstanding == 0
                    || (state.bytes <= self.max_bytes && bytes <= self.max_bytes - state.bytes))
            {
                break;
            }
            state = self.shared.changed.wait(state).unwrap();
        }
        let task = Box::new(factory());
        state.outstanding += 1;
        state.bytes += bytes;
        state.jobs.push_back(Job { bytes, task });
        self.shared.changed.notify_all();
        Ok(())
    }
    pub fn finish(&mut self) -> io::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        while state.outstanding != 0 {
            state = self.shared.changed.wait(state).unwrap();
        }
        state.check()
    }
    pub fn check(&self) -> io::Result<()> {
        self.shared.state.lock().unwrap().check()
    }
    #[cfg(test)]
    pub(crate) fn outstanding(&self) -> (usize, usize) {
        let state = self.shared.state.lock().unwrap();
        (state.outstanding, state.bytes)
    }
}
impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shared.state.lock().unwrap().stopped = true;
        self.shared.changed.notify_all();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}
fn worker(shared: Arc<Shared>) {
    loop {
        let (job, skip) = {
            let mut state = shared.state.lock().unwrap();
            while state.jobs.is_empty() && !state.stopped {
                state = shared.changed.wait(state).unwrap();
            }
            let Some(job) = state.jobs.pop_front() else {
                return;
            };
            (job, state.error.is_some())
        };
        let bytes = job.bytes;
        let result = if skip {
            drop(job);
            Ok(())
        } else {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(job.task))
                .unwrap_or_else(|_| Err(io::Error::other("worker task panicked")))
        };
        let mut state = shared.state.lock().unwrap();
        if let Err(error) = result {
            state.error.get_or_insert((error.kind(), error.to_string()));
        }
        state.bytes -= bytes;
        state.outstanding -= 1;
        shared.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tasks_execute_concurrently_with_bounded_payloads() {
        use std::sync::Barrier;
        let barrier = Arc::new(Barrier::new(5));
        let mut pool = WorkerPool::new("test", 4, 4, 16).unwrap();
        for _ in 0..4 {
            let barrier = barrier.clone();
            pool.submit_with(4, || {
                move || {
                    barrier.wait();
                    Ok(())
                }
            })
            .unwrap();
        }
        assert_eq!(pool.outstanding(), (4, 16));
        barrier.wait();
        pool.finish().unwrap();
        assert_eq!(pool.outstanding(), (0, 0));
    }
    #[test]
    fn panic_is_sticky_and_drop_joins() {
        let mut pool = WorkerPool::new("test", 2, 2, 8).unwrap();
        pool.submit_with(4, || || panic!("injected")).unwrap();
        assert!(pool.finish().is_err());
        assert!(pool.submit_with(1, || || Ok(())).is_err());
        assert_eq!(pool.outstanding(), (0, 0));
    }
}
