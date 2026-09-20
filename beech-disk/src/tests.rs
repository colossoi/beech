use super::*;
use std::{
    fs,
    io::{self, BufRead, Write},
};
#[derive(Debug, PartialEq, Eq)]
struct Number(u64);
impl Number {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        writer.write_all(&self.0.to_le_bytes())
    }
    fn read(reader: &mut dyn BufRead) -> io::Result<Option<Self>> {
        if reader.fill_buf()?.is_empty() {
            return Ok(None);
        }
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Some(Self(u64::from_le_bytes(bytes))))
    }
    fn memory_size(&self) -> usize {
        8
    }
}
#[test]
fn external_sort_merges_many_runs_with_small_fan_in() {
    let workspace = Workspace::new().unwrap();
    for memory_bytes in [1, 40, 100_000] {
        let mut sort = ExternalSort::new(
            &workspace,
            SortLimits::new(memory_bytes, 3).unwrap(),
            |a: &Number, b: &Number| a.0.cmp(&b.0),
            Number::write,
            Number::read,
            Number::memory_size,
        );
        let input: Vec<_> = (0..700).map(|i| (i * 137) % 233).collect();
        for &n in &input {
            sort.push(Number(n)).unwrap();
        }
        let mut output = sort.finish().unwrap();
        let actual: Vec<_> = output.reader().unwrap().map(|r| r.unwrap().0).collect();
        let mut expected = input;
        expected.sort();
        assert_eq!(actual, expected);
    }
}
#[test]
#[cfg(unix)]
fn publication_is_complete_and_never_clobbers_immutable_files() {
    let workspace = Workspace::new().unwrap();
    let path = workspace.path().join("object");
    atomic_write(&path, |f| f.write_all(b"original")).unwrap();
    assert_eq!(
        atomic_write(&path, |f| f.write_all(b"other")).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(&path).unwrap(), b"original");
    let failed = workspace.path().join("failed");
    assert!(
        atomic_write(&failed, |f| {
            f.write_all(b"partial")?;
            Err(io::Error::other("injected"))
        })
        .is_err()
    );
    assert!(!failed.exists());
    assert_eq!(fs::read_dir(workspace.path()).unwrap().count(), 1);
    atomic_replace(&path, b"replacement").unwrap();
    assert_eq!(fs::read(path).unwrap(), b"replacement");
}
#[test]
fn spool_returns_independent_buffered_readers() {
    let workspace = Workspace::new().unwrap();
    let path = workspace.path().to_owned();
    let mut spool = Spool::new(&workspace).unwrap();
    spool.append(|writer| Number(7).write(writer)).unwrap();
    spool.append(|writer| Number(9).write(writer)).unwrap();
    let mut first = spool.reader().unwrap();
    let mut second = spool.reader().unwrap();
    assert_eq!(Number::read(&mut first).unwrap(), Some(Number(7)));
    assert_eq!(Number::read(&mut first).unwrap(), Some(Number(9)));
    assert_eq!(Number::read(&mut second).unwrap(), Some(Number(7)));
    assert_eq!(Number::read(&mut first).unwrap(), None);
    drop((first, second, spool, workspace));
    assert!(!path.exists());
}

#[test]
fn failed_spool_write_cannot_be_read_as_a_successful_prefix() {
    let workspace = Workspace::new().unwrap();
    let mut spool = Spool::new(&workspace).unwrap();
    spool.append(|writer| Number(1).write(writer)).unwrap();
    assert!(
        spool
            .append(|writer| {
                writer.write_all(b"partial")?;
                Err(io::Error::other("injected"))
            })
            .is_err()
    );
    assert!(spool.reader().is_err());
    assert!(spool.append(|writer| Number(2).write(writer)).is_err());
}

#[test]
fn sort_live_records_remain_bounded_as_input_grows() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    struct Tracked(u64);
    impl Tracked {
        fn new(value: u64) -> Self {
            let live = LIVE.fetch_add(1, Ordering::SeqCst) + 1;
            PEAK.fetch_max(live, Ordering::SeqCst);
            Self(value)
        }
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            LIVE.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl Tracked {
        fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
            Number(self.0).write(writer)
        }
        fn read(reader: &mut dyn BufRead) -> io::Result<Option<Self>> {
            Ok(Number::read(reader)?.map(|n| Self::new(n.0)))
        }
        fn memory_size(&self) -> usize {
            8
        }
    }
    let workspace = Workspace::new().unwrap();
    let path = workspace.path().to_owned();
    let mut sort = ExternalSort::new(
        &workspace,
        SortLimits::new(128, 3).unwrap(),
        |a: &Tracked, b: &Tracked| a.0.cmp(&b.0),
        Tracked::write,
        Tracked::read,
        Tracked::memory_size,
    );
    for i in (0..5000).rev() {
        sort.push(Tracked::new(i)).unwrap();
        assert!(
            fs::read_dir(&path).unwrap().count() < 30,
            "run bookkeeping grew with input"
        );
    }
    let mut output = sort.finish().unwrap();
    for (expected, row) in output.reader().unwrap().enumerate() {
        assert_eq!(row.unwrap().0, expected as u64);
    }
    assert!(PEAK.load(Ordering::SeqCst) <= 17);
    assert_eq!(LIVE.load(Ordering::SeqCst), 0);
    drop(output);
    drop(workspace);
    assert!(!path.exists());
}

#[test]
fn heap_merge_is_lazy_handles_empty_inputs_and_removes_exhausted_inputs() {
    use std::{cell::Cell, rc::Rc};
    struct Input {
        values: std::vec::IntoIter<io::Result<i32>>,
        ended: bool,
        calls: Rc<Cell<usize>>,
    }
    impl Iterator for Input {
        type Item = io::Result<i32>;
        fn next(&mut self) -> Option<Self::Item> {
            assert!(!self.ended, "polled an exhausted input");
            self.calls.set(self.calls.get() + 1);
            let value = self.values.next();
            self.ended = value.is_none();
            value
        }
    }
    let calls = Rc::new(Cell::new(0));
    let inputs = [vec![], vec![1, 3, 3], vec![2, 4]].map(|values| Input {
        values: values.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
        ended: false,
        calls: calls.clone(),
    });
    let mut merged = IterMerger::new(inputs.into(), i32::cmp).unwrap();
    assert_eq!(calls.get(), 3); // Only heads are loaded.
    assert_eq!(merged.next().unwrap().unwrap(), 1);
    assert_eq!(calls.get(), 3); // Refill waits until the next requested output.
    assert_eq!(
        merged.by_ref().collect::<io::Result<Vec<_>>>().unwrap(),
        vec![2, 3, 3, 4]
    );
    assert!(merged.next().is_none());
    assert!(merged.next().is_none());
}

#[test]
fn heap_merge_reports_initial_and_later_io_errors_without_losing_prior_output() {
    assert!(IterMerger::new(vec![vec![Err(io::Error::other("initial"))].into_iter()], i32::cmp).is_err());
    let inputs = vec![
        vec![Ok(1), Err(io::Error::other("later")), Ok(3)].into_iter(),
        vec![Ok(2)].into_iter(),
    ];
    let mut merged = IterMerger::new(inputs, i32::cmp).unwrap();
    assert_eq!(merged.next().unwrap().unwrap(), 1);
    assert_eq!(merged.next().unwrap().unwrap_err().to_string(), "later");
    assert!(merged.next().is_none());
    assert!(merged.next().is_none());
}

#[test]
fn final_merge_does_not_create_an_output_file_and_can_be_reopened() {
    let workspace = Workspace::new().unwrap();
    let mut sort = ExternalSort::new(
        &workspace,
        SortLimits::new(8, 8).unwrap(),
        |a: &Number, b: &Number| a.0.cmp(&b.0),
        Number::write,
        Number::read,
        Number::memory_size,
    );
    for n in [3, 1, 2] {
        sort.push(Number(n)).unwrap();
    }
    let mut runs = sort.finish().unwrap();
    assert_eq!(runs.len(), 3);
    assert_eq!(fs::read_dir(workspace.path()).unwrap().count(), 3);
    for _ in 0..2 {
        assert_eq!(
            runs.reader().unwrap().map(|n| n.unwrap().0).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(fs::read_dir(workspace.path()).unwrap().count(), 3);
    }
    drop(runs);
    assert_eq!(fs::read_dir(workspace.path()).unwrap().count(), 0);
}

#[test]
fn sort_limits_reject_invalid_resource_settings() {
    assert_eq!(
        SortLimits::new(0, 2).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for inputs in [0, 1] {
        assert_eq!(
            SortLimits::new(1024, inputs).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    let limits = SortLimits::new(1024, 4).unwrap();
    assert_eq!(limits.chunk_bytes(), 1024);
    assert_eq!(limits.max_merge_inputs(), 4);
}

#[test]
#[cfg(unix)]
fn directory_sync_propagates_io_errors() {
    let workspace = Workspace::new().unwrap();
    sync_directory(workspace.path()).unwrap();
    assert_eq!(
        sync_directory(&workspace.path().join("missing")).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
}

#[test]
#[cfg(not(unix))]
fn unsupported_directory_sync_prevents_publication() {
    let workspace = Workspace::new().unwrap();
    assert_eq!(
        sync_directory(workspace.path()).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    let path = workspace.path().join("object");
    assert_eq!(
        atomic_write(&path, |_| panic!("must not invoke writer")).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert!(!path.exists());
    fs::write(&path, b"original").unwrap();
    assert_eq!(
        atomic_replace(&path, b"replacement").unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(fs::read(&path).unwrap(), b"original");
    assert_eq!(fs::read_dir(workspace.path()).unwrap().count(), 1);
}
