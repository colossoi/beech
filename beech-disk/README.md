# beech-disk

Filesystem tools independent of Beech's table and object formats:

- `Workspace` owns temporary directories and keeps them alive while scratch files
  or sorted-run readers exist. Normal drop and error unwinding clean up scratch data;
  `close` reports explicit cleanup failures.
- `Workspace::stage_file` installs a complete scratch filename without syncing
  either contents or directory. At publication, `install_file` syncs its contents
  and installs it; sync the destination directory before publishing its root pointer.
- `atomic_write` writes and syncs a temporary file, then installs the destination
  with a hard link. An existing destination produces `AlreadyExists` and is
  never overwritten. `install_file` syncs and links a staged file.
- `atomic_replace` syncs a temporary file and atomically replaces a mutable
  pointer. Use this only after its referenced immutable files are installed.
- `read_at` supplies portable positional reads on Unix and Windows. It was
  extracted from the core object reader, which now uses this shared helper.
- `Spool` provides temporary buffered byte storage with caller-defined encoding.
  Opening a standard `BufReader<File>` seals the file; separate readers have
  independent offsets. The caller retains the spool or workspace during reading.
- `IterMerger` lazily merges sorted, fallible iterators with a comparator and a
  binary heap: O(log(inputs)) selection and one head per input. It adapts the
  repository's original `rust/merge` implementation from commit `cd12aaa`.
  Exhausted inputs are removed; the first I/O error terminates the merge.
- `ExternalSort<T, C>` sorts bounded chunks and merges their heads. The caller
  supplies separate writing, decoding (`BufRead`), memory-accounting, and
  ordering functions. There is no serialization trait or required framing.

`SortLimits` defaults to 4 chunk workers, an 8 MiB per-chunk budget and a merge
fan-in of 32. `with_workers(1)` selects inline serial processing. Completed
runs are closed. Cascading merges retain only O(fan-in × log(chunks)) scratch-file
names, and each merge opens at most fan-in input files plus one output. Equal
records are retained; their relative order is unspecified. `finish` returns
`SortedRuns`; its `reader` streams the final heap merge without writing a final
output file. Readers can be reopened for a validation pass before processing.

Chunk sorting and run encoding/writing execute independently in the shared
`WorkerPool` task scheduler. Completed runs are collected in submission order;
cascading merges and the final lazy merge remain sequential. At most `workers`
submitted chunks and one producer chunk are retained, so the default chunk-record
bound is 40 MiB, plus merge heads; individual oversized records may exceed this. Run receivers are also
bounded by the worker count. Records must be `Send`; comparators must be
`Send + Sync + 'static` because workers own their jobs.

The budget covers the accounted records in a sort chunk, not the process's total
RSS. A single larger record is allowed. Merging retains one decoded head per
input; caller codecs may also allocate serialized record buffers. Allocator overhead and
I/O buffers are additional. Memory therefore depends on the budget, fan-in, and
largest record, rather than the total input size. The memory-accounting function must
include owned allocations.

Publication requires hard-link and atomic-replacement support on the destination
filesystem; staging must be on the same filesystem. Files are synced before
publication. On Unix, temporary-name removal precedes the directory sync.
Directory sync is currently implemented only on Unix. Other platforms return
`Unsupported`; atomic write and replacement reject publication before writing
when directory sync is unsupported. The Windows replacement primitive uses
`MOVEFILE_WRITE_THROUGH` and `MOVEFILE_REPLACE_EXISTING`, but publication remains
unavailable until directory-sync support is implemented.
An error syncing a directory after pointer
replacement is an ambiguous commit; inspect the pointer to determine its state.
Scratch spools are not a crash-recovery log and are not synced per record.

Run `cargo test -p beech-disk --locked`.

`install_files` publishes a directory of staged immutable objects. On macOS it
uses plain `fsync` per object and one full directory sync to flush the batch;
It uses exclusive rename to move staged files into place on macOS; other
platforms keep hard links and per-file durability syncs. Publish the root only after
this function succeeds. Existing regular destination files are trusted by name.

`PageStore` stores encoded mutable scratch pages in a disposable, single-process
redb database. `ScratchStore` exposes provider-neutral byte-key `get`, `put`, and
`delete` operations with immediate read-your-writes; `PageStore::with_store`
accepts alternative providers. `PageStore::with_cache` enables a byte-budgeted
decoded LRU using a caller-supplied allocation-size function. Dirty eviction or
explicit `flush` encodes the latest value; clean eviction skips writes. Cache
hits skip codecs and KV reads, and oversized values bypass retention. Without
cache configuration every write encodes and every read decodes. Each provider
mutation commits a non-durable redb transaction. Its private file backend suppresses sync requests
also during database creation and shutdown; this scratch data is not a recovery log.

Each store owns a unique temporary database file and retains its workspace.
Drop discards dirty decoded values without flushing, closes the database, and
removes its file before releasing the workspace.
Operation errors poison the typed page store. Page statistics count live/peak
encoded payload bytes and cumulative encoded writes, not database file size,
physical I/O, provider cache memory, or codec buffers. Separate counters report
decoded retained bytes, peak bytes, hits, misses and dirty evictions; the decoded
budget excludes LRU bookkeeping and transient clones. Final immutable objects
and filesystem publication do not use this database.

Staging writes directly to its private filename with `create_new`, without an
intermediate temporary name or hard link. Write failure removes the partial file.
Installation into the repository happens only at publication. Cross-filesystem
installation fails without copying.

`FileOutput` is a streaming queue for completed private immutable files. Submission
may block for capacity; `finish` waits until accepted files have been written and
closed, returning deferred errors. Neither submission nor completion syncs files.
The default `ThreadPoolFileOutput` uses four workers, at most twenty queued plus
active files, and eight MiB of queued plus active payloads. A single oversized
file is admitted only with an empty queue. Payloads are copied after capacity is
available; caller-owned buffers and filename metadata are outside the byte limit.
Pending names are deduplicated; completed names are checked on disk so there is
no growing in-memory set of every object ID. Output errors remain sticky. Drop
joins workers before releasing the workspace. This boundary supports future
platform-specific output providers without changing encoders or publication.

`WorkerPool` is a reusable bounded queue used by both chunk sorting and file
output. Its job/count and payload budgets include queued and executing tasks.
Submission can defer payload copies until capacity exists. `finish` waits for
completion; failures and worker panics remain sticky and cancel queued tasks.
Drop joins workers before releasing jobs and their workspace owners. Completed
results kept by callers are outside the queue budget; sorting separately bounds
those results. The two consumers use separate pool instances and budgets.
