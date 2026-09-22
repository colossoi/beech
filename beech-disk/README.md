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

`SortLimits` defaults to an 8 MiB chunk budget and a merge fan-in of 32. Completed
runs are closed. Cascading merges retain only O(fan-in × log(chunks)) scratch-file
names, and each merge opens at most fan-in input files plus one output. Equal
records are retained; their relative order is unspecified. `finish` returns
`SortedRuns`; its `reader` streams the final heap merge without writing a final
output file. Readers can be reopened for a validation pass before processing.

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

`PageStore` caches mutable scratch pages within a configurable decoded-value
budget, using the shared `beech-mem::lru::Lru`. Caller-supplied codecs encode
dirty LRU evictions and decode reloads. Writes are made without syncing; clean evictions do not rewrite disk. Oversized pages and a
zero-byte budget bypass the cache. Errors poison the store. Caller-supplied
memory accounting determines resident weights. Cache metadata and active caller/codec buffers are outside the byte budget. Use one store per
workspace `node-*` namespace; it does not cache arbitrary workspace files.

Staging writes directly to its private filename with `create_new`, without an
intermediate temporary name or hard link. Write failure removes the partial file.
Installation into the repository happens only at publication. Cross-filesystem
installation fails without copying.
