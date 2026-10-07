# beech-write

The writer has three separate operations:

- `build_table(sink, name, schema, rows, options)` validates and sorts logical
  `Row` values and builds a tree. It stages Parquet leaves and Thrift internal
  nodes through `ObjectSink`, returning a `Table`. Empty tables are supported.
- `apply_changes(changes, table, source, sink, options)` applies insert,
  update, and delete operations and returns the updated `Table`, including its
  row-ID high-water mark. Changes run in submission order; repeated keys see
  earlier edits in the same transaction.
  Keys must match their rows; missing
  update/delete keys and duplicate inserts are errors. Empty changes reuse the
  current root without writing anything.
- `publish_table(writer, table, tables, previous_transaction_id)` replaces the
  table's entry in the supplied directory, retaining its other entries. It stages
  the table, transaction, root object, and root pointer, returning all three IDs.
  The caller then commits the writer.

Schemas are explicit `TableSchema` values. CSV inference belongs to the CLI.
Leaves contain physical Arrow batches, including `__beech_row_id`, and exactly
one Parquet row group. All stored IDs come from the codecs. `ProbShaper` chooses
boundaries using canonical logical bytes; its hash is never an object ID.
`BuildOptions` measures logical bytes, not compressed file size. New branch
splits have at least two children so even very small targets terminate.

`Transaction::push` validates and spools incoming mutations to disk. A failed
push discards its workspace and permanently rejects further use. `build` sorts
rows using `beech-disk`, rejects duplicate keys, and streams the result into the
bulk builder. `SortLimits` applies only to bulk creation (4 chunk workers,
8 MiB per chunk and 32 merge inputs by default). Chunk sorting and run writing
are parallel; merge, validation and tree shaping remain sequential. File output
uses the same reusable worker scheduler with a separate pool.

`apply` processes the input spool in submission order. Each mutation finds its
leaf in a private working tree, reads that leaf, applies the change, and reshapes
it using `ProbShaper`. Modified ancestors are reshaped too. Unchanged nodes retain
immutable references; modified nodes use private temporary IDs in a
`beech-disk::PageStore`, backed by a disposable single-process redb database.
Decoded working nodes use an 8 MiB LRU by default. Repeated edits replace the
cached value without encoding or KV writes. Dirty evictions encode the latest
version into redb; clean evictions need no write. Cache misses decode and retain
spilled values. Oversized nodes bypass the cache. `Transaction::with_decoded_budget`
and `WorkingTable::with_decoded_budget` configure this budget; zero disables it.
Temporary writes are immediately readable and non-durable.
The scratch backend also suppresses redb syncs during creation and shutdown.

`WorkingTable` exposes the same mutable tree for callers that need to read
unfinished edits. `apply` changes its private root immediately, `scan` plus
`next_row` streams the current key-ordered view one row at a time, and `finish`
stages only the reachable final nodes, directly from the decoded cache or redb.
Finalization does not flush dirty nodes to redb before encoding final objects.
Its scan retains one active decoded leaf in addition to the working-node LRU. Dropping the working table rolls back its scratch state. SQLite uses this interface for
read-your-writes transactions.

The shared shaper has a cutoff at four times the logical target plus one record;
branches require two children before splitting. Empty nodes are removed and
one-child roots collapse. Identical updates do not rewrite or reshape anything.
After every operation succeeds, finalization encodes only reachable working
nodes, bottom-up, into Parquet leaves and Thrift branches. Intermediate versions
never reach the object sink. Root publication still happens only at writer commit.
On any processing or staging error, discard the transaction and abort the writer;
there is no per-operation rollback. Temporary KV mutations use non-durable
commits; this workspace is not a recovery log.

Updates retain the decoded LRU, current leaf, one branch per active ancestor, shaping groups,
encoding buffers, the redb cache, and the repository caches. Large individual
rows or existing nodes can still be expensive. The default repository budgets are 16 MiB metadata
and 128 MiB columns; active readers may retain evicted entries. Final encoding
can hold rows, Arrow arrays, and Parquet bytes simultaneously. Bulk sorting has
its own chunk/merge budgets and logarithmic run bookkeeping. Snapshot publication
also holds a table-name/ID map proportional to the number of tables. These are
working-set bounds, not a hard process-memory limit. The decoded budget counts
node values and owned vector/string/binary capacities. LRU bookkeeping, allocator
overhead and temporary clones are excluded. Drop discards dirty cached values;
`PageStore::flush` explicitly writes them if a caller needs a scratch checkpoint.

Splitting is local; there is no boundary realignment or sibling rebalancing.
Deletions can leave underfull branches. The resulting tree is valid but its shape
and root ID can depend on update history, rather than matching a fresh build.
Row IDs are supplied by callers, preserved by the builder, and distinct from
keys; callers needing unique row IDs must allocate them accordingly.

## Filesystem publication

`FileWriter::new(directory)` acquires an exclusive OS file lock. Acquire it
**before reading the current snapshot** to avoid lost updates between cooperating
writers. Readers do not acquire a lock. The lock releases on drop, including
process exit; the `.beech-write.lock` file remains in the directory.

Objects are staged in a private temporary directory on the same filesystem.
Staging queues completed object bytes through `beech-disk::FileOutput`. The default
bounded thread pool creates, writes and closes files concurrently, without syncs.
`FileWriter::with_output_options` configures worker/file/byte limits;
`FileWriter::with_output` accepts another output backend for the private workspace.
`put` may block for capacity and may defer worker errors. `stage_root` waits for
all output before validating the root; commit also waits before installing objects.
Abort/drop joins outstanding workers before deleting staging and releasing the
writer lock. Snapshot metadata staging therefore waits for queued output.
Commit
walks the staging directory (no in-memory ID set), syncs each completed object’s
contents, installs files without overwriting existing objects,
and atomically replaces the text `root` pointer last. On macOS, object contents
use plain `fsync`, followed by one full flush of the destination directory for
the batch, with exclusive rename moving the files into their final locations.
Other platforms use hard links. Root-pointer publication retains its separate
durability syncs. Existing objects are reused
by ID using metadata checks, without reading or comparing their contents. The
writer trusts codec-produced IDs; integrity verification belongs to the reader
or an explicit integrity check. Objects use bare 64-digit
hex filenames; `root` contains the **root object's ID**, not the transaction ID.
On Unix, the directory is synced before and after root replacement.

Abort/drop discards staged files. It never deletes previously published objects.
A failed commit may leave unreferenced immutable objects; garbage collection is
not implemented. If a directory sync fails after root replacement, publication
may already have occurred: reopen `root` to resolve the outcome. Atomic rename
and file locks require filesystem support. On non-Unix platforms, writer
creation returns `Unsupported` because directory sync is not implemented. Writers that bypass the lock are outside this coordination
protocol.

Run `cargo test -p beech-write -p beech-test-fixtures --locked` for writer,
publication, merge, and cursor integration tests.

## Transaction statistics and demo

`Transaction::apply_with_stats` returns `(Table, TransactionStats)`. It records
operation/no-op counts, leaf and branch visits, temporary rewrites, local splits,
root collapses, final staged objects/bytes, elapsed apply time, peak logical
scratch payload bytes and cumulative encoded page writes. Visits include repeated
visits and no-ops; they are not distinct-node
counts or physical disk reads. Finalization reads are excluded from visits.
Scratch peak includes the input spool and live encoded temporary nodes, but not
redb page overhead/cache, publication staging, filesystem allocation overhead, or
the existing repository. Encoded writes do not measure physical disk I/O. Staged
bytes count bytes passed to the sink before possible deduplication. Statistics
currently cover ordered updates, not the bulk builder or snapshot publication.

Run `cargo run -p beech-write --example tree_growth -- 48 target/tree-growth-demo.json`.
The demo starts empty and commits one insertion at a time, reopening and checking
every snapshot. Small 128-byte logical targets make splits visible. It prints
costs per insertion and saves JSON plus an interactive HTML fragment for replay.
The temporary repository is removed after the demo; replay data retains all
measured snapshots. Pass `--keep-workspace` to retain the database directory
(including on failure); the demo prints its path. Per-transaction scratch files
are still cleaned up normally. To save just the final state, use
`--export-final /path/to/new-directory`. The destination must not exist. The demo
copies reachable leaves and branches unchanged, then publishes fresh snapshot
metadata without predecessor history. The result is a standalone Beech repository.

Each row is a separate durable commit. Repository bytes in the demo include
historical objects.

For a larger transaction, run:

```sh
cargo run -p beech-write --release --example random_edits
```

This builds and commits 5,000 rows, then submits 1,000 seeded random inserts,
updates, and deletes in one transaction. It uses the default 1,000-byte logical
node target (the growth replay uses 128 bytes to expose splits). It verifies
all final rows against an in-memory reference model and checks that the old
snapshot remains readable. The temporary database is removed after verification.

Scratch bytes written count encoded payloads passed to the KV provider; leaf/branch
writes count logical page replacements. These counters do not measure physical I/O.

The page store receives encoding and decoding callbacks. Finalization directly
produces published Parquet/Thrift objects. Shaping still encodes logical rows/keys
to choose boundaries.
