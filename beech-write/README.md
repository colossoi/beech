# beech-write

The writer has three separate operations:

- `build_table(sink, name, schema, rows, options)` validates and sorts logical
  `Row` values and builds a tree. It stages Parquet leaves and Thrift internal
  nodes through `ObjectSink`, returning a `Table`. Empty tables are supported.
- `apply_changes(changes, table, source, sink, options)` applies insert,
  update, and delete operations and returns the updated `Table`, including its
  row-ID high-water mark. Changes are externally sorted; repeated keys are rejected.
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

`Transaction::push` validates and spools incoming mutations to disk. Its `build`
and `apply` methods externally sort them using `beech-disk`; `build_table` and
`apply_changes` are iterator conveniences over this same transaction path.
The final heap merge streams directly into validation and tree processing;
validation reopens the runs rather than materializing another sorted file.
`SortLimits` controls chunk memory and merge fan-in (8 MiB and 32 by default).

Updates read one sorted change ahead, visit affected paths, and spool merged rows
and replacement node references. Large appends do not accumulate a whole merged
leaf or a whole tree level in RAM. Encoding holds one output leaf; branch building
holds two child groups to absorb a final singleton. Probabilistic splitting has a
hard cutoff at four times the logical target, plus one record (branches still need
at least two children). No-op updates preserve the original root and stage no
objects, though they use temporary scratch files.

Memory includes sort chunks or merge heads, one decoded input leaf/batch, output
encoding buffers, active ancestor references, and the repository cache. The sort
budget is not a process-wide memory limit; individual large rows/keys and existing
large leaves still matter. Scratch spools provide bounded transaction processing,
not crash recovery or resumable transactions.

The default repository budgets are 16 MiB of metadata and 128 MiB of column
cache, in addition to the default 8 MiB sort chunk or up to 32 merge heads.
Active readers can retain evicted cache entries. The update traversal retains
one decoded branch per ancestor; output references go to spools. Sort-run
bookkeeping grows logarithmically with input size. Snapshot publication also
holds the table-name/ID map in memory, proportional to the number of tables.
These are accounting budgets, not a hard RSS ceiling: encoding can hold rows,
Arrow arrays, and Parquet bytes simultaneously.

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
Staging syncs each object. Commit walks the staging directory (no in-memory ID
set), links files into place without overwriting existing objects,
and atomically replaces the text `root` pointer last. Existing objects are reused
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
