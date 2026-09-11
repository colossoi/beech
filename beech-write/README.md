# beech-write

The writer has three separate operations:

- `build_table(sink, name, schema, rows, options)` validates and sorts logical
  `Row` values and builds a tree. It stages Parquet leaves and Thrift internal
  nodes through `ObjectSink`, returning a `Table`. Empty tables are supported.
- `rebuild_with_changes(changes, table, source, sink, options)` applies insert,
  update, and delete operations and returns the new `Option<NodeRef>`. Changes
  must have strictly increasing unique keys. Keys must match their rows; missing
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
`BuildOptions` measures logical bytes, not compressed file size. Branch groups
have at least two children so even very small targets terminate.

Both construction and updates currently materialize rows in memory. Updates
rebuild the entire tree; they are not incremental copy-on-write. Deterministic
encoding allows unchanged objects to be reused by identity. Row IDs are supplied
by callers, preserved by the builder, and distinct from keys; callers needing
unique row IDs must allocate them accordingly.

## Filesystem publication

`FileWriter::new(directory)` acquires an exclusive OS file lock. Acquire it
**before reading the current snapshot** to avoid lost updates between cooperating
writers. Readers do not acquire a lock. The lock releases on drop, including
process exit; the `.beech-write.lock` file remains in the directory.

Objects are staged in a private temporary directory on the same filesystem.
Commit syncs files, links them into place without overwriting existing objects,
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
and file locks require filesystem support; non-Unix directory durability is
platform-dependent. Writers that bypass the lock are outside this coordination
protocol.

Run `cargo test -p beech-write -p beech-test-fixtures --locked` for writer,
publication, merge, and cursor integration tests.
