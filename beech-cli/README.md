# beech-cli

Import CSV data and inspect Parquet-backed Beech snapshots:

```sh
cargo run -p beech-cli -- load-csv data.csv -o /tmp/beech-data -t items --key-columns id
cargo run -p beech-cli -- info -d /tmp/beech-data
cargo run -p beech-cli -- inspect -d /tmp/beech-data <node-id>
cargo run -p beech-sqlite3-test -- /tmp/beech-data items
```

`load-csv` defaults to replace mode. It replaces the named table and preserves
all other tables and the previous transaction link. `--mode insert` requires an
existing table, parses CSV against that table's schema, and rejects duplicate
keys. Insert row IDs start above the high-water mark stored in table metadata,
including deleted rows; replace starts at zero. Allocation requires no row scan.
Insert updates only affected leaves and their ancestor paths.

Key columns can be comma-separated names or zero-based indexes. Their order is
significant. For headerless CSV, pass `--has-headers false`; column names become
`col_0`, `col_1`, etc. Empty input is rejected. All rows must have the same width.

CSV input is copied to a temporary file. Inference and parsing stream that disk
snapshot. Replace mode externally sorts spooled rows with an 8 MiB chunk budget
and merge fan-in of 32. Insert mode processes spooled mutations in input order
against a private disk-backed working tree. Large individual records and
codec/cache buffers add to memory use. Only final tree objects are staged, and
the new snapshot becomes visible when the transaction commits.

Replace mode infers types across each entire column: Int64, UInt64, Boolean,
Float64, or Utf8. Mixed numeric/text columns remain text. Large integer values
are not rounded to fit a mixed floating-point column. Insert requires matching
column names, order, and key columns and parses values using the existing types.
Empty fields are empty strings, not a special null marker; they fail numeric
parsing when inserting into a numeric column. Decimal insertion accepts exact
fixed-point text without rounding; binary insertion uses the UTF-8 field bytes.

`--target-node-size` and `--node-size-stddev` must be positive and measure
canonical logical bytes before compression. Each leaf is a complete Parquet
file, so very small targets incur substantial file overhead.

`info` emits JSON with `root_node`, `tree_depth` (leaf height is zero), and row
counts for each table. `inspect` accepts an object ID or path and resolves its
schema from nodes reachable in the current snapshot. It emits `node_type`,
`children`, inclusive child fence keys, and row counts. Historical or unattached
nodes are not inspected by this command.

Imports stage objects and publish the root atomically under an exclusive writer
lock. Failed imports leave the previous snapshot readable. Old `.bch`/Avro data
must be reimported. See [writer publication details](../beech-write/README.md).
