# beech-core

A content-addressed prolly tree with column-oriented Parquet leaves and Thrift
Compact Protocol metadata. Each leaf contains exactly one row group. The tree
builder determines leaf boundaries.

Core, the writer, CLI, SQLite adapter, and shared fixtures use this format.
The previous Avro storage format is incompatible; old data must be reimported.
See [the writer](../beech-write/README.md) for construction and publication APIs
and [the CLI](../beech-cli/README.md) for CSV imports.

## Layout

- `lib.rs`, `schema.rs`, `value.rs`, `decimal.rs`: tree/table types and values.
- `query.rs`, `plan.rs`: projected scans, predicates, shared key-prefix selection and cost estimates.
- `codec/thrift.rs`, `schema/beech.thrift`: Thrift metadata codecs.
- `codec/parquet.rs`: Parquet writing, footer/column decoding, statistics, and I/O adapter.
- `codec/mod.rs`: encoded objects, content hashing, and private format tags.
- `storage/`: immutable byte access, decoded caches, repository, and snapshots.

Domain constructors validate values without serializing them. Encoders return
content IDs alongside bytes; decoded values do not carry their own object ID.
Internal nodes retain their logical schema for validation. Queries consume decoded
scalar statistics; Parquet footer details stay inside the codec.

## Reading

Create a `Repository` over a `BackingStore`, then select a root with `snapshot`.
`FileStore` opens objects named by hexadecimal content ID. `Scan` yields Arrow
batches; `RowCursor` adapts these to `(row_id, values)` rows.

Keep the ID returned by `codec::thrift::encode_table` when publishing a table;
when reading, obtain it from the transaction's table directory. Schema and
transaction IDs likewise come from their encoders or the references used to load
them. Core exposes `plan::select_key_prefix` and `plan::estimate`
for adapters, and `ScanRequest` for concrete scans. SQLite owns its `AccessPlan`,
argument binding, and plan serialization.

Snapshots share immutable decoded objects. Metadata and column caches have
separate configurable budgets; approximate weights are isolated in
`storage/accounting.rs`. Active readers can retain evicted arrays, so these
budgets do not cap process memory. Columns are decoded lazily and cached by leaf
ID and physical column. Optional `verify_leaves` verifies complete leaf hashes;
metadata hashes are always verified.

Schemas support Boolean, Int32, Int64, UInt64, Float32, Float64, Decimal128,
Utf8, and Binary. Keys are sorted and unique, with schema-exact comparisons.
The physical schema starts with the signed `__beech_row_id` column. Decimals
retain their scale and require an exact match to the column's declared scale.
Float key ordering preserves signed zero and NaN bits; predicates use numeric
float comparisons.

## Development

Run from the workspace root:

```text
cargo test -p beech-core --locked
cargo clippy -p beech-core --all-targets --locked -- -D warnings
cargo fmt -p beech-core -- --check
```

The SQLite adapter uses the same repository and supports projected SQL scans:

```text
cargo test -p beech-sqlite3 --locked
cargo run -p beech-sqlite3-test -- path/to/data table_name
```

The runner expects content-ID filenames and a text `root` file containing the
encoded root object's ID. It prints the row count and the first five rows.
SQLite exposes Decimal128 and UInt64 as exact text; SQLite numeric conversions
apply when SQL expressions perform arithmetic on those values.

To use the native SQLite shell, build the adapter with its loadable-extension
feature. This uses the shell's SQLite API; the default build bundles SQLite for
the Rust runner and tests.

```powershell
cargo build -p beech-sqlite3 --lib --no-default-features --features loadable_extension
cargo run -p beech-core --example interoperability -- write target/sqlite-demo
Copy-Item target/sqlite-demo/root-id.txt target/sqlite-demo/root
sqlite3
```

Then, from the repository root in the SQLite shell:

```sql
.load ./target/debug/beech_sqlite3
.headers on
.mode column
CREATE VIRTUAL TABLE items USING beech('target/sqlite-demo', 'example');
SELECT count(*) FROM items;
SELECT rowid, key, label FROM items WHERE key >= 3 ORDER BY key;
```

On Linux/macOS, use `.load ./target/debug/libbeech_sqlite3` and `cp` for the
root-file copy. Rebuild with the extension feature after building the bundled
adapter, since both modes use the same library filename. Run adapter tests with
the default features: `cargo test -p beech-sqlite3`.

Generated Thrift Rust is checked in. To regenerate it with Thrift 0.24.0:

```powershell
./beech-core/schema/generate.ps1 -Compiler /path/to/thrift
```

Or from a POSIX shell:

```sh
./beech-core/schema/generate.sh /path/to/thrift
```

Both scripts default to `thrift` on `PATH` and repair a known list-of-union
generator defect. Arrow/Parquet 59.3.0 and the Snappy writer settings are pinned
to keep encoding reproducible.

Tests use an in-memory fixture store and checked-in files produced by PyArrow
25.0.1. Decimal fixtures cover fixed-byte and integer physical encodings;
zero/multiple-row-group fixtures check layout rejection. Python is not required.

`examples/interoperability.rs` exercises encoding and reopening fixture objects.
`examples/leaf_bench.rs` measures projection and cache reuse. The historical
`benchmarks/leaf-size.csv` was recorded on Windows on 2026-09-10 before the decoded
column cache; its byte-read counters and timings do not describe current caching.
