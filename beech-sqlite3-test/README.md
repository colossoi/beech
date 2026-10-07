# SQLite stress runner

Build and run the differential stress executable:

```sh
cargo run --release -p beech-sqlite3-test --bin sqlite-stress -- \
  --duration-secs 300 --max-disk-mib 512 --tables 4 --initial-rows 1000 --seed 42
```

Or build with `cargo build --release -p beech-sqlite3-test --bin sqlite-stress`
and run `target/release/sqlite-stress` directly. Use `--help` for all options.
The original snapshot inspection executable remains the package's default.

Each run creates a fresh SQLite database containing several Beech virtual
tables and matching native SQLite reference tables. All Beech tables (`data0`, `data1`, etc.) share one repository in `repo/`,
with a single root containing every table. Alternating writes must preserve
the other tables in that root. Each transaction writes one Beech table; the
runner does not yet test multi-table write transactions or concurrent writers.

The seeded workload repeatedly selects among ten operations:

| ID | Operation |
| --- | --- |
| 0 | Insert a batch and commit |
| 1 | Insert a batch and roll back |
| 2 | Bulk value update |
| 3 | Bulk delete |
| 4 | Change a key and rowid, including negative rowids |
| 5 | Update and delete in a transaction, then roll back |
| 6 | Filtered ordered reads and aggregates |
| 7 | Reconnect and check every table's persisted rows |
| 8 | Reject a duplicate rowid/key and check unchanged contents |
| 9 | Join two tables |

Payloads include NULL, Unicode, and variable-length text. The runner compares
affected-row counts and complete ordered rows against native SQLite after
each operation, including reads inside transactions before rollback. Failures
exit nonzero and report the seed, step, table, and operation. Use the same seed,
table count, and initial row count to repeat the operation sequence. Progress
prints each attempted step and completed-operation counts every 25 steps.

The duration includes setup. A supervisor stops the worker at the time limit,
even during setup or a slow operation. The last attempted operation may be
interrupted and is not claimed as verified. A normal time or disk-budget stop
exits successfully; a worker error or observed budget overshoot exits nonzero.

## Disk budget and cleanup

`--max-disk-mib` is a **monitored budget, not a hard filesystem quota**. The
supervisor sums file lengths in the run directory approximately every 20 ms,
including immutable repository history, SQLite journals, and temporary redb
and sorting files. It stops at 90% of the budget to leave write headroom and
reports the peak observed bytes. Writes between samples can overshoot, especially
with tiny budgets; overshoot is reported as an error. Filesystem metadata,
allocation rounding, build outputs, and externally redirected console logs
are outside this accounting. Use a filesystem quota for an absolute ceiling.

`--directory /path/to/parent` puts the isolated run directory on a chosen disk.
The parent must already exist. No existing database is modified. Temporary-file
environment variables are set only for the child worker so scratch files stay
inside the measured directory. Run files are removed on normal termination and
worker failure. Add `--keep` to retain them for inspection; the runner prints
their location. A forced termination of the supervisor itself can leave files
behind. A retained database may require recovery after an interrupted operation.

Run the CLI integration checks with:

```sh
cargo test -p beech-sqlite3-test
```
