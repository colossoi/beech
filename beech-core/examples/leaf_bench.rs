//! Small reproducible leaf-size sweep, not a statistically rigorous benchmark.
//! cargo run --release -p beech-core --example leaf_bench
#[path = "../tests/support/mod.rs"]
mod support;
use beech_core::{
    codec::EncodedNode,
    query::{ConstraintOp, Predicate, Scan, ScanRequest},
    storage::Repository,
    *,
};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Instant};
use support::{MemoryStore, batch_from_rows};
struct Tree {
    table: Table,
    store: Arc<MemoryStore>,
    objects: Vec<EncodedNode>,
}
fn build(s: &TableSchema, rows: &[Row], leaf_rows: usize) -> Result<Tree> {
    let store = Arc::new(MemoryStore::default());
    let mut objects = vec![];
    for rows in rows.chunks(leaf_rows) {
        let node = codec::parquet::encode_leaf(s, &batch_from_rows(s, rows)?)?;
        store.put(node.reference().id(), node.bytes().clone())?;
        objects.push(node);
    }
    let mut level: Vec<_> = objects.iter().map(|n| n.reference().clone()).collect();
    while level.len() > 1 {
        let mut groups: Vec<Vec<_>> = level.chunks(8).map(|g| g.to_vec()).collect();
        if groups.len() > 1 && groups.last().unwrap().len() == 1 {
            let last = groups.pop().unwrap();
            groups.last_mut().unwrap().extend(last);
        }
        level = groups
            .into_iter()
            .map(|children| {
                let node = codec::thrift::encode_internal(
                    &InternalNode::new(s, children[0].height() + 1, children)?,
                    s,
                )?;
                store.put(node.reference().id(), node.bytes().clone())?;
                let r = node.reference().clone();
                objects.push(node);
                Ok(r)
            })
            .collect::<Result<_>>()?;
    }
    Ok(Tree {
        table: Table::new(
            "bench",
            s.clone(),
            level.pop(),
            rows.iter().map(|r| r.0).max().unwrap_or(-1),
        )?,
        store,
        objects,
    })
}
fn main() -> Result<()> {
    let schema = TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("bucket", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, false),
        ],
        vec![0],
    )?;
    let count = 8192;
    let mut rows: Vec<Row> = (0..count)
        .map(|i| {
            let payload = (0..16)
                .flat_map(|j| {
                    Sha256::digest(
                        [
                            b"beech.object\0\x01\x01".as_slice(),
                            format!("{i}/{j}").as_bytes(),
                        ]
                        .concat(),
                    )
                    .to_vec()
                })
                .collect();
            (
                i,
                vec![
                    Scalar::Int64(i),
                    Scalar::Int32((i / 64) as i32),
                    Scalar::Utf8(format!("row {i}")),
                    Scalar::Binary(payload),
                ],
            )
        })
        .collect();
    println!(
        "leaf_rows,leaves,internal_nodes,height,file_bytes,build_ms,operation,rows,ms,object_opens,column_loads,pruned_nodes,pruned_leaves"
    );
    for leaf_rows in [64, 256, 1024] {
        let started = Instant::now();
        let tree = build(&schema, &rows, leaf_rows)?;
        let build_ms = started.elapsed().as_secs_f64() * 1000.0;
        let source = Repository::new(tree.store.clone());
        let leaves = tree.objects.iter().filter(|n| n.reference().height() == 0).count();
        let bytes = tree.objects.iter().map(|n| n.bytes().len()).sum::<usize>();
        for op in ["point_cold", "point_warm", "range", "narrow", "full", "nonkey"] {
            let opens = tree.store.opens();
            let loads = source.stats()?.columns.loads;
            let mut req = ScanRequest::all(&tree.table);
            req.projection = vec![0];
            match op {
                "point_cold" | "point_warm" => {
                    req.predicates.push(Predicate::new(0, ConstraintOp::Eq, Scalar::Int64(count / 2)))
                }
                "range" => {
                    req.predicates.push(Predicate::new(0, ConstraintOp::Ge, Scalar::Int64(count / 2)));
                    req.predicates.push(Predicate::new(0, ConstraintOp::Lt, Scalar::Int64(count / 2 + 64)));
                }
                "full" => req.projection = vec![0, 1, 2, 3],
                "nonkey" => req.predicates.push(Predicate::new(1, ConstraintOp::Eq, Scalar::Int32(64))),
                _ => {}
            }
            let started = Instant::now();
            let mut scan = Scan::new(&source, &tree.table, req)?;
            let mut n = 0;
            for batch in scan.by_ref() {
                n += batch?.num_rows();
            }
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            println!(
                "{leaf_rows},{leaves},{},{},{bytes},{build_ms:.3},{op},{n},{ms:.3},{},{},{},{}",
                tree.objects.len() - leaves,
                tree.table.root().unwrap().height(),
                tree.store.opens() - opens,
                source.stats()?.columns.loads - loads,
                scan.metrics().pruned_nodes,
                scan.metrics().pruned_leaves
            );
        }
        // Fixed boundaries isolate leaf rewrite granularity; the unit tests separately
        // exercise content-defined boundaries and their resynchronization.
        rows.last_mut().unwrap().1[2] = Scalar::Utf8("updated last row".into());
        let updated = build(&schema, &rows, leaf_rows)?;
        let changed = updated
            .objects
            .iter()
            .filter(|n| !tree.objects.iter().any(|o| o.reference().id() == n.reference().id()))
            .collect::<Vec<_>>();
        eprintln!(
            "leaf_rows={leaf_rows}: last-row update replaces {} objects / {} bytes",
            changed.len(),
            changed.iter().map(|n| n.bytes().len()).sum::<usize>()
        );
        rows.last_mut().unwrap().1[2] = Scalar::Utf8(format!("row {}", count - 1));
    }
    Ok(())
}
