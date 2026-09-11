//! Exercise core as an external crate: no access to private validation or format details.
mod support;
use beech_core::{
    plan::{self, CandidateConstraint},
    query::{ConstraintOp, Predicate, RowCursor, Scan, ScanRequest},
    storage::{BackingStore, Leaf, ObjectFile, Repository},
    *,
};
use std::{collections::BTreeMap, sync::Arc, time::UNIX_EPOCH};
use support::{MemoryStore, batch_from_rows};

fn schema() -> TableSchema {
    TableSchema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ],
        vec![0],
    )
    .unwrap()
}

/// A custom store only needs to understand IDs and byte/file handles.
struct StoreAdapter(Arc<MemoryStore>);
impl BackingStore for StoreAdapter {
    fn get(&self, id: &Id) -> Result<ObjectFile> {
        self.0.get(id)
    }
}

#[test]
fn typed_encoders_support_store_reopen_plan_and_projection() -> Result<()> {
    let schema = schema();
    let store = Arc::new(MemoryStore::default());
    let rows = (0..4)
        .map(|n| (100 + n, vec![Scalar::Int64(n), Scalar::Utf8(format!("row {n}"))]))
        .collect::<Vec<_>>();
    let mut children = vec![];
    for chunk in rows.chunks(2) {
        let encoded = codec::parquet::encode_leaf(&schema, &batch_from_rows(&schema, chunk)?)?;
        store.put(encoded.reference().id(), encoded.bytes().clone())?;
        children.push(encoded.reference().clone());
    }
    let node = InternalNode::new(&schema, 1, children)?;
    assert_eq!(node.row_count()?, 4);
    assert_eq!(node.seek(&schema, &vec![Scalar::Int64(2)])?, Some(1));
    let encoded = codec::thrift::encode_internal(&node, &schema)?;
    store.put(encoded.reference().id(), encoded.bytes().clone())?;
    let table = Table::new("items", schema, Some(encoded.reference().clone()))?;
    let object = codec::thrift::encode_table(&table)?;
    let table_id = object.id();
    store.put(object.id(), object.bytes().clone())?;
    let txn = Transaction::new(
        Id::default(),
        UNIX_EPOCH,
        BTreeMap::from([(table.name().into(), table_id)]),
    )?;
    let object = codec::thrift::encode_transaction(&txn)?;
    let transaction_id = object.id();
    store.put(object.id(), object.bytes().clone())?;
    let object = codec::thrift::encode_root(&Root::new(transaction_id))?;
    store.put(object.id(), object.bytes().clone())?;
    let root_id = object.id();
    let source = Repository::with_options(
        StoreAdapter(store),
        storage::RepositoryOptions {
            verify_leaves: true,
            ..Default::default()
        },
    );
    let repository = Arc::new(source);
    let source = repository.snapshot(root_id)?;
    assert_eq!(source.transaction().as_ref(), &txn);
    assert_eq!(source.transaction().tables()["items"], table_id);
    let reopened = source.table("items")?;
    assert_eq!(&table, reopened.as_ref());
    assert_eq!(
        RowCursor::new(&source, &reopened, vec![])?.collect::<Result<Vec<_>>>()?,
        rows
    );

    let candidates = [CandidateConstraint {
        column: 0,
        op: ConstraintOp::Eq,
    }];
    let selected = plan::select_key_prefix(table.schema(), &candidates);
    assert_eq!(selected, vec![0]);
    assert_eq!(
        plan::estimate(&table, selected.iter().map(|&i| candidates[i])).estimated_rows,
        1
    );
    let mut request = ScanRequest::all(&table);
    request.predicates.push(Predicate::new(0, ConstraintOp::Eq, Scalar::Int64(2)));
    request.projection = vec![1];
    let mut scan = Scan::new(&source, &table, request)?;
    let result = scan.by_ref().collect::<Result<Vec<_>>>()?;
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(result[0].schema().field(0).name(), "value");
    assert_eq!(scan.metrics().output_rows, 1);
    Ok(())
}

struct SingleLeafSource {
    repository: Repository,
}
impl NodeSource for SingleLeafSource {
    fn get_internal(&self, reference: &NodeRef, _: &TableSchema) -> Result<Arc<InternalNode>> {
        Err(BeechError::NotFound(reference.id()))
    }
    fn open_leaf(&self, reference: &NodeRef, schema: &TableSchema) -> Result<Leaf> {
        self.repository.open_leaf(reference, schema)
    }
}

#[test]
fn custom_node_source_can_open_and_scan_projected_leaves() -> Result<()> {
    let schema = schema();
    let rows = vec![(7, vec![Scalar::Int64(1), Scalar::Null])];
    let node = codec::parquet::encode_leaf(&schema, &batch_from_rows(&schema, &rows)?)?;
    let reference = node.reference();
    assert_eq!(
        NodeRef::new(
            &schema,
            reference.id(),
            reference.height(),
            reference.row_count(),
            reference.max_key().clone()
        )?,
        *reference
    );
    assert!(NodeRef::new(&schema, reference.id(), 0, 0, reference.max_key().clone()).is_err());
    let table = Arc::new(Table::new("custom", schema, Some(reference.clone()))?);
    let store = MemoryStore::default();
    store.put(reference.id(), node.bytes().clone())?;
    let source = SingleLeafSource {
        repository: Repository::new(store),
    };
    let reader = source.open_leaf(reference, table.schema())?;
    let batches = reader.read(&[2], 16)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].column(0).null_count(), 1);
    assert_eq!(
        RowCursor::new(&source, &table, vec![])?.collect::<Result<Vec<_>>>()?,
        rows
    );
    assert_eq!(
        Scan::new(&source, &table, ScanRequest::all(&table))?.next().unwrap()?.num_rows(),
        1
    );
    Ok(())
}

#[test]
fn stored_ids_are_immutable_and_bad_metadata_is_rejected() -> Result<()> {
    let object = codec::thrift::encode_root(&Root::new(Id::default()))?;
    let store = Arc::new(MemoryStore::default());
    store.put(object.id(), object.bytes().clone())?;
    store.put(object.id(), object.bytes().clone())?;
    assert!(store.put(object.id(), b"changed".to_vec()).is_err());
    let wrong_id = Id::from([9; 32]);
    store.put(wrong_id, object.bytes().clone())?;
    let source = Repository::new(store);
    assert!(matches!(source.get_root(&wrong_id), Err(BeechError::HashMismatch(id)) if id == wrong_id));
    Ok(())
}
