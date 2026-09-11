use crate::Writer;
use beech_core::{
    codec::{self, EncodedObject},
    Id, Result, Root, Table, Transaction,
};
use std::{collections::BTreeMap, time::SystemTime};

/// IDs from the encoders. `root_id`, not `transaction_id`, is published in `root`.
#[derive(Debug, Clone, Copy)]
pub struct Publication {
    pub root_id: Id,
    pub transaction_id: Id,
    pub table_id: Id,
}

/// Replace one directory entry while retaining all other tables in a snapshot.
/// A successful return stages the root pointer; publication occurs at commit.
pub fn publish_table<W: Writer>(
    writer: &mut W,
    table: &Table,
    mut tables: BTreeMap<String, Id>,
    prev_id: Option<Id>,
) -> Result<Publication> {
    let table_id = save_object(writer, &codec::thrift::encode_table(table)?)?;
    tables.insert(table.name().to_owned(), table_id);
    let transaction = Transaction::new(prev_id.unwrap_or_default(), SystemTime::now(), tables)?;
    let transaction_id = save_object(writer, &codec::thrift::encode_transaction(&transaction)?)?;
    let root_id = save_object(writer, &codec::thrift::encode_root(&Root::new(transaction_id))?)?;
    writer.stage_root(root_id)?;
    Ok(Publication {
        root_id,
        transaction_id,
        table_id,
    })
}

fn save_object<W: Writer>(writer: &mut W, object: &EncodedObject) -> Result<Id> {
    writer.put(object.id(), object.bytes())?;
    Ok(object.id())
}
