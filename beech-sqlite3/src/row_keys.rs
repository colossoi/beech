use beech_core::{
    Key, Result,
    codec::thrift::{decode_key, encode_key},
};
use beech_disk::{RedbScratchStore, ScratchStore, Workspace};

/// Transaction-local mappings captured by SQLite cursors before mutations.
/// Keep serialized keys on disk; dropping the transaction removes the store.
pub(crate) struct RowKeys(RedbScratchStore);

impl RowKeys {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self(RedbScratchStore::new(&Workspace::new()?)?))
    }

    pub(crate) fn insert(&mut self, row_id: i64, key: &Key) -> Result<()> {
        self.0.put(&row_id.to_be_bytes(), &encode_key(key)?)?;
        Ok(())
    }

    pub(crate) fn remove(&mut self, row_id: i64) -> Result<Option<Key>> {
        let id = row_id.to_be_bytes();
        let Some(bytes) = self.0.get(&id)? else {
            return Ok(None);
        };
        let key = decode_key(&bytes)?;
        self.0.delete(&id)?;
        Ok(Some(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beech_core::Scalar;

    #[test]
    fn mappings_preserve_composite_keys_and_replace_consumed_rowids() {
        let mut mappings = RowKeys::new().unwrap();
        let key = vec![
            Scalar::Utf8("a\0b".into()),
            Scalar::Binary(vec![0, 255]),
            Scalar::Null,
        ];
        for id in [i64::MIN, 0, i64::MAX] {
            mappings.insert(id, &key).unwrap();
        }
        let replacement = vec![Scalar::Int64(42)];
        mappings.insert(0, &replacement).unwrap();
        assert_eq!(mappings.remove(0).unwrap(), Some(replacement));
        assert_eq!(mappings.remove(0).unwrap(), None);
        for id in [i64::MIN, i64::MAX] {
            assert_eq!(mappings.remove(id).unwrap(), Some(key.clone()));
        }
        mappings.insert(0, &key).unwrap();
        assert_eq!(mappings.remove(0).unwrap(), Some(key));
    }
}
