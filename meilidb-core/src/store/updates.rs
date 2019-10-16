use zlmdb::types::{OwnedType, Serde};
use zlmdb::Result as ZResult;
use crate::update::Update;
use super::BEU64;

#[derive(Copy, Clone)]
pub struct Updates {
    pub(crate) updates: zlmdb::Database<OwnedType<BEU64>, Serde<Update>>,
}

impl Updates {
    // TODO do not trigger deserialize if possible
    pub fn last_update_id(&self, reader: &zlmdb::RoTxn) -> ZResult<Option<(u64, Update)>> {
        match self.updates.last(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    // TODO do not trigger deserialize if possible
    fn first_update_id(&self, reader: &zlmdb::RoTxn) -> ZResult<Option<(u64, Update)>> {
        match self.updates.first(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    // TODO do not trigger deserialize if possible
    pub fn contains(&self, reader: &zlmdb::RoTxn, update_id: u64) -> ZResult<bool> {
        let update_id = BEU64::new(update_id);
        self.updates.get(reader, &update_id).map(|v| v.is_some())
    }

    pub fn put_update(
        &self,
        writer: &mut zlmdb::RwTxn,
        update_id: u64,
        update: &Update,
    ) -> ZResult<()>
    {
        // TODO prefer using serde_json?
        let update_id = BEU64::new(update_id);
        self.updates.put(writer, &update_id, update)
    }

    pub fn pop_front(&self, writer: &mut zlmdb::RwTxn) -> ZResult<Option<(u64, Update)>> {
        match self.first_update_id(writer)? {
            Some((update_id, update)) => {
                let key = BEU64::new(update_id);
                self.updates.delete(writer, &key)?;
                Ok(Some((update_id, update)))
            },
            None => Ok(None)
        }
    }
}
