use super::BEU64;
use crate::database::UpdateT;
use crate::update::Update;
use heed::types::{OwnedType, SerdeJson};
use heed::Result as ZResult;

#[derive(Copy, Clone)]
pub struct Updates {
    pub(crate) updates: heed::Database<OwnedType<BEU64>, SerdeJson<Update>>,
}

impl Updates {
    // TODO do not trigger deserialize if possible
    pub fn last_update(self, reader: &heed::RoTxn<UpdateT>) -> ZResult<Option<(u64, Update)>> {
        match self.updates.last(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    // TODO do not trigger deserialize if possible
    pub fn first_update(self, reader: &heed::RoTxn<UpdateT>) -> ZResult<Option<(u64, Update)>> {
        match self.updates.first(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    // TODO do not trigger deserialize if possible
    pub fn get(self, reader: &heed::RoTxn<UpdateT>, update_id: u64) -> ZResult<Option<Update>> {
        let update_id = BEU64::new(update_id);
        self.updates.get(reader, &update_id)
    }

    pub fn put_update(
        self,
        writer: &mut heed::RwTxn<UpdateT>,
        update_id: u64,
        update: &Update,
    ) -> ZResult<()> {
        // TODO prefer using serde_json?
        let update_id = BEU64::new(update_id);
        self.updates.put(writer, &update_id, update)
    }

    pub fn del_update(self, writer: &mut heed::RwTxn<UpdateT>, update_id: u64) -> ZResult<bool> {
        let update_id = BEU64::new(update_id);
        self.updates.delete(writer, &update_id)
    }

    pub fn pop_front(self, writer: &mut heed::RwTxn<UpdateT>) -> ZResult<Option<(u64, Update)>> {
        match self.first_update(writer)? {
            Some((update_id, update)) => {
                let key = BEU64::new(update_id);
                self.updates.delete(writer, &key)?;
                Ok(Some((update_id, update)))
            }
            None => Ok(None),
        }
    }

    pub fn clear(self, writer: &mut heed::RwTxn<UpdateT>) -> ZResult<()> {
        self.updates.clear(writer)
    }
}
