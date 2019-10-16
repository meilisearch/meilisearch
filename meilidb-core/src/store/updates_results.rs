use zlmdb::types::{OwnedType, Serde};
use zlmdb::Result as ZResult;
use crate::update::UpdateResult;
use super::BEU64;

#[derive(Copy, Clone)]
pub struct UpdatesResults {
    pub(crate) updates_results: zlmdb::Database<OwnedType<BEU64>, Serde<UpdateResult>>,
}

impl UpdatesResults {
    pub fn last_update_id(&self, reader: &zlmdb::RoTxn) -> ZResult<Option<(u64, UpdateResult)>> {
        match self.updates_results.last(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    pub fn put_update_result(
        &self,
        writer: &mut zlmdb::RwTxn,
        update_id: u64,
        update_result: &UpdateResult,
    ) -> ZResult<()>
    {
        let update_id = BEU64::new(update_id);
        self.updates_results.put(writer, &update_id, update_result)
    }

    pub fn update_result(
        &self,
        reader: &zlmdb::RoTxn,
        update_id: u64,
    ) -> ZResult<Option<UpdateResult>>
    {
        let update_id = BEU64::new(update_id);
        self.updates_results.get(reader, &update_id)
    }
}
