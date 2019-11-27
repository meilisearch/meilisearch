use super::BEU64;
use crate::database::UpdateT;
use crate::update::ProcessedUpdateResult;
use heed::types::{OwnedType, SerdeJson};
use heed::Result as ZResult;

#[derive(Copy, Clone)]
pub struct UpdatesResults {
    pub(crate) updates_results: heed::Database<OwnedType<BEU64>, SerdeJson<ProcessedUpdateResult>>,
}

impl UpdatesResults {
    pub fn last_update(
        self,
        reader: &heed::RoTxn<UpdateT>,
    ) -> ZResult<Option<(u64, ProcessedUpdateResult)>> {
        match self.updates_results.last(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    pub fn put_update_result(
        self,
        writer: &mut heed::RwTxn<UpdateT>,
        update_id: u64,
        update_result: &ProcessedUpdateResult,
    ) -> ZResult<()> {
        let update_id = BEU64::new(update_id);
        self.updates_results.put(writer, &update_id, update_result)
    }

    pub fn update_result(
        self,
        reader: &heed::RoTxn<UpdateT>,
        update_id: u64,
    ) -> ZResult<Option<ProcessedUpdateResult>> {
        let update_id = BEU64::new(update_id);
        self.updates_results.get(reader, &update_id)
    }

    pub fn clear(self, writer: &mut heed::RwTxn<UpdateT>) -> ZResult<()> {
        self.updates_results.clear(writer)
    }
}
