use rkv::Value;
use crate::{update::UpdateResult, MResult};

#[derive(Copy, Clone)]
pub struct UpdatesResults {
    pub(crate) updates_results: rkv::SingleStore,
}

impl UpdatesResults {
    pub fn put_update_result(
        &self,
        writer: &mut rkv::Writer,
        update_id: u64,
        update_result: &UpdateResult,
    ) -> MResult<()>
    {
        let update_id_bytes = update_id.to_be_bytes();
        let update_result = bincode::serialize(&update_result)?;
        let blob = Value::Blob(&update_result);
        self.updates_results.put(writer, update_id_bytes, &blob)?;
        Ok(())
    }

    pub fn update_result<T: rkv::Readable>(
        &self,
        reader: &T,
        update_id: u64,
    ) -> MResult<Option<UpdateResult>>
    {
        let update_id_bytes = update_id.to_be_bytes();

        match self.updates_results.get(reader, update_id_bytes)? {
            Some(Value::Blob(bytes)) => {
                let update_result = bincode::deserialize(&bytes)?;
                Ok(Some(update_result))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }
}
