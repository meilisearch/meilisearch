use std::convert::TryInto;
use rkv::Value;
use crate::{update::UpdateResult, MResult};

#[derive(Copy, Clone)]
pub struct UpdatesResults {
    pub(crate) updates_results: rkv::SingleStore,
}

impl UpdatesResults {
    // TODO we should use the MDB_LAST op but
    //      it is not exposed by the rkv library
    pub fn last_update_id<'a>(
        &self,
        reader: &'a impl rkv::Readable,
    ) -> Result<Option<(u64, Option<Value<'a>>)>, rkv::StoreError>
    {
        let mut last = None;
        let iter = self.updates_results.iter_start(reader)?;
        for result in iter {
            let (key, data) = result?;
            last = Some((key, data));
        }

        let (last_key, last_data) = match last {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let array = last_key.try_into().unwrap();
        let number = u64::from_be_bytes(array);

        Ok(Some((number, last_data)))
    }

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

    pub fn update_result(
        &self,
        reader: &impl rkv::Readable,
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
