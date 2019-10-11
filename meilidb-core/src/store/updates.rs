use std::convert::TryInto;
use rkv::Value;
use crate::{update::Update, MResult};

#[derive(Copy, Clone)]
pub struct Updates {
    pub(crate) updates: rkv::SingleStore,
}

impl Updates {
    // TODO we should use the MDB_LAST op but
    //      it is not exposed by the rkv library
    pub fn last_update_id<'a>(
        &self,
        reader: &'a impl rkv::Readable,
    ) -> Result<Option<(u64, Option<Value<'a>>)>, rkv::StoreError>
    {
        let mut last = None;
        let iter = self.updates.iter_start(reader)?;
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

    fn first_update_id<'a>(
        &self,
        reader: &'a impl rkv::Readable,
    ) -> Result<Option<(u64, Option<Value<'a>>)>, rkv::StoreError>
    {
        let mut iter = self.updates.iter_start(reader)?;
        let (first_key, first_data) = match iter.next() {
            Some(result) => result?,
            None => return Ok(None),
        };

        let array = first_key.try_into().unwrap();
        let number = u64::from_be_bytes(array);

        Ok(Some((number, first_data)))
    }

    pub fn contains(
        &self,
        reader: &impl rkv::Readable,
        update_id: u64,
    ) -> Result<bool, rkv::StoreError>
    {
        let update_id_bytes = update_id.to_be_bytes();
        self.updates.get(reader, update_id_bytes).map(|v| v.is_some())
    }

    pub fn put_update(
        &self,
        writer: &mut rkv::Writer,
        update_id: u64,
        update: &Update,
    ) -> MResult<()>
    {
        let update_id_bytes = update_id.to_be_bytes();
        let update = serde_json::to_vec(&update)?;
        let blob = Value::Blob(&update);
        self.updates.put(writer, update_id_bytes, &blob)?;
        Ok(())
    }

    pub fn pop_front(
        &self,
        writer: &mut rkv::Writer,
    ) -> MResult<Option<(u64, Update)>>
    {
        let (first_id, first_data) = match self.first_update_id(writer)? {
            Some(entry) => entry,
            None => return Ok(None),
        };

        match first_data {
            Some(Value::Blob(bytes)) => {
                let update = serde_json::from_slice(&bytes)?;
                // remove it from the database now
                let first_id_bytes = first_id.to_be_bytes();
                self.updates.delete(writer, first_id_bytes)?;

                Ok(Some((first_id, update)))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }
}
