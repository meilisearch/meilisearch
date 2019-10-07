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
    fn last_update_id<'a>(
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

    pub fn push_back(
        &self,
        writer: &mut rkv::Writer,
        update: &Update,
    ) -> MResult<u64>
    {
        let last_update_id = self.last_update_id(writer)?;
        let last_update_id = last_update_id.map_or(0, |(n, _)| n + 1);
        let last_update_id_bytes = last_update_id.to_be_bytes();

        let update = rmp_serde::to_vec_named(&update)?;
        let blob = Value::Blob(&update);
        self.updates.put(writer, last_update_id_bytes, &blob)?;

        Ok(last_update_id)
    }

    pub fn pop_front(
        &self,
        writer: &mut rkv::Writer,
    ) -> MResult<Option<(u64, Update)>>
    {
        let (last_id, last_data) = match self.first_update_id(writer)? {
            Some(entry) => entry,
            None => return Ok(None),
        };

        match last_data {
            Some(Value::Blob(bytes)) => {
                let update = rmp_serde::from_read_ref(&bytes)?;

                // remove it from the database now
                let last_id_bytes = last_id.to_be_bytes();
                self.updates.delete(writer, last_id_bytes)?;

                Ok(Some((last_id, update)))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }
}
