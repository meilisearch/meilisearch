use std::convert::TryInto;
use rkv::Value;
use crate::update::Update;

#[derive(Copy, Clone)]
pub struct Updates {
    pub(crate) updates: rkv::SingleStore,
}

impl Updates {
    // TODO we should use the MDB_LAST op but
    //      it is not exposed by the rkv library
    fn last_update_id<'a, T: rkv::Readable>(
        &self,
        reader: &'a T,
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

    pub fn contains<T: rkv::Readable>(
        &self,
        reader: &T,
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
    ) -> Result<u64, rkv::StoreError>
    {
        let last_update_id = self.last_update_id(writer)?;
        let last_update_id = last_update_id.map_or(0, |(n, _)| n + 1);
        let last_update_id_bytes = last_update_id.to_be_bytes();

        let update = rmp_serde::to_vec_named(&update).unwrap();
        let blob = Value::Blob(&update);
        self.updates.put(writer, last_update_id_bytes, &blob)?;

        Ok(last_update_id)
    }

    pub fn pop_back(
        &self,
        writer: &mut rkv::Writer,
    ) -> Result<Option<(u64, Update)>, rkv::StoreError>
    {
        let (last_id, last_data) = match self.last_update_id(writer)? {
            Some(entry) => entry,
            None => return Ok(None),
        };

        match last_data {
            Some(Value::Blob(bytes)) => {
                let update = rmp_serde::from_read_ref(&bytes).unwrap();
                Ok(Some((last_id, update)))
            },
            Some(value) => panic!("invalid type {:?}", value),
            None => Ok(None),
        }
    }
}
