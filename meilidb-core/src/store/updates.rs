use super::BEU64;
use crate::update::Update;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use zlmdb::types::OwnedType;
use zlmdb::{BytesDecode, BytesEncode, Result as ZResult};

pub struct SerdeJson<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for SerdeJson<T>
where
    T: Serialize,
{
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        serde_json::to_vec(item).map(Cow::Owned).ok()
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeJson<T>
where
    T: Deserialize<'a> + Clone,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        serde_json::from_slice(bytes).ok()
    }
}

#[derive(Copy, Clone)]
pub struct Updates {
    pub(crate) updates: zlmdb::Database<OwnedType<BEU64>, SerdeJson<Update>>,
}

impl Updates {
    // TODO do not trigger deserialize if possible
    pub fn last_update_id(self, reader: &zlmdb::RoTxn) -> ZResult<Option<(u64, Update)>> {
        match self.updates.last(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    // TODO do not trigger deserialize if possible
    fn first_update_id(self, reader: &zlmdb::RoTxn) -> ZResult<Option<(u64, Update)>> {
        match self.updates.first(reader)? {
            Some((key, data)) => Ok(Some((key.get(), data))),
            None => Ok(None),
        }
    }

    // TODO do not trigger deserialize if possible
    pub fn contains(self, reader: &zlmdb::RoTxn, update_id: u64) -> ZResult<bool> {
        let update_id = BEU64::new(update_id);
        self.updates.get(reader, &update_id).map(|v| v.is_some())
    }

    pub fn put_update(
        self,
        writer: &mut zlmdb::RwTxn,
        update_id: u64,
        update: &Update,
    ) -> ZResult<()> {
        // TODO prefer using serde_json?
        let update_id = BEU64::new(update_id);
        self.updates.put(writer, &update_id, update)
    }

    pub fn pop_front(self, writer: &mut zlmdb::RwTxn) -> ZResult<Option<(u64, Update)>> {
        match self.first_update_id(writer)? {
            Some((update_id, update)) => {
                let key = BEU64::new(update_id);
                self.updates.delete(writer, &key)?;
                Ok(Some((update_id, update)))
            }
            None => Ok(None),
        }
    }
}
