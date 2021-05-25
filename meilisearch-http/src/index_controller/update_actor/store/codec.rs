use std::{borrow::Cow, convert::TryInto, mem::size_of};

use heed::{BytesDecode, BytesEncode};
use uuid::Uuid;

pub struct NextIdCodec;

pub enum NextIdKey {
    Global,
    Index(Uuid),
}

impl<'a> BytesEncode<'a> for NextIdCodec {
    type EItem = NextIdKey;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        match item {
            NextIdKey::Global => Some(Cow::Borrowed(b"__global__")),
            NextIdKey::Index(ref uuid) => Some(Cow::Borrowed(uuid.as_bytes())),
        }
    }
}

pub struct PendingKeyCodec;

impl<'a> BytesEncode<'a> for PendingKeyCodec {
    type EItem = (u64, Uuid, u64);

    fn bytes_encode((global_id, uuid, update_id): &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut bytes = Vec::with_capacity(size_of::<Self::EItem>());
        bytes.extend_from_slice(&global_id.to_be_bytes());
        bytes.extend_from_slice(uuid.as_bytes());
        bytes.extend_from_slice(&update_id.to_be_bytes());
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for PendingKeyCodec {
    type DItem = (u64, Uuid, u64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let global_id_bytes = bytes.get(0..size_of::<u64>())?.try_into().ok()?;
        let global_id = u64::from_be_bytes(global_id_bytes);

        let uuid_bytes = bytes
            .get(size_of::<u64>()..(size_of::<u64>() + size_of::<Uuid>()))?
            .try_into()
            .ok()?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        let update_id_bytes = bytes
            .get((size_of::<u64>() + size_of::<Uuid>())..)?
            .try_into()
            .ok()?;
        let update_id = u64::from_be_bytes(update_id_bytes);

        Some((global_id, uuid, update_id))
    }
}

pub struct UpdateKeyCodec;

impl<'a> BytesEncode<'a> for UpdateKeyCodec {
    type EItem = (Uuid, u64);

    fn bytes_encode((uuid, update_id): &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut bytes = Vec::with_capacity(size_of::<Self::EItem>());
        bytes.extend_from_slice(uuid.as_bytes());
        bytes.extend_from_slice(&update_id.to_be_bytes());
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for UpdateKeyCodec {
    type DItem = (Uuid, u64);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let uuid_bytes = bytes.get(0..size_of::<Uuid>())?.try_into().ok()?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        let update_id_bytes = bytes.get(size_of::<Uuid>()..)?.try_into().ok()?;
        let update_id = u64::from_be_bytes(update_id_bytes);

        Some((uuid, update_id))
    }
}
