mod criterion;
mod fields_ids_map;
mod index;
mod mdfs;
mod query_tokens;
mod search;
pub mod heed_codec;
pub mod proximity;
pub mod subcommand;
pub mod tokenizer;
pub mod update;

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use anyhow::Context;
use fxhash::{FxHasher32, FxHasher64};
use serde_json::{Map, Value};

pub use self::criterion::{Criterion, default_criteria};
pub use self::fields_ids_map::FieldsIdsMap;
pub use self::index::Index;
pub use self::search::{Search, SearchResult};
pub use self::heed_codec::{
    RoaringBitmapCodec, BEU32StrCodec, StrStrU8Codec,
    ObkvCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec,
};
pub use self::update::UpdateStore;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;
pub type DocumentId = u32;
pub type Attribute = u32;
pub type Position = u32;

type MergeFn = for<'a> fn(&[u8], &[Cow<'a, [u8]>]) -> anyhow::Result<Vec<u8>>;

/// Transform a raw obkv store into a JSON Object.
pub fn obkv_to_json(
    displayed_fields: &[u8],
    fields_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReader,
) -> anyhow::Result<Map<String, Value>>
{
    displayed_fields.iter()
        .copied()
        .flat_map(|id| obkv.get(id).map(|value| (id, value)))
        .map(|(id, value)| {
            let name = fields_ids_map.name(id).context("unknown obkv field id")?;
            let value = serde_json::from_slice(value)?;
            Ok((name.to_owned(), value))
        })
        .collect()
}
