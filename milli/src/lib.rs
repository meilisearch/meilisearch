#[macro_use] extern crate pest_derive;

mod criterion;
mod external_documents_ids;
mod fields_ids_map;
mod index;
mod mdfs;
mod query_tokens;
mod search;
mod update_store;
pub mod facet;
pub mod heed_codec;
pub mod proximity;
pub mod subcommand;
pub mod update;

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use anyhow::Context;
use fxhash::{FxHasher32, FxHasher64};
use serde_json::{Map, Value};

pub use self::criterion::{Criterion, default_criteria};
pub use self::external_documents_ids::ExternalDocumentsIds;
pub use self::fields_ids_map::FieldsIdsMap;
pub use self::heed_codec::{BEU32StrCodec, StrStrU8Codec, ObkvCodec};
pub use self::heed_codec::{RoaringBitmapCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec};
pub use self::index::Index;
pub use self::search::{Search, FacetDistribution, FacetCondition, SearchResult};
pub use self::update_store::UpdateStore;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type SmallVec8<T> = smallvec::SmallVec<[T; 8]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;
pub type Attribute = u32;
pub type DocumentId = u32;
pub type FieldId = u8;
pub type Position = u32;

type MergeFn = for<'a> fn(&[u8], &[Cow<'a, [u8]>]) -> anyhow::Result<Vec<u8>>;

/// Transform a raw obkv store into a JSON Object.
pub fn obkv_to_json(
    displayed_fields: &[FieldId],
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

/// Transform a JSON value into a string that can be indexed.
pub fn json_to_string(value: &Value) -> Option<String> {

    fn inner(value: &Value, output: &mut String) -> bool {
        use std::fmt::Write;
        match value {
            Value::Null => false,
            Value::Bool(boolean) => write!(output, "{}", boolean).is_ok(),
            Value::Number(number) => write!(output, "{}", number).is_ok(),
            Value::String(string) => write!(output, "{}", string).is_ok(),
            Value::Array(array) => {
                let mut count = 0;
                for value in array {
                    if inner(value, output) {
                        output.push_str(". ");
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            },
            Value::Object(object) => {
                let mut buffer = String::new();
                let mut count = 0;
                for (key, value) in object {
                    buffer.clear();
                    let _ = write!(&mut buffer, "{}: ", key);
                    if inner(value, &mut buffer) {
                        buffer.push_str(". ");
                        // We write the "key: value. " pair only when
                        // we are sure that the value can be written.
                        output.push_str(&buffer);
                        count += 1;
                    }
                }
                // check that at least one value was written
                count != 0
            },
        }
    }

    let mut string = String::new();
    if inner(value, &mut string) {
        Some(string)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_to_string_object() {
        let value = json!({
            "name": "John Doe",
            "age": 43,
            "not_there": null,
        });

        let string = json_to_string(&value).unwrap();
        assert_eq!(string, "name: John Doe. age: 43. ");
    }

    #[test]
    fn json_to_string_array() {
        let value = json!([
            { "name": "John Doe" },
            43,
            "hello",
            [ "I", "am", "fine" ],
            null,
        ]);

        let string = json_to_string(&value).unwrap();
        // We don't care about having two point (.) after the other as
        // the distance of hard separators is clamped to 8 anyway.
        assert_eq!(string, "name: John Doe. . 43. hello. I. am. fine. . ");
    }
}
