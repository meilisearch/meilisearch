#[macro_use]
extern crate pest_derive;

mod criterion;
mod error;
mod external_documents_ids;
pub mod facet;
mod fields_ids_map;
pub mod heed_codec;
pub mod index;
pub mod proximity;
mod search;
pub mod tree_level;
pub mod update;

use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::hash::BuildHasherDefault;

use fxhash::{FxHasher32, FxHasher64};
pub use grenad::CompressionType;
use serde_json::{Map, Value};

pub use self::criterion::{default_criteria, AscDesc, Criterion, Member};
pub use self::error::{
    Error, FieldIdMapMissingEntry, InternalError, SerializationError, UserError,
};
pub use self::external_documents_ids::ExternalDocumentsIds;
pub use self::fields_ids_map::FieldsIdsMap;
pub use self::heed_codec::{
    BEU32StrCodec, BoRoaringBitmapCodec, BoRoaringBitmapLenCodec, CboRoaringBitmapCodec,
    CboRoaringBitmapLenCodec, FieldIdWordCountCodec, ObkvCodec, RoaringBitmapCodec,
    RoaringBitmapLenCodec, StrLevelPositionCodec, StrStrU8Codec,
};
pub use self::index::Index;
pub use self::search::{FacetDistribution, FilterCondition, MatchingWords, Search, SearchResult};
pub use self::tree_level::TreeLevel;

pub type Result<T> = std::result::Result<T, error::Error>;

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec8<T> = smallvec::SmallVec<[T; 8]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type BEU64 = heed::zerocopy::U64<heed::byteorder::BE>;
pub type Attribute = u32;
pub type DocumentId = u32;
pub type FieldId = u16;
pub type Position = u32;
pub type FieldDistribution = BTreeMap<String, u64>;
pub type GeoPoint = rstar::primitives::GeomWithData<[f64; 2], DocumentId>;

/// Transform a raw obkv store into a JSON Object.
pub fn obkv_to_json(
    displayed_fields: &[FieldId],
    fields_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReaderU16,
) -> Result<Map<String, Value>> {
    displayed_fields
        .iter()
        .copied()
        .flat_map(|id| obkv.get(id).map(|value| (id, value)))
        .map(|(id, value)| {
            let name = fields_ids_map.name(id).ok_or(error::FieldIdMapMissingEntry::FieldId {
                field_id: id,
                process: "obkv_to_json",
            })?;
            let value = serde_json::from_slice(value).map_err(error::InternalError::SerdeJson)?;
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
            }
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
            }
        }
    }

    let mut string = String::new();
    if inner(value, &mut string) {
        Some(string)
    } else {
        None
    }
}

/// Divides one slice into two at an index, returns `None` if mid is out of bounds.
fn try_split_at<T>(slice: &[T], mid: usize) -> Option<(&[T], &[T])> {
    if mid <= slice.len() {
        Some(slice.split_at(mid))
    } else {
        None
    }
}

/// Divides one slice into an array and the tail at an index,
/// returns `None` if `N` is out of bounds.
fn try_split_array_at<T, const N: usize>(slice: &[T]) -> Option<([T; N], &[T])>
where
    [T; N]: for<'a> TryFrom<&'a [T]>,
{
    let (head, tail) = try_split_at(slice, N)?;
    let head = head.try_into().ok()?;
    Some((head, tail))
}

/// Return the distance between two points in meters.
fn distance_between_two_points(a: &[f64; 2], b: &[f64; 2]) -> f64 {
    let a = haversine::Location { latitude: a[0], longitude: a[1] };
    let b = haversine::Location { latitude: b[0], longitude: b[1] };

    haversine::distance(a, b, haversine::Units::Kilometers) * 1000.
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
