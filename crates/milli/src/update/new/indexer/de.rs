use std::ops::ControlFlow;

use bumpalo::Bump;
use bumparaw_collections::RawVec;
use rustc_hash::FxBuildHasher;
use serde::de::{DeserializeSeed, Deserializer as _, Visitor};
use serde_json::value::RawValue;

use crate::documents::{
    validate_document_id_str, DocumentIdExtractionError, FieldIdMapper, PrimaryKey,
};
use crate::fields_ids_map::MutFieldIdMapper;
use crate::{FieldId, UserError};

// visits a document to fill the top level fields of the field id map and retrieve the external document id.
pub struct FieldAndDocidExtractor<'p, 'indexer, Mapper: MutFieldIdMapper> {
    fields_ids_map: &'p mut Mapper,
    primary_key: &'p PrimaryKey<'p>,
    indexer: &'indexer Bump,
}

impl<'p, 'indexer, Mapper: MutFieldIdMapper> FieldAndDocidExtractor<'p, 'indexer, Mapper> {
    pub fn new(
        fields_ids_map: &'p mut Mapper,
        primary_key: &'p PrimaryKey<'p>,
        indexer: &'indexer Bump,
    ) -> Self {
        Self { fields_ids_map, primary_key, indexer }
    }
}

impl<'de, 'p, 'indexer: 'de, Mapper: MutFieldIdMapper> Visitor<'de>
    for FieldAndDocidExtractor<'p, 'indexer, Mapper>
{
    type Value =
        Result<Result<DeOrBumpStr<'de, 'indexer>, DocumentIdExtractionError>, crate::UserError>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        // We need to remember if we encountered a semantic error, because raw values don't like to be parsed partially
        // (trying to do so results in parsing errors).
        // So we'll exhaust all keys and values even if we encounter an error, and we'll then return any error we detected.
        let mut attribute_limit_reached = false;
        let mut document_id_extraction_error = None;
        let mut docid = None;

        while let Some(((level_name, right), (fid, fields_ids_map))) =
            map.next_key_seed(ComponentsSeed {
                name: self.primary_key.name(),
                visitor: MutFieldIdMapVisitor(self.fields_ids_map),
            })?
        {
            self.fields_ids_map = fields_ids_map;

            let value: &'de RawValue = map.next_value()?;
            if attribute_limit_reached || document_id_extraction_error.is_some() {
                continue;
            }

            let Some(_fid) = fid else {
                attribute_limit_reached = true;
                continue;
            };

            match match_component(level_name, right, value, self.indexer, &mut docid) {
                ControlFlow::Continue(()) => continue,
                ControlFlow::Break(Err(err)) => return Err(serde::de::Error::custom(err)),
                ControlFlow::Break(Ok(err)) => {
                    document_id_extraction_error = Some(err);
                    continue;
                }
            }
        }

        // return previously detected errors
        if attribute_limit_reached {
            return Ok(Err(UserError::AttributeLimitReached));
        }
        if let Some(document_id_extraction_error) = document_id_extraction_error {
            return Ok(Ok(Err(document_id_extraction_error)));
        }

        Ok(Ok(match docid {
            Some(docid) => Ok(docid),
            None => Err(DocumentIdExtractionError::MissingDocumentId),
        }))
    }
}

struct NestedPrimaryKeyVisitor<'a, 'bump> {
    components: &'a str,
    bump: &'bump Bump,
}

impl<'de, 'a, 'bump: 'de> Visitor<'de> for NestedPrimaryKeyVisitor<'a, 'bump> {
    type Value = std::result::Result<Option<DeOrBumpStr<'de, 'bump>>, DocumentIdExtractionError>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut docid = None;
        while let Some(((matched_component, right), _)) = map.next_key_seed(ComponentsSeed {
            name: self.components,
            visitor: serde::de::IgnoredAny,
        })? {
            let value: &'de RawValue = map.next_value()?;

            match match_component(matched_component, right, value, self.bump, &mut docid) {
                ControlFlow::Continue(()) => continue,
                ControlFlow::Break(Err(err)) => return Err(serde::de::Error::custom(err)),
                ControlFlow::Break(Ok(err)) => return Ok(Err(err)),
            }
        }
        Ok(Ok(docid))
    }
}

/// Either a `&'de str` or a `&'bump str`.
pub enum DeOrBumpStr<'de, 'bump: 'de> {
    /// Lifetime of the deserializer
    De(&'de str),
    /// Lifetime of the allocator
    Bump(&'bump str),
}

impl<'de, 'bump: 'de> DeOrBumpStr<'de, 'bump> {
    /// Returns a `&'bump str`, possibly allocating to extend its lifetime.
    pub fn to_bump(&self, bump: &'bump Bump) -> &'bump str {
        match self {
            DeOrBumpStr::De(de) => bump.alloc_str(de),
            DeOrBumpStr::Bump(bump) => bump,
        }
    }

    /// Returns a `&'de str`.
    ///
    /// This function never allocates because `'bump: 'de`.
    pub fn to_de(&self) -> &'de str {
        match self {
            DeOrBumpStr::De(de) => de,
            DeOrBumpStr::Bump(bump) => bump,
        }
    }
}

struct ComponentsSeed<'a, V> {
    name: &'a str,
    visitor: V,
}

impl<'de, 'a, V: Visitor<'de>> DeserializeSeed<'de> for ComponentsSeed<'a, V> {
    type Value = ((&'a str, &'a str), V::Value);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ComponentsSeedVisitor<'a, V> {
            name: &'a str,
            visitor: V,
        }

        impl<'a, V> ComponentsSeedVisitor<'a, V> {
            fn match_str(&self, v: &str) -> (&'a str, &'a str) {
                let p = PrimaryKey::Nested { name: self.name };
                for (name, right) in p.possible_level_names() {
                    if name == v {
                        return (name, right);
                    }
                }
                ("", self.name)
            }
        }

        impl<'de, 'a, V: Visitor<'de>> Visitor<'de> for ComponentsSeedVisitor<'a, V> {
            type Value = ((&'a str, &'a str), V::Value);
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "expecting a string")
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let matched = self.match_str(v);
                let inner = self.visitor.visit_borrowed_str(v)?;
                Ok((matched, inner))
            }

            fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let matched = self.match_str(v);
                let inner = self.visitor.visit_str(v)?;

                Ok((matched, inner))
            }
        }
        deserializer
            .deserialize_str(ComponentsSeedVisitor { name: self.name, visitor: self.visitor })
    }
}

struct MutFieldIdMapVisitor<'a, Mapper: MutFieldIdMapper>(&'a mut Mapper);

impl<'de, 'a, Mapper: MutFieldIdMapper> Visitor<'de> for MutFieldIdMapVisitor<'a, Mapper> {
    type Value = (Option<FieldId>, &'a mut Mapper);

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "expecting a string")
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok((self.0.insert(v), self.0))
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok((self.0.insert(v), self.0))
    }
}

pub struct FieldIdMapVisitor<'a, Mapper: FieldIdMapper>(pub &'a Mapper);

impl<'de, 'a, Mapper: FieldIdMapper> Visitor<'de> for FieldIdMapVisitor<'a, Mapper> {
    type Value = Option<FieldId>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "expecting a string")
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(self.0.id(v))
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(self.0.id(v))
    }
}
pub struct DocumentIdVisitor<'indexer>(pub &'indexer Bump);

impl<'de, 'indexer: 'de> Visitor<'de> for DocumentIdVisitor<'indexer> {
    type Value = std::result::Result<DeOrBumpStr<'de, 'indexer>, DocumentIdExtractionError>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "an integer or a string")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(validate_document_id_str(v)
            .ok_or_else(|| {
                DocumentIdExtractionError::InvalidDocumentId(UserError::InvalidDocumentId {
                    document_id: serde_json::Value::String(v.to_owned()),
                })
            })
            .map(DeOrBumpStr::De))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let v = self.0.alloc_str(v);
        Ok(match self.visit_borrowed_str(v)? {
            Ok(_) => Ok(DeOrBumpStr::Bump(v)),
            Err(err) => Err(err),
        })
    }

    fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        use std::fmt::Write as _;

        let mut out = bumpalo::collections::String::new_in(self.0);
        write!(&mut out, "{v}").unwrap();
        Ok(Ok(DeOrBumpStr::Bump(out.into_bump_str())))
    }

    fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        use std::fmt::Write as _;

        let mut out = bumpalo::collections::String::new_in(self.0);
        write!(&mut out, "{v}").unwrap();
        Ok(Ok(DeOrBumpStr::Bump(out.into_bump_str())))
    }
}

pub fn match_component<'de, 'indexer: 'de>(
    first_level_name: &str,
    right: &str,
    value: &'de RawValue,
    bump: &'indexer Bump,
    docid: &mut Option<DeOrBumpStr<'de, 'indexer>>,
) -> ControlFlow<Result<DocumentIdExtractionError, serde_json::Error>, ()> {
    if first_level_name.is_empty() {
        return ControlFlow::Continue(());
    }

    let value = if right.is_empty() {
        match value.deserialize_any(DocumentIdVisitor(bump)).map_err(|_err| {
            DocumentIdExtractionError::InvalidDocumentId(UserError::InvalidDocumentId {
                document_id: serde_json::to_value(value).unwrap(),
            })
        }) {
            Ok(Ok(value)) => value,
            Ok(Err(err)) | Err(err) => return ControlFlow::Break(Ok(err)),
        }
    } else {
        // if right is not empty, recursively extract right components from value
        let res = value.deserialize_map(NestedPrimaryKeyVisitor { components: right, bump });
        match res {
            Ok(Ok(Some(value))) => value,
            Ok(Ok(None)) => return ControlFlow::Continue(()),
            Ok(Err(err)) => return ControlFlow::Break(Ok(err)),
            Err(err) if err.is_data() => return ControlFlow::Continue(()), // we expected the field to be a map, but it was not and that's OK.
            Err(err) => return ControlFlow::Break(Err(err)),
        }
    };
    if let Some(_previous_value) = docid.replace(value) {
        return ControlFlow::Break(Ok(DocumentIdExtractionError::TooManyDocumentIds(2)));
    }
    ControlFlow::Continue(())
}

pub struct DeserrRawValue<'a> {
    value: &'a RawValue,
    alloc: &'a Bump,
}

impl<'a> DeserrRawValue<'a> {
    pub fn new_in(value: &'a RawValue, alloc: &'a Bump) -> Self {
        Self { value, alloc }
    }
}

pub struct DeserrRawVec<'a> {
    vec: RawVec<'a>,
    alloc: &'a Bump,
}

impl<'a> deserr::Sequence for DeserrRawVec<'a> {
    type Value = DeserrRawValue<'a>;

    type Iter = DeserrRawVecIter<'a>;

    fn len(&self) -> usize {
        self.vec.len()
    }

    fn into_iter(self) -> Self::Iter {
        DeserrRawVecIter { it: self.vec.into_iter(), alloc: self.alloc }
    }
}

pub struct DeserrRawVecIter<'a> {
    it: bumparaw_collections::vec::iter::IntoIter<'a>,
    alloc: &'a Bump,
}

impl<'a> Iterator for DeserrRawVecIter<'a> {
    type Item = DeserrRawValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.it.next()?;
        Some(DeserrRawValue { value: next, alloc: self.alloc })
    }
}

pub struct DeserrRawMap<'a> {
    map: bumparaw_collections::RawMap<'a, FxBuildHasher>,
    alloc: &'a Bump,
}

impl<'a> deserr::Map for DeserrRawMap<'a> {
    type Value = DeserrRawValue<'a>;

    type Iter = DeserrRawMapIter<'a>;

    fn len(&self) -> usize {
        self.map.len()
    }

    fn remove(&mut self, _key: &str) -> Option<Self::Value> {
        unimplemented!()
    }

    fn into_iter(self) -> Self::Iter {
        DeserrRawMapIter { it: self.map.into_iter(), alloc: self.alloc }
    }
}

pub struct DeserrRawMapIter<'a> {
    it: bumparaw_collections::map::iter::IntoIter<'a>,
    alloc: &'a Bump,
}

impl<'a> Iterator for DeserrRawMapIter<'a> {
    type Item = (String, DeserrRawValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let (name, value) = self.it.next()?;
        Some((name.to_string(), DeserrRawValue { value, alloc: self.alloc }))
    }
}

impl<'a> deserr::IntoValue for DeserrRawValue<'a> {
    type Sequence = DeserrRawVec<'a>;

    type Map = DeserrRawMap<'a>;

    fn kind(&self) -> deserr::ValueKind {
        self.value.deserialize_any(DeserrKindVisitor).unwrap()
    }

    fn into_value(self) -> deserr::Value<Self> {
        self.value.deserialize_any(DeserrRawValueVisitor { alloc: self.alloc }).unwrap()
    }
}

pub struct DeserrKindVisitor;

impl<'de> Visitor<'de> for DeserrKindVisitor {
    type Value = deserr::ValueKind;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "any value")
    }

    fn visit_bool<E>(self, _v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::ValueKind::Boolean)
    }

    fn visit_i64<E>(self, _v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::ValueKind::NegativeInteger)
    }

    fn visit_u64<E>(self, _v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::ValueKind::Integer)
    }

    fn visit_f64<E>(self, _v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::ValueKind::Float)
    }

    fn visit_str<E>(self, _v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::ValueKind::String)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::ValueKind::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::ValueKind::Null)
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, _seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        Ok(deserr::ValueKind::Sequence)
    }

    fn visit_map<A>(self, _map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        Ok(deserr::ValueKind::Map)
    }
}

pub struct DeserrRawValueVisitor<'a> {
    alloc: &'a Bump,
}

impl<'de> Visitor<'de> for DeserrRawValueVisitor<'de> {
    type Value = deserr::Value<DeserrRawValue<'de>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "any value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::Boolean(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::NegativeInteger(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::Integer(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::Float(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::String(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::String(v))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(deserr::Value::Null)
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut raw_vec = RawVec::new_in(self.alloc);
        while let Some(next) = seq.next_element()? {
            raw_vec.push(next);
        }
        Ok(deserr::Value::Sequence(DeserrRawVec { vec: raw_vec, alloc: self.alloc }))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let _ = map;
        Err(serde::de::Error::invalid_type(serde::de::Unexpected::Map, &self))
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        let _ = data;
        Err(serde::de::Error::invalid_type(serde::de::Unexpected::Enum, &self))
    }
}
