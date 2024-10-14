use bumpalo::Bump;
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

pub struct DocidExtractor<'p, 'indexer, Mapper: FieldIdMapper> {
    fields_ids_map: &'p Mapper,
    primary_key: &'p PrimaryKey<'p>,
    indexer: &'indexer Bump,
}

impl<'p, 'indexer, Mapper: FieldIdMapper> DocidExtractor<'p, 'indexer, Mapper> {
    pub fn new(
        fields_ids_map: &'p Mapper,
        primary_key: &'p PrimaryKey<'p>,
        indexer: &'indexer Bump,
    ) -> Self {
        Self { fields_ids_map, primary_key, indexer }
    }
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

impl<'de, 'p, 'indexer: 'de, Mapper: MutFieldIdMapper> serde::de::Visitor<'de>
    for FieldAndDocidExtractor<'p, 'indexer, Mapper>
{
    type Value = std::result::Result<DeOrBumpStr<'de, 'indexer>, DocumentIdExtractionError>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A>(mut self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut docid = None;
        while let Some((fid, fields_ids_map)) =
            map.next_key_seed(MutFieldIdMapSeed(self.fields_ids_map))?
        {
            use serde::de::Deserializer as _;
            self.fields_ids_map = fields_ids_map;
            /// FIXME unwrap => too many fields
            let fid = fid.unwrap();

            match self.primary_key {
                PrimaryKey::Flat { name: _, field_id } => {
                    let value: &'de RawValue = map.next_value()?;
                    if fid == *field_id {
                        let value = match value
                            .deserialize_any(DocumentIdVisitor(self.indexer))
                            .map_err(|_err| {
                                DocumentIdExtractionError::InvalidDocumentId(
                                    UserError::InvalidDocumentId {
                                        document_id: serde_json::to_value(value).unwrap(),
                                    },
                                )
                            }) {
                            Ok(Ok(value)) => value,
                            Ok(Err(err)) | Err(err) => return Ok(Err(err)),
                        };
                        if let Some(_previous_value) = docid.replace(value) {
                            return Ok(Err(DocumentIdExtractionError::TooManyDocumentIds(2)));
                        }
                    }
                }
                PrimaryKey::Nested { name } => todo!(),
            }
        }
        Ok(match docid {
            Some(docid) => Ok(docid),
            None => Err(DocumentIdExtractionError::MissingDocumentId),
        })
    }
}

impl<'de, 'p, 'indexer: 'de, Mapper: FieldIdMapper> serde::de::Visitor<'de>
    for DocidExtractor<'p, 'indexer, Mapper>
{
    type Value = std::result::Result<DeOrBumpStr<'de, 'indexer>, DocumentIdExtractionError>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut docid = None;
        while let Some(fid) = map.next_key_seed(FieldIdMapSeed(self.fields_ids_map))? {
            use serde::de::Deserializer as _;

            let Some(fid) = fid else {
                continue;
            };

            match self.primary_key {
                PrimaryKey::Flat { name: _, field_id } => {
                    let value: &'de RawValue = map.next_value()?;
                    if fid == *field_id {
                        let value = match value
                            .deserialize_any(DocumentIdVisitor(self.indexer))
                            .map_err(|_err| {
                                DocumentIdExtractionError::InvalidDocumentId(
                                    UserError::InvalidDocumentId {
                                        document_id: serde_json::to_value(value).unwrap(),
                                    },
                                )
                            }) {
                            Ok(Ok(value)) => value,
                            Ok(Err(err)) | Err(err) => return Ok(Err(err)),
                        };
                        if let Some(_previous_value) = docid.replace(value) {
                            return Ok(Err(DocumentIdExtractionError::TooManyDocumentIds(2)));
                        }
                    }
                }
                PrimaryKey::Nested { name } => todo!(),
            }
        }
        Ok(match docid {
            Some(docid) => Ok(docid),
            None => Err(DocumentIdExtractionError::MissingDocumentId),
        })
    }
}

pub enum DeOrBumpStr<'de, 'bump: 'de> {
    De(&'de str),
    Bump(&'bump str),
}

impl<'de, 'bump: 'de> DeOrBumpStr<'de, 'bump> {
    pub fn to_bump(&self, bump: &'bump Bump) -> &'bump str {
        match self {
            DeOrBumpStr::De(de) => bump.alloc_str(de),
            DeOrBumpStr::Bump(bump) => *bump,
        }
    }

    pub fn to_de(&self) -> &'de str {
        match self {
            DeOrBumpStr::De(de) => *de,
            DeOrBumpStr::Bump(bump) => *bump,
        }
    }
}

struct MutFieldIdMapSeed<'a, Mapper: MutFieldIdMapper>(&'a mut Mapper);

impl<'de, 'a, Mapper: MutFieldIdMapper> serde::de::DeserializeSeed<'de>
    for MutFieldIdMapSeed<'a, Mapper>
{
    type Value = (Option<FieldId>, &'a mut Mapper);

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MutFieldIdMapVisitor<'a, Mapper: MutFieldIdMapper>(&'a mut Mapper);
        impl<'de, 'a, Mapper: MutFieldIdMapper> serde::de::Visitor<'de>
            for MutFieldIdMapVisitor<'a, Mapper>
        {
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
        deserializer.deserialize_str(MutFieldIdMapVisitor(self.0))
    }
}

struct FieldIdMapSeed<'a, Mapper: FieldIdMapper>(&'a Mapper);

impl<'de, 'a, Mapper: FieldIdMapper> serde::de::DeserializeSeed<'de>
    for FieldIdMapSeed<'a, Mapper>
{
    type Value = Option<FieldId>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldIdMapVisitor<'a, Mapper: FieldIdMapper>(&'a Mapper);
        impl<'de, 'a, Mapper: FieldIdMapper> serde::de::Visitor<'de> for FieldIdMapVisitor<'a, Mapper> {
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
        deserializer.deserialize_str(FieldIdMapVisitor(self.0))
    }
}

pub struct DocumentIdVisitor<'indexer>(pub &'indexer Bump);

impl<'de, 'indexer: 'de> serde::de::Visitor<'de> for DocumentIdVisitor<'indexer> {
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

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let v = self.0.alloc_str(v);
        self.visit_borrowed_str(v)?;
        Ok(Ok(DeOrBumpStr::Bump(v)))
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

        let mut out = bumpalo::collections::String::new_in(&self.0);
        write!(&mut out, "{v}");
        Ok(Ok(DeOrBumpStr::Bump(out.into_bump_str())))
    }
}
