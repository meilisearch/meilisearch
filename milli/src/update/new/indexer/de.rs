use std::ops::ControlFlow;

use bumpalo::Bump;
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
        let mut docid = None;

        while let Some(((level_name, right), (fid, fields_ids_map))) =
            map.next_key_seed(ComponentsSeed {
                name: self.primary_key.name(),
                visitor: MutFieldIdMapVisitor(self.fields_ids_map),
            })?
        {
            let Some(fid) = fid else {
                return Ok(Err(crate::UserError::AttributeLimitReached));
            };
            self.fields_ids_map = fields_ids_map;

            let value: &'de RawValue = map.next_value()?;

            match match_component(level_name, right, value, self.indexer, &mut docid) {
                ControlFlow::Continue(()) => continue,
                ControlFlow::Break(Err(err)) => return Err(serde::de::Error::custom(err)),
                ControlFlow::Break(Ok(err)) => return Ok(Ok(Err(err))),
            }
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
