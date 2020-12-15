use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;

use cow_utils::CowUtils;
use either::Either;
use heed::types::{Str, OwnedType};
use indexmap::IndexMap;
use serde_json::Value;

use meilisearch_schema::{FieldId, Schema};
use meilisearch_types::DocumentId;

use crate::database::MainT;
use crate::error::{FacetError, MResult};
use crate::store::BEU16;

/// Data structure used to represent a boolean expression in the form of nested arrays.
/// Values in the outer array are and-ed together, values in the inner arrays are or-ed together.
#[derive(Debug, PartialEq)]
pub struct FacetFilter(Vec<Either<Vec<FacetKey>, FacetKey>>);

impl Deref for FacetFilter {
    type Target = Vec<Either<Vec<FacetKey>, FacetKey>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FacetFilter {
    pub fn from_str(
        s: &str,
        schema: &Schema,
        attributes_for_faceting: &[FieldId],
    ) -> MResult<FacetFilter> {
        if attributes_for_faceting.is_empty() {
            return Err(FacetError::NoAttributesForFaceting.into());
        }
        let parsed = serde_json::from_str::<Value>(s).map_err(|e| FacetError::ParsingError(e.to_string()))?;
        let mut filter = Vec::new();
        match parsed {
            Value::Array(and_exprs) => {
                if and_exprs.is_empty() {
                    return Err(FacetError::EmptyArray.into());
                }
                for expr in and_exprs {
                    match expr {
                        Value::String(s) => {
                            let key = FacetKey::from_str( &s, schema, attributes_for_faceting)?;
                            filter.push(Either::Right(key));
                        }
                        Value::Array(or_exprs) => {
                            if or_exprs.is_empty() {
                                return Err(FacetError::EmptyArray.into());
                            }
                            let mut inner = Vec::new();
                            for expr in or_exprs {
                                match expr {
                                    Value::String(s) => {
                                        let key = FacetKey::from_str( &s, schema, attributes_for_faceting)?;
                                        inner.push(key);
                                    }
                                    bad_value => return Err(FacetError::unexpected_token(&["String"], bad_value).into()),
                                }
                            }
                            filter.push(Either::Left(inner));
                        }
                        bad_value => return Err(FacetError::unexpected_token(&["Array", "String"], bad_value).into()),
                    }
                }
                Ok(Self(filter))
            }
            bad_value => Err(FacetError::unexpected_token(&["Array"], bad_value).into()),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
#[repr(C)]
pub struct FacetKey(FieldId, String);

impl FacetKey {
    pub fn new(field_id: FieldId, value: String) -> Self {
        let value = match value.cow_to_lowercase() {
                Cow::Borrowed(_) => value,
                Cow::Owned(s) => s,
        };
        Self(field_id, value)
    }

    pub fn key(&self) -> FieldId {
        self.0
    }

    pub fn value(&self) -> &str {
        &self.1
    }

    // TODO improve parser
    fn from_str(
        s: &str,
        schema: &Schema,
        attributes_for_faceting: &[FieldId],
    ) -> Result<Self, FacetError> {
        let mut split = s.splitn(2, ':');
        let key = split
            .next()
            .ok_or_else(|| FacetError::InvalidFormat(s.to_string()))?
            .trim();
        let field_id = schema
            .id(key)
            .ok_or_else(|| FacetError::AttributeNotFound(key.to_string()))?;

        if !attributes_for_faceting.contains(&field_id) {
            return Err(FacetError::attribute_not_set(
                    attributes_for_faceting
                    .iter()
                    .filter_map(|&id| schema.name(id))
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
                    key))
        }
        let value = split
            .next()
            .ok_or_else(|| FacetError::InvalidFormat(s.to_string()))?
            .trim();
        // unquoting the string if need be:
        let mut indices = value.char_indices();
        let value =  match (indices.next(), indices.last()) {
            (Some((s, '\'')), Some((e, '\''))) |
            (Some((s, '\"')), Some((e, '\"'))) => value[s + 1..e].to_string(),
            _ => value.to_string(),
        };
        Ok(Self::new(field_id, value))
    }
}

impl<'a> heed::BytesEncode<'a> for FacetKey {
    type EItem = FacetKey;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let mut buffer = Vec::with_capacity(2 + item.1.len());
        let id = BEU16::new(item.key().into());
        let id_bytes = OwnedType::bytes_encode(&id)?;
        let value_bytes = Str::bytes_encode(item.value())?;
        buffer.extend_from_slice(id_bytes.as_ref());
        buffer.extend_from_slice(value_bytes.as_ref());
        Some(Cow::Owned(buffer))
    }
}

impl<'a> heed::BytesDecode<'a> for FacetKey {
    type DItem = FacetKey;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (id_bytes, value_bytes) = bytes.split_at(2);
        let id = OwnedType::<BEU16>::bytes_decode(id_bytes)?;
        let id = id.get().into();
        let string = Str::bytes_decode(&value_bytes)?;
        Some(FacetKey(id, string.to_string()))
    }
}

pub fn add_to_facet_map(
    facet_map: &mut HashMap<FacetKey, (String, Vec<DocumentId>)>,
    field_id: FieldId,
    value: Value,
    document_id: DocumentId,
) -> Result<(), FacetError> {
    let value = match value {
        Value::String(s) => s,
        // ignore null
        Value::Null => return Ok(()),
        value => return Err(FacetError::InvalidDocumentAttribute(value.to_string())),
    };
    let key = FacetKey::new(field_id, value.clone());
    facet_map.entry(key).or_insert_with(|| (value, Vec::new())).1.push(document_id);
    Ok(())
}

pub fn facet_map_from_docids(
    rtxn: &heed::RoTxn<MainT>,
    index: &crate::Index,
    document_ids: &[DocumentId],
    attributes_for_facetting: &[FieldId],
) -> MResult<HashMap<FacetKey, (String, Vec<DocumentId>)>> {
    // A hashmap that ascociate a facet key to a pair containing the original facet attribute
    // string with it's case preserved, and a list of document ids for that facet attribute.
    let mut facet_map: HashMap<FacetKey, (String, Vec<DocumentId>)> = HashMap::new();
    for document_id in document_ids {
        for result in index
            .documents_fields
            .document_fields(rtxn, *document_id)?
        {
            let (field_id, bytes) = result?;
            if attributes_for_facetting.contains(&field_id) {
                match serde_json::from_slice(bytes)? {
                    Value::Array(values) => {
                        for v in values {
                            add_to_facet_map(&mut facet_map, field_id, v, *document_id)?;
                        }
                    }
                    v => add_to_facet_map(&mut facet_map, field_id, v, *document_id)?,
                };
            }
        }
    }
    Ok(facet_map)
}

pub fn facet_map_from_docs(
    schema: &Schema,
    documents: &HashMap<DocumentId, IndexMap<String, Value>>,
    attributes_for_facetting: &[FieldId],
) -> MResult<HashMap<FacetKey, (String, Vec<DocumentId>)>> {
    let mut facet_map = HashMap::new();
    let attributes_for_facetting = attributes_for_facetting
        .iter()
        .filter_map(|&id| schema.name(id).map(|name| (id, name)))
        .collect::<Vec<_>>();

    for (id, document) in documents {
        for (field_id, name) in &attributes_for_facetting {
            if let Some(value) = document.get(*name) {
                match value {
                    Value::Array(values) => {
                        for v in values {
                            add_to_facet_map(&mut facet_map, *field_id, v.clone(), *id)?;
                        }
                    }
                    v => add_to_facet_map(&mut facet_map, *field_id, v.clone(), *id)?,
                }
            }
        }
    }
    Ok(facet_map)
}

#[cfg(test)]
mod test {
    use super::*;
    use meilisearch_schema::Schema;

    #[test]
    fn test_facet_key() {
        let mut schema = Schema::default();
        let id = schema.insert_with_position("hello").unwrap().0;
        let facet_list = [schema.id("hello").unwrap()];
        assert_eq!(
            FacetKey::from_str("hello:12", &schema, &facet_list).unwrap(),
            FacetKey::new(id, "12".to_string())
        );
        assert_eq!(
            FacetKey::from_str("hello:\"foo bar\"", &schema, &facet_list).unwrap(),
            FacetKey::new(id, "foo bar".to_string())
        );
        assert_eq!(
            FacetKey::from_str("hello:'foo bar'", &schema, &facet_list).unwrap(),
            FacetKey::new(id, "foo bar".to_string())
        );
        // weird case
        assert_eq!(
            FacetKey::from_str("hello:blabla:machin", &schema, &facet_list).unwrap(),
            FacetKey::new(id, "blabla:machin".to_string())
        );

        assert_eq!(
            FacetKey::from_str("hello:\"\"", &schema, &facet_list).unwrap(),
            FacetKey::new(id, "".to_string())
        );

        assert_eq!(
            FacetKey::from_str("hello:'", &schema, &facet_list).unwrap(),
            FacetKey::new(id, "'".to_string())
        );
        assert_eq!(
            FacetKey::from_str("hello:''", &schema, &facet_list).unwrap(),
            FacetKey::new(id, "".to_string())
        );
        assert!(FacetKey::from_str("hello", &schema, &facet_list).is_err());
        assert!(FacetKey::from_str("toto:12", &schema, &facet_list).is_err());
    }

    #[test]
    fn test_parse_facet_array() {
        use either::Either::{Left, Right};
        let mut schema = Schema::default();
        let _id = schema.insert_with_position("hello").unwrap();
        let facet_list = [schema.id("hello").unwrap()];
        assert_eq!(
            FacetFilter::from_str("[[\"hello:12\"]]", &schema, &facet_list).unwrap(),
            FacetFilter(vec![Left(vec![FacetKey(FieldId(0), "12".to_string())])])
        );
        assert_eq!(
            FacetFilter::from_str("[\"hello:12\"]", &schema, &facet_list).unwrap(),
            FacetFilter(vec![Right(FacetKey(FieldId(0), "12".to_string()))])
        );
        assert_eq!(
            FacetFilter::from_str("[\"hello:12\", \"hello:13\"]", &schema, &facet_list).unwrap(),
            FacetFilter(vec![
                Right(FacetKey(FieldId(0), "12".to_string())),
                Right(FacetKey(FieldId(0), "13".to_string()))
            ])
        );
        assert_eq!(
            FacetFilter::from_str("[[\"hello:12\", \"hello:13\"]]", &schema, &facet_list).unwrap(),
            FacetFilter(vec![Left(vec![
                FacetKey(FieldId(0), "12".to_string()),
                FacetKey(FieldId(0), "13".to_string())
            ])])
        );
        assert_eq!(
            FacetFilter::from_str(
                "[[\"hello:12\", \"hello:13\"], \"hello:14\"]",
                &schema,
                &facet_list
            )
            .unwrap(),
            FacetFilter(vec![
                Left(vec![
                    FacetKey(FieldId(0), "12".to_string()),
                    FacetKey(FieldId(0), "13".to_string())
                ]),
                Right(FacetKey(FieldId(0), "14".to_string()))
            ])
        );

        // invalid array depths
        assert!(FacetFilter::from_str(
            "[[[\"hello:12\", \"hello:13\"], \"hello:14\"]]",
            &schema,
            &facet_list
        )
        .is_err());
        assert!(FacetFilter::from_str(
            "[[[\"hello:12\", \"hello:13\"]], \"hello:14\"]]",
            &schema,
            &facet_list
        )
        .is_err());
        assert!(FacetFilter::from_str("\"hello:14\"", &schema, &facet_list).is_err());

        // unexisting key
        assert!(FacetFilter::from_str("[\"foo:12\"]", &schema, &facet_list).is_err());

        // invalid facet key
        assert!(FacetFilter::from_str("[\"foo=12\"]", &schema, &facet_list).is_err());
        assert!(FacetFilter::from_str("[\"foo12\"]", &schema, &facet_list).is_err());
        assert!(FacetFilter::from_str("[\"\"]", &schema, &facet_list).is_err());

        // empty array error
        assert!(FacetFilter::from_str("[]", &schema, &facet_list).is_err());
        assert!(FacetFilter::from_str("[\"hello:12\", []]", &schema, &facet_list).is_err());
    }
}
