use std::iter;
use std::ops::ControlFlow;
use std::result::Result as StdResult;

use bumpalo::Bump;
use serde_json::value::RawValue;
use serde_json::Value;

use crate::fields_ids_map::MutFieldIdMapper;
use crate::update::new::indexer::de::{match_component, DeOrBumpStr};
use crate::update::new::KvReaderFieldId;
use crate::{FieldId, InternalError, Object, Result, UserError};

/// The symbol used to define levels in a nested primary key.
const PRIMARY_KEY_SPLIT_SYMBOL: char = '.';

/// The default primary that is used when not specified.
pub const DEFAULT_PRIMARY_KEY: &str = "id";

/// Trait for objects that can map the name of a field to its [`FieldId`].
pub trait FieldIdMapper {
    /// Attempts to map the passed name to its [`FieldId`].
    ///
    /// `None` if the field with this name was not found.
    fn id(&self, name: &str) -> Option<FieldId>;

    fn name(&self, id: FieldId) -> Option<&str>;
}

impl<T> FieldIdMapper for &T
where
    T: FieldIdMapper,
{
    fn id(&self, name: &str) -> Option<FieldId> {
        T::id(self, name)
    }

    fn name(&self, id: FieldId) -> Option<&str> {
        T::name(self, id)
    }
}

/// A type that represent the type of primary key that has been set
/// for this index, a classic flat one or a nested one.
#[derive(Debug, Clone, Copy)]
pub enum PrimaryKey<'a> {
    Flat { name: &'a str, field_id: FieldId },
    Nested { name: &'a str },
}

pub enum DocumentIdExtractionError {
    InvalidDocumentId(UserError),
    MissingDocumentId,
    TooManyDocumentIds(usize),
}

impl<'a> PrimaryKey<'a> {
    pub fn new(path: &'a str, fields: &impl FieldIdMapper) -> Option<Self> {
        Some(if path.contains(PRIMARY_KEY_SPLIT_SYMBOL) {
            Self::Nested { name: path }
        } else {
            let field_id = fields.id(path)?;
            Self::Flat { name: path, field_id }
        })
    }

    pub fn new_or_insert(
        path: &'a str,
        fields: &mut impl MutFieldIdMapper,
    ) -> StdResult<Self, UserError> {
        Ok(if path.contains(PRIMARY_KEY_SPLIT_SYMBOL) {
            Self::Nested { name: path }
        } else {
            let field_id = fields.insert(path).ok_or(UserError::AttributeLimitReached)?;
            Self::Flat { name: path, field_id }
        })
    }

    pub fn name(&self) -> &'a str {
        match self {
            PrimaryKey::Flat { name, .. } => name,
            PrimaryKey::Nested { name } => name,
        }
    }

    pub fn document_id(
        &self,
        document: &obkv::KvReader<FieldId>,
        fields: &impl FieldIdMapper,
    ) -> Result<StdResult<String, DocumentIdExtractionError>> {
        match self {
            PrimaryKey::Flat { name: _, field_id } => match document.get(*field_id) {
                Some(document_id_bytes) => {
                    let document_id = serde_json::from_slice(document_id_bytes)
                        .map_err(InternalError::SerdeJson)?;
                    match validate_document_id_value(document_id) {
                        Ok(document_id) => Ok(Ok(document_id)),
                        Err(user_error) => {
                            Ok(Err(DocumentIdExtractionError::InvalidDocumentId(user_error)))
                        }
                    }
                }
                None => Ok(Err(DocumentIdExtractionError::MissingDocumentId)),
            },
            nested @ PrimaryKey::Nested { .. } => {
                let mut matching_documents_ids = Vec::new();
                for (first_level_name, right) in nested.possible_level_names() {
                    if let Some(field_id) = fields.id(first_level_name) {
                        if let Some(value_bytes) = document.get(field_id) {
                            let object = serde_json::from_slice(value_bytes)
                                .map_err(InternalError::SerdeJson)?;
                            fetch_matching_values(object, right, &mut matching_documents_ids);

                            if matching_documents_ids.len() >= 2 {
                                return Ok(Err(DocumentIdExtractionError::TooManyDocumentIds(
                                    matching_documents_ids.len(),
                                )));
                            }
                        }
                    }
                }

                match matching_documents_ids.pop() {
                    Some(document_id) => match validate_document_id_value(document_id) {
                        Ok(document_id) => Ok(Ok(document_id)),
                        Err(user_error) => {
                            Ok(Err(DocumentIdExtractionError::InvalidDocumentId(user_error)))
                        }
                    },
                    None => Ok(Err(DocumentIdExtractionError::MissingDocumentId)),
                }
            }
        }
    }

    pub fn extract_docid_from_db<'pl, 'bump: 'pl, Mapper: FieldIdMapper>(
        &self,
        document: &'pl KvReaderFieldId,
        db_fields_ids_map: &Mapper,
        indexer: &'bump Bump,
    ) -> Result<DeOrBumpStr<'pl, 'bump>> {
        use serde::Deserializer as _;

        match self {
            PrimaryKey::Flat { name: _, field_id } => {
                let Some(document_id) = document.get(*field_id) else {
                    return Err(InternalError::DocumentsError(
                        crate::documents::Error::InvalidDocumentFormat,
                    )
                    .into());
                };

                let document_id: &RawValue =
                    serde_json::from_slice(document_id).map_err(InternalError::SerdeJson)?;

                let document_id = document_id
                    .deserialize_any(crate::update::new::indexer::de::DocumentIdVisitor(indexer))
                    .map_err(InternalError::SerdeJson)?;

                let external_document_id = match document_id {
                    Ok(document_id) => Ok(document_id),
                    Err(_) => Err(InternalError::DocumentsError(
                        crate::documents::Error::InvalidDocumentFormat,
                    )),
                }?;

                Ok(external_document_id)
            }
            nested @ PrimaryKey::Nested { name: _ } => {
                let mut docid = None;
                for (first_level, right) in nested.possible_level_names() {
                    let Some(fid) = db_fields_ids_map.id(first_level) else { continue };

                    let Some(value) = document.get(fid) else { continue };
                    let value: &RawValue =
                        serde_json::from_slice(value).map_err(InternalError::SerdeJson)?;
                    match match_component(first_level, right, value, indexer, &mut docid) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(Ok(_)) => {
                            return Err(InternalError::DocumentsError(
                                crate::documents::Error::InvalidDocumentFormat,
                            )
                            .into())
                        }
                        ControlFlow::Break(Err(err)) => {
                            return Err(InternalError::SerdeJson(err).into())
                        }
                    }
                }
                Ok(docid.ok_or(InternalError::DocumentsError(
                    crate::documents::Error::InvalidDocumentFormat,
                ))?)
            }
        }
    }

    pub fn extract_fields_and_docid<'pl, 'bump: 'pl, Mapper: MutFieldIdMapper>(
        &self,
        document: &'pl RawValue,
        new_fields_ids_map: &mut Mapper,
        indexer: &'bump Bump,
    ) -> Result<DeOrBumpStr<'pl, 'bump>> {
        use serde::Deserializer as _;
        let res = document
            .deserialize_map(crate::update::new::indexer::de::FieldAndDocidExtractor::new(
                new_fields_ids_map,
                self,
                indexer,
            ))
            .map_err(UserError::SerdeJson)??;

        let external_document_id = match res {
            Ok(document_id) => Ok(document_id),
            Err(DocumentIdExtractionError::InvalidDocumentId(e)) => Err(e),
            Err(DocumentIdExtractionError::MissingDocumentId) => {
                Err(UserError::MissingDocumentId {
                    primary_key: self.name().to_string(),
                    document: serde_json::from_str(document.get()).unwrap(),
                })
            }
            Err(DocumentIdExtractionError::TooManyDocumentIds(_)) => {
                Err(UserError::TooManyDocumentIds {
                    primary_key: self.name().to_string(),
                    document: serde_json::from_str(document.get()).unwrap(),
                })
            }
        }?;

        Ok(external_document_id)
    }

    /// Returns an `Iterator` that gives all the possible fields names the primary key
    /// can have depending of the first level name and depth of the objects.
    pub fn possible_level_names(&self) -> impl Iterator<Item = (&'a str, &'a str)> + '_ {
        let name = self.name();
        name.match_indices(PRIMARY_KEY_SPLIT_SYMBOL)
            .map(move |(i, _)| (&name[..i], &name[i + PRIMARY_KEY_SPLIT_SYMBOL.len_utf8()..]))
            .chain(iter::once((name, "")))
    }
}

fn fetch_matching_values(value: Value, selector: &str, output: &mut Vec<Value>) {
    match value {
        Value::Object(object) => fetch_matching_values_in_object(object, selector, "", output),
        otherwise => output.push(otherwise),
    }
}

fn fetch_matching_values_in_object(
    object: Object,
    selector: &str,
    base_key: &str,
    output: &mut Vec<Value>,
) {
    for (key, value) in object {
        let base_key = if base_key.is_empty() {
            key.to_string()
        } else {
            format!("{}{}{}", base_key, PRIMARY_KEY_SPLIT_SYMBOL, key)
        };

        if starts_with(selector, &base_key) {
            match value {
                Value::Object(object) => {
                    fetch_matching_values_in_object(object, selector, &base_key, output)
                }
                value => output.push(value),
            }
        }
    }
}

fn starts_with(selector: &str, key: &str) -> bool {
    selector.strip_prefix(key).map_or(false, |tail| {
        tail.chars().next().map(|c| c == PRIMARY_KEY_SPLIT_SYMBOL).unwrap_or(true)
    })
}

// FIXME: move to a DocumentId struct

pub fn validate_document_id_str(document_id: &str) -> Option<&str> {
    if document_id.is_empty()
        || document_id.len() >= 512
        || !document_id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        None
    } else {
        Some(document_id)
    }
}

pub fn validate_document_id_value(document_id: Value) -> StdResult<String, UserError> {
    match document_id {
        Value::String(string) => match validate_document_id_str(&string) {
            Some(s) if s.len() == string.len() => Ok(string),
            Some(s) => Ok(s.to_string()),
            None => Err(UserError::InvalidDocumentId { document_id: Value::String(string) }),
        },
        // a `u64` or `i64` cannot be more than 512 bytes once converted to a string
        Value::Number(number) if !number.is_f64() => Ok(number.to_string()),
        content => Err(UserError::InvalidDocumentId { document_id: content }),
    }
}
