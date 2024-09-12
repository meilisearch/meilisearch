use std::borrow::Cow;
use std::iter;
use std::result::Result as StdResult;

use serde_json::{from_str, Value};

use crate::update::new::{CowStr, TopLevelMap};
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

    pub fn name(&self) -> &str {
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

    /// Returns the document ID based on the primary and
    /// search for it recursively in zero-copy-deserialized documents.
    pub fn document_id_from_top_level_map<'p>(
        &self,
        document: &TopLevelMap<'p>,
    ) -> Result<StdResult<CowStr<'p>, DocumentIdExtractionError>> {
        fn get_docid<'p>(
            document: &TopLevelMap<'p>,
            primary_key: &[&str],
        ) -> Result<StdResult<CowStr<'p>, DocumentIdExtractionError>> {
            match primary_key {
                [] => unreachable!("arrrgh"), // would None be ok?
                [primary_key] => match document.0.get(*primary_key) {
                    Some(value) => match from_str::<u64>(value.get()) {
                        Ok(value) => Ok(Ok(CowStr(Cow::Owned(value.to_string())))),
                        Err(_) => match from_str(value.get()) {
                            Ok(document_id) => Ok(Ok(document_id)),
                            Err(e) => Ok(Err(DocumentIdExtractionError::InvalidDocumentId(
                                UserError::SerdeJson(e),
                            ))),
                        },
                    },
                    None => Ok(Err(DocumentIdExtractionError::MissingDocumentId)),
                },
                [head, tail @ ..] => match document.0.get(*head) {
                    Some(value) => {
                        let document = from_str(value.get()).map_err(InternalError::SerdeJson)?;
                        get_docid(&document, tail)
                    }
                    None => Ok(Err(DocumentIdExtractionError::MissingDocumentId)),
                },
            }
        }

        /// TODO do not allocate a vec everytime here
        let primary_key: Vec<_> = self.name().split(PRIMARY_KEY_SPLIT_SYMBOL).collect();
        get_docid(document, &primary_key)
    }

    /// Returns an `Iterator` that gives all the possible fields names the primary key
    /// can have depending of the first level name and depth of the objects.
    pub fn possible_level_names(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
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

fn validate_document_id(document_id: &str) -> Option<&str> {
    if !document_id.is_empty()
        && document_id.chars().all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_'))
    {
        Some(document_id)
    } else {
        None
    }
}

pub fn validate_document_id_value(document_id: Value) -> StdResult<String, UserError> {
    match document_id {
        Value::String(string) => match validate_document_id(&string) {
            Some(s) if s.len() == string.len() => Ok(string),
            Some(s) => Ok(s.to_string()),
            None => Err(UserError::InvalidDocumentId { document_id: Value::String(string) }),
        },
        Value::Number(number) if !number.is_f64() => Ok(number.to_string()),
        content => Err(UserError::InvalidDocumentId { document_id: content }),
    }
}
