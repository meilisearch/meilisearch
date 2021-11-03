use std::collections::BTreeSet;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::{fmt, io, str};

use heed::{Error as HeedError, MdbError};
use rayon::ThreadPoolBuildError;
use serde_json::{Map, Value};

use crate::search::ParserRule;
use crate::{CriterionError, DocumentId, FieldId, SortError};

pub type Object = Map<String, Value>;

pub fn is_reserved_keyword(keyword: &str) -> bool {
    ["_geo", "_geoDistance", "_geoPoint", "_geoRadius"].contains(&keyword)
}

#[derive(Debug)]
pub enum Error {
    InternalError(InternalError),
    IoError(io::Error),
    UserError(UserError),
}

#[derive(Debug)]
pub enum InternalError {
    DatabaseClosing,
    DatabaseMissingEntry { db_name: &'static str, key: Option<&'static str> },
    FieldIdMapMissingEntry(FieldIdMapMissingEntry),
    Fst(fst::Error),
    GrenadInvalidCompressionType,
    IndexingMergingKeys { process: &'static str },
    InvalidDatabaseTyping,
    RayonThreadPool(ThreadPoolBuildError),
    SerdeJson(serde_json::Error),
    Serialization(SerializationError),
    Store(MdbError),
    Utf8(str::Utf8Error),
}

#[derive(Debug)]
pub enum SerializationError {
    Decoding { db_name: Option<&'static str> },
    Encoding { db_name: Option<&'static str> },
    InvalidNumberSerialization,
}

#[derive(Debug)]
pub enum FieldIdMapMissingEntry {
    FieldId { field_id: FieldId, process: &'static str },
    FieldName { field_name: String, process: &'static str },
}

#[derive(Debug)]
pub enum UserError {
    AttributeLimitReached,
    CriterionError(CriterionError),
    DocumentLimitReached,
    InvalidDocumentId { document_id: Value },
    InvalidFacetsDistribution { invalid_facets_name: BTreeSet<String> },
    InvalidFilter(FilterError),
    InvalidGeoField { document_id: Value, object: Value },
    InvalidSortableAttribute { field: String, valid_fields: BTreeSet<String> },
    SortRankingRuleMissing,
    InvalidStoreFile,
    MaxDatabaseSizeReached,
    MissingDocumentId { primary_key: String, document: Object },
    MissingPrimaryKey,
    NoSpaceLeftOnDevice,
    PrimaryKeyCannotBeChanged(String),
    SerdeJson(serde_json::Error),
    SortError(SortError),
    UnknownInternalDocumentId { document_id: DocumentId },
}

#[derive(Debug)]
pub enum FilterError {
    InvalidAttribute { field: String, valid_fields: BTreeSet<String> },
    ReservedKeyword { field: String, context: Option<String> },
    Syntax(pest::error::Error<ParserRule>),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        // TODO must be improved and more precise
        Error::IoError(error)
    }
}

impl From<fst::Error> for Error {
    fn from(error: fst::Error) -> Error {
        Error::InternalError(InternalError::Fst(error))
    }
}

impl<E> From<grenad::Error<E>> for Error
where
    Error: From<E>,
{
    fn from(error: grenad::Error<E>) -> Error {
        match error {
            grenad::Error::Io(error) => Error::IoError(error),
            grenad::Error::Merge(error) => Error::from(error),
            grenad::Error::InvalidCompressionType => {
                Error::InternalError(InternalError::GrenadInvalidCompressionType)
            }
        }
    }
}

impl From<str::Utf8Error> for Error {
    fn from(error: str::Utf8Error) -> Error {
        Error::InternalError(InternalError::Utf8(error))
    }
}

impl From<Infallible> for Error {
    fn from(_error: Infallible) -> Error {
        unreachable!()
    }
}

impl From<HeedError> for Error {
    fn from(error: HeedError) -> Error {
        use self::Error::*;
        use self::InternalError::*;
        use self::SerializationError::*;
        use self::UserError::*;

        match error {
            HeedError::Io(error) => Error::from(error),
            HeedError::Mdb(MdbError::MapFull) => UserError(MaxDatabaseSizeReached),
            HeedError::Mdb(MdbError::Invalid) => UserError(InvalidStoreFile),
            HeedError::Mdb(error) => InternalError(Store(error)),
            HeedError::Encoding => InternalError(Serialization(Encoding { db_name: None })),
            HeedError::Decoding => InternalError(Serialization(Decoding { db_name: None })),
            HeedError::InvalidDatabaseTyping => InternalError(InvalidDatabaseTyping),
            HeedError::DatabaseClosing => InternalError(DatabaseClosing),
        }
    }
}

impl From<ThreadPoolBuildError> for Error {
    fn from(error: ThreadPoolBuildError) -> Error {
        Error::InternalError(InternalError::RayonThreadPool(error))
    }
}

impl From<FieldIdMapMissingEntry> for Error {
    fn from(error: FieldIdMapMissingEntry) -> Error {
        Error::InternalError(InternalError::FieldIdMapMissingEntry(error))
    }
}

impl From<InternalError> for Error {
    fn from(error: InternalError) -> Error {
        Error::InternalError(error)
    }
}

impl From<UserError> for Error {
    fn from(error: UserError) -> Error {
        Error::UserError(error)
    }
}

impl From<FilterError> for Error {
    fn from(error: FilterError) -> Error {
        Error::UserError(UserError::InvalidFilter(error))
    }
}

impl From<SerializationError> for Error {
    fn from(error: SerializationError) -> Error {
        Error::InternalError(InternalError::Serialization(error))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InternalError(error) => write!(f, "internal: {}.", error),
            Self::IoError(error) => error.fmt(f),
            Self::UserError(error) => error.fmt(f),
        }
    }
}

impl StdError for Error {}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::DatabaseMissingEntry { db_name, key } => {
                write!(f, "Missing {} in the {} database.", key.unwrap_or("key"), db_name)
            }
            Self::FieldIdMapMissingEntry(error) => error.fmt(f),
            Self::Fst(error) => error.fmt(f),
            Self::GrenadInvalidCompressionType => {
                f.write_str("Invalid compression type have been specified to grenad.")
            }
            Self::IndexingMergingKeys { process } => {
                write!(f, "Invalid merge while processing {}.", process)
            }
            Self::Serialization(error) => error.fmt(f),
            Self::InvalidDatabaseTyping => HeedError::InvalidDatabaseTyping.fmt(f),
            Self::RayonThreadPool(error) => error.fmt(f),
            Self::SerdeJson(error) => error.fmt(f),
            Self::DatabaseClosing => HeedError::DatabaseClosing.fmt(f),
            Self::Store(error) => error.fmt(f),
            Self::Utf8(error) => error.fmt(f),
        }
    }
}

impl StdError for InternalError {}

impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AttributeLimitReached => f.write_str("Maximum number of attributes reached."),
            Self::CriterionError(error) => write!(f, "{}", error),
            Self::DocumentLimitReached => f.write_str("Maximum number of documents reached."),
            Self::InvalidFacetsDistribution { invalid_facets_name } => {
                let name_list =
                    invalid_facets_name.iter().map(AsRef::as_ref).collect::<Vec<_>>().join(", ");
                write!(
                    f,
                    "Invalid facet distribution, the fields `{}` are not set as filterable.",
                    name_list
                )
            }
            Self::InvalidFilter(error) => error.fmt(f),
            Self::InvalidGeoField { document_id, object } => {
                let document_id = match document_id {
                    Value::String(id) => id.clone(),
                    _ => document_id.to_string(),
                };
                let object = match object {
                    Value::String(id) => id.clone(),
                    _ => object.to_string(),
                };
                write!(
                    f,
                    "The document with the id: `{}` contains an invalid _geo field: `{}`.",
                    document_id, object
                )
            },
            Self::InvalidDocumentId { document_id } => {
                let document_id = match document_id {
                    Value::String(id) => id.clone(),
                    _ => document_id.to_string(),
                };
                write!(
                    f,
                    "Document identifier `{}` is invalid. \
A document identifier can be of type integer or string, \
only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_).",
                    document_id
                )
            }
            Self::InvalidSortableAttribute { field, valid_fields } => {
                let valid_names =
                    valid_fields.iter().map(AsRef::as_ref).collect::<Vec<_>>().join(", ");
                write!(
                    f,
                    "Attribute `{}` is not sortable. Available sortable attributes are: `{}`.",
                    field, valid_names
                )
            }
            Self::SortRankingRuleMissing => f.write_str(
                "The sort ranking rule must be specified in the \
ranking rules settings to use the sort parameter at search time.",
            ),
            Self::MissingDocumentId { primary_key, document } => {
                let json = serde_json::to_string(document).unwrap();
                write!(f, "Document doesn't have a `{}` attribute: `{}`.", primary_key, json)
            }
            Self::MissingPrimaryKey => f.write_str("Missing primary key."),
            Self::MaxDatabaseSizeReached => f.write_str("Maximum database size has been reached."),
            Self::NoSpaceLeftOnDevice => f.write_str("There is no more space left on the device. Consider increasing the size of the disk/partition."),
            Self::InvalidStoreFile => f.write_str("The database file is in an invalid state."),
            Self::PrimaryKeyCannotBeChanged(primary_key) => {
                write!(f, "Index already has a primary key: `{}`.", primary_key)
            }
            Self::SerdeJson(error) => error.fmt(f),
            Self::SortError(error) => write!(f, "{}", error),
            Self::UnknownInternalDocumentId { document_id } => {
                write!(f, "An unknown internal document id have been used: `{}`.", document_id)
            }
        }
    }
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidAttribute { field, valid_fields } => write!(
                f,
                "Attribute `{}` is not filterable. Available filterable attributes are: `{}`.",
                field,
                valid_fields
                    .clone()
                    .into_iter()
                    .reduce(|left, right| left + "`, `" + &right)
                    .unwrap_or_default()
            ),
            Self::ReservedKeyword { field, context: Some(context) } => {
                write!(
                    f,
                    "`{}` is a reserved keyword and thus can't be used as a filter expression. {}",
                    field, context
                )
            }
            Self::ReservedKeyword { field, context: None } => {
                write!(
                    f,
                    "`{}` is a reserved keyword and thus can't be used as a filter expression.",
                    field
                )
            }
            Self::Syntax(syntax_helper) => {
                write!(f, "Invalid syntax for the filter parameter: `{}`.", syntax_helper)
            }
        }
    }
}

impl StdError for UserError {}

impl fmt::Display for FieldIdMapMissingEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::FieldId { field_id, process } => {
                write!(f, "unknown field id {} coming from the {} process", field_id, process)
            }
            Self::FieldName { field_name, process } => {
                write!(f, "unknown field name {} coming from the {} process", field_name, process)
            }
        }
    }
}

impl StdError for FieldIdMapMissingEntry {}

impl fmt::Display for SerializationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Decoding { db_name: Some(name) } => {
                write!(f, "decoding from the {} database failed", name)
            }
            Self::Decoding { db_name: None } => f.write_str("decoding failed"),
            Self::Encoding { db_name: Some(name) } => {
                write!(f, "encoding into the {} database failed", name)
            }
            Self::Encoding { db_name: None } => f.write_str("encoding failed"),
            Self::InvalidNumberSerialization => f.write_str("number is not a valid finite number"),
        }
    }
}

impl StdError for SerializationError {}
