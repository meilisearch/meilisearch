use std::error::Error as StdError;
use std::{fmt, io};

use heed::{MdbError, Error as HeedError};
use serde_json::{Map, Value};

use crate::{DocumentId, FieldId};

pub type Object = Map<String, Value>;

#[derive(Debug)]
pub enum Error {
    InternalError(InternalError),
    IoError(io::Error),
    UserError(UserError),
}

#[derive(Debug)]
pub enum InternalError {
    DatabaseMissingEntry { db_name: &'static str, key: Option<&'static str> },
    FieldIdMapMissingEntry(FieldIdMapMissingEntry),
    IndexingMergingKeys { process: &'static str },
    SerializationError(SerializationError),
    StoreError(MdbError),
    InvalidDatabaseTyping,
    DatabaseClosing,
}

#[derive(Debug)]
pub enum SerializationError {
    Decoding { db_name: Option<&'static str> },
    Encoding { db_name: Option<&'static str> },
    InvalidNumberSerialization,
}

#[derive(Debug)]
pub enum FieldIdMapMissingEntry {
    FieldId { field_id: FieldId, from_db_name: &'static str },
    FieldName { field_name: String, from_db_name: &'static str },
}

#[derive(Debug)]
pub enum UserError {
    AttributeLimitReached,
    DocumentLimitReached,
    InvalidCriterionName { name: String },
    InvalidDocumentId { document_id: Value },
    MissingDocumentId { document: Object },
    MissingPrimaryKey,
    DatabaseSizeReached,
    NoSpaceLeftOnDevice,
    InvalidStoreFile,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        // TODO must be improved and more precise
        Error::IoError(error)
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
            HeedError::Mdb(MdbError::MapFull) => UserError(DatabaseSizeReached),
            HeedError::Mdb(MdbError::Invalid) => UserError(InvalidStoreFile),
            HeedError::Mdb(error) => InternalError(StoreError(error)),
            HeedError::Encoding => InternalError(SerializationError(Encoding { db_name: None })),
            HeedError::Decoding => InternalError(SerializationError(Decoding { db_name: None })),
            HeedError::InvalidDatabaseTyping => InternalError(InvalidDatabaseTyping),
            HeedError::DatabaseClosing => InternalError(DatabaseClosing),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InternalError(error) => write!(f, "internal: {}", error),
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
                write!(f, "missing {} in the {} database", key.unwrap_or("key"), db_name)
            },
            Self::FieldIdMapMissingEntry(error) => error.fmt(f),
            Self::IndexingMergingKeys { process } => {
                write!(f, "invalid merge while processing {}", process)
            },
            Self::SerializationError(error) => error.fmt(f),
            Self::StoreError(error) => error.fmt(f),
            Self::InvalidDatabaseTyping => HeedError::InvalidDatabaseTyping.fmt(f),
            Self::DatabaseClosing => HeedError::DatabaseClosing.fmt(f),
        }
    }
}

impl StdError for InternalError {}

impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::AttributeLimitReached => f.write_str("maximum number of attributes reached"),
            Self::DocumentLimitReached => f.write_str("maximum number of documents reached"),
            Self::InvalidCriterionName { name } => write!(f, "invalid criterion {}", name),
            Self::InvalidDocumentId { document_id } => {
                let json = serde_json::to_string(document_id).unwrap();
                write!(f, "document identifier is invalid {}", json)
            },
            Self::MissingDocumentId { document } => {
                let json = serde_json::to_string(document).unwrap();
                write!(f, "document doesn't have an identifier {}", json)
            },
            Self::MissingPrimaryKey => f.write_str("missing primary key"),
            Self::DatabaseSizeReached => f.write_str("database size reached"),
            // TODO where can we find it instead of writing the text ourselves?
            Self::NoSpaceLeftOnDevice => f.write_str("no space left on device"),
            Self::InvalidStoreFile => f.write_str("store file is not a valid database file"),
        }
    }
}

impl StdError for UserError {}

impl fmt::Display for FieldIdMapMissingEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::FieldId { field_id, from_db_name } => {
                write!(f, "unknown field id {} coming from {} database", field_id, from_db_name)
            },
            Self::FieldName { field_name, from_db_name } => {
                write!(f, "unknown field name {} coming from {} database", field_name, from_db_name)
            },
        }
    }
}

impl StdError for FieldIdMapMissingEntry {}

impl fmt::Display for SerializationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Decoding { db_name: Some(name) } => {
                write!(f, "decoding from the {} database failed", name)
            },
            Self::Decoding { db_name: None } => f.write_str("decoding failed"),
            Self::Encoding { db_name: Some(name) } => {
                write!(f, "encoding into the {} database failed", name)
            },
            Self::Encoding { db_name: None } => f.write_str("encoding failed"),
            Self::InvalidNumberSerialization => f.write_str("number is not a valid finite number"),
        }
    }
}

impl StdError for SerializationError {}
