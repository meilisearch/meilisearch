use std::io;

use crate::{DocumentId, FieldId};
use heed::{MdbError, Error as HeedError};

pub type Object = serde_json::Map<String, serde_json::Value>;

pub enum Error {
    InternalError(InternalError),
    IoError(io::Error),
    UserError(UserError),
}

pub enum InternalError {
    DatabaseMissingEntry(DatabaseMissingEntry),
    FieldIdMapMissingEntry(FieldIdMapMissingEntry),
    IndexingMergingKeys(IndexingMergingKeys),
    SerializationError(SerializationError),
    StoreError(MdbError),
    InvalidDatabaseTyping,
    DatabaseClosing,
}

pub enum SerializationError {
    Decoding { db_name: Option<&'static str> },
    Encoding { db_name: Option<&'static str> },
    InvalidNumberSerialization,
}

pub enum IndexingMergingKeys {
    DocIdWordPosition,
    Document,
    MainFstDeserialization,
    WordLevelPositionDocids,
    WordPrefixLevelPositionDocids,
}

pub enum FieldIdMapMissingEntry {
    DisplayedFieldId { field_id: FieldId },
    DisplayedFieldName { field_name: String },
    FacetedFieldName { field_name: String },
    FilterableFieldName { field_name: String },
    SearchableFieldName { field_name: String },
}

pub enum DatabaseMissingEntry {
    DocumentId { internal_id: DocumentId },
    FacetValuesDocids,
    IndexCreationTime,
    IndexUpdateTime,
}

pub enum UserError {
    AttributeLimitReached,
    DocumentLimitReached,
    InvalidCriterionName { name: String },
    InvalidDocumentId { document_id: DocumentId },
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
