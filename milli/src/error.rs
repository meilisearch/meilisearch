use std::io;

use crate::{DocumentId, FieldId};

pub enum Error {
    InternalError(InternalError),
    IoError(io::Error),
    UserError(UserError),
}

pub enum InternalError {
    DatabaseMissingEntry(DatabaseMissingEntry),
    FieldIdMapMissingEntry(FieldIdMapMissingEntry),
    IndexingMergingKeys(IndexingMergingKeys),
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

}
