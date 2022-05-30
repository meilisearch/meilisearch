use anyhow::bail;
use meilisearch_error::Code;
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::index::{Settings, Unchecked};

#[derive(Serialize, Deserialize)]
pub struct UpdateEntry {
    pub uuid: Uuid,
    pub update: UpdateStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateFormat {
    Json,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocumentAdditionResult {
    pub nb_documents: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateResult {
    DocumentsAddition(DocumentAdditionResult),
    DocumentDeletion { deleted: u64 },
    Other,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition {
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        primary_key: Option<String>,
    },
    ClearDocuments,
    DeleteDocuments {
        ids: Vec<String>,
    },
    Settings(Settings<Unchecked>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Enqueued {
    pub update_id: u64,
    pub meta: UpdateMeta,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    pub content: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processed {
    pub success: UpdateResult,
    #[serde(with = "time::serde::rfc3339")]
    pub processed_at: OffsetDateTime,
    #[serde(flatten)]
    pub from: Processing,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Processing {
    #[serde(flatten)]
    pub from: Enqueued,
    #[serde(with = "time::serde::rfc3339")]
    pub started_processing_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Aborted {
    #[serde(flatten)]
    pub from: Enqueued,
    #[serde(with = "time::serde::rfc3339")]
    pub aborted_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Failed {
    #[serde(flatten)]
    pub from: Processing,
    pub error: ResponseError,
    #[serde(with = "time::serde::rfc3339")]
    pub failed_at: OffsetDateTime,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum UpdateStatus {
    Processing(Processing),
    Enqueued(Enqueued),
    Processed(Processed),
    Aborted(Aborted),
    Failed(Failed),
}

type StatusCode = ();

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResponseError {
    #[serde(skip)]
    pub code: StatusCode,
    pub message: String,
    pub error_code: String,
    pub error_type: String,
    pub error_link: String,
}

pub fn error_code_from_str(s: &str) -> anyhow::Result<Code> {
    let code = match s {
        "index_creation_failed" => Code::CreateIndex,
        "index_already_exists" => Code::IndexAlreadyExists,
        "index_not_found" => Code::IndexNotFound,
        "invalid_index_uid" => Code::InvalidIndexUid,
        "invalid_state" => Code::InvalidState,
        "missing_primary_key" => Code::MissingPrimaryKey,
        "primary_key_already_present" => Code::PrimaryKeyAlreadyPresent,
        "invalid_request" => Code::InvalidRankingRule,
        "max_fields_limit_exceeded" => Code::MaxFieldsLimitExceeded,
        "missing_document_id" => Code::MissingDocumentId,
        "invalid_facet" => Code::Filter,
        "invalid_filter" => Code::Filter,
        "invalid_sort" => Code::Sort,
        "bad_parameter" => Code::BadParameter,
        "bad_request" => Code::BadRequest,
        "document_not_found" => Code::DocumentNotFound,
        "internal" => Code::Internal,
        "invalid_geo_field" => Code::InvalidGeoField,
        "invalid_token" => Code::InvalidToken,
        "missing_authorization_header" => Code::MissingAuthorizationHeader,
        "payload_too_large" => Code::PayloadTooLarge,
        "unretrievable_document" => Code::RetrieveDocument,
        "search_error" => Code::SearchDocuments,
        "unsupported_media_type" => Code::UnsupportedMediaType,
        "dump_already_in_progress" => Code::DumpAlreadyInProgress,
        "dump_process_failed" => Code::DumpProcessFailed,
        _ => bail!("unknow error code."),
    };

    Ok(code)
}
