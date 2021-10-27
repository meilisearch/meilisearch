use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Deserializer, Value};
use tempfile::NamedTempFile;

use crate::index_controller::dump_actor::loaders::compat::{asc_ranking_rule, desc_ranking_rule};
use crate::index_controller::dump_actor::Metadata;
use crate::index_controller::updates::status::{
    Aborted, Enqueued, Failed, Processed, Processing, UpdateResult, UpdateStatus,
};
use crate::index_controller::updates::store::dump::UpdateEntry;
use crate::index_controller::updates::store::Update;
use crate::options::IndexerOpts;

use super::v3;

/// The dump v2 reads the dump folder and patches all the needed file to make it compatible with a
/// dump v3, then calls the dump v3 to actually handle the dump.
pub fn load_dump(
    meta: Metadata,
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    index_db_size: usize,
    update_db_size: usize,
    indexing_options: &IndexerOpts,
) -> anyhow::Result<()> {
    let indexes_path = src.as_ref().join("indexes");

    let dir_entries = std::fs::read_dir(indexes_path)?;
    for entry in dir_entries {
        let entry = entry?;

        // rename the index folder
        let path = entry.path();
        let new_path = patch_index_uuid_path(&path).expect("invalid index folder.");

        std::fs::rename(path, &new_path)?;

        let settings_path = new_path.join("meta.json");

        patch_settings(settings_path)?;
    }

    let update_dir = src.as_ref().join("updates");
    let update_path = update_dir.join("data.jsonl");
    patch_updates(update_dir, update_path)?;

    v3::load_dump(
        meta,
        src,
        dst,
        index_db_size,
        update_db_size,
        indexing_options,
    )
}

fn patch_index_uuid_path(path: &Path) -> Option<PathBuf> {
    let uuid = path.file_name()?.to_str()?.trim_start_matches("index-");
    let new_path = path.parent()?.join(uuid);
    Some(new_path)
}

fn patch_settings(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let mut meta_file = File::open(&path)?;
    let mut meta: Value = serde_json::from_reader(&mut meta_file)?;

    // We first deserialize the dump meta into a serde_json::Value and change
    // the custom ranking rules settings from the old format to the new format.
    if let Some(ranking_rules) = meta.pointer_mut("/settings/rankingRules") {
        patch_custom_ranking_rules(ranking_rules);
    }

    let mut meta_file = OpenOptions::new().truncate(true).write(true).open(path)?;

    serde_json::to_writer(&mut meta_file, &meta)?;

    Ok(())
}

fn patch_updates(dir: impl AsRef<Path>, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let mut output_update_file = NamedTempFile::new_in(&dir)?;
    let update_file = File::open(&path)?;

    let stream = Deserializer::from_reader(update_file).into_iter::<compat::UpdateEntry>();

    for update in stream {
        let update_entry = update?;

        let update_entry = UpdateEntry::from(update_entry);

        serde_json::to_writer(&mut output_update_file, &update_entry)?;
        output_update_file.write_all(b"\n")?;
    }

    output_update_file.flush()?;
    output_update_file.persist(path)?;

    Ok(())
}

/// Converts the ranking rules from the format `asc(_)`, `desc(_)` to the format `_:asc`, `_:desc`.
///
/// This is done for compatibility reasons, and to avoid a new dump version,
/// since the new syntax was introduced soon after the new dump version.
fn patch_custom_ranking_rules(ranking_rules: &mut Value) {
    *ranking_rules = match ranking_rules.take() {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| match value {
                Value::String(s) if s.starts_with("asc") => asc_ranking_rule(&s)
                    .map(|f| format!("{}:asc", f))
                    .map(Value::String),
                Value::String(s) if s.starts_with("desc") => desc_ranking_rule(&s)
                    .map(|f| format!("{}:desc", f))
                    .map(Value::String),
                otherwise => Some(otherwise),
            })
            .collect(),
        otherwise => otherwise,
    }
}

impl From<compat::UpdateEntry> for UpdateEntry {
    fn from(compat::UpdateEntry { uuid, update }: compat::UpdateEntry) -> Self {
        let update = match update {
            compat::UpdateStatus::Processing(meta) => UpdateStatus::Processing(meta.into()),
            compat::UpdateStatus::Enqueued(meta) => UpdateStatus::Enqueued(meta.into()),
            compat::UpdateStatus::Processed(meta) => UpdateStatus::Processed(meta.into()),
            compat::UpdateStatus::Aborted(meta) => UpdateStatus::Aborted(meta.into()),
            compat::UpdateStatus::Failed(meta) => UpdateStatus::Failed(meta.into()),
        };

        Self { uuid, update }
    }
}

impl From<compat::Failed> for Failed {
    fn from(other: compat::Failed) -> Self {
        let compat::Failed {
            from,
            error,
            failed_at,
        } = other;

        Self {
            from: from.into(),
            msg: error.message,
            code: compat::error_code_from_str(&error.error_code)
                .expect("Invalid update: Invalid error code"),
            failed_at,
        }
    }
}

impl From<compat::Aborted> for Aborted {
    fn from(other: compat::Aborted) -> Self {
        let compat::Aborted { from, aborted_at } = other;

        Self {
            from: from.into(),
            aborted_at,
        }
    }
}

impl From<compat::Processing> for Processing {
    fn from(other: compat::Processing) -> Self {
        let compat::Processing {
            from,
            started_processing_at,
        } = other;

        Self {
            from: from.into(),
            started_processing_at,
        }
    }
}

impl From<compat::Enqueued> for Enqueued {
    fn from(other: compat::Enqueued) -> Self {
        let compat::Enqueued {
            update_id,
            meta,
            enqueued_at,
            content,
        } = other;

        let meta = match meta {
            compat::UpdateMeta::DocumentsAddition {
                method,
                primary_key,
                ..
            } => {
                Update::DocumentAddition {
                    primary_key,
                    method,
                    // Just ignore if the uuid is no present. If it is needed later, an error will
                    // be thrown.
                    content_uuid: content.unwrap_or_default(),
                }
            }
            compat::UpdateMeta::ClearDocuments => Update::ClearDocuments,
            compat::UpdateMeta::DeleteDocuments { ids } => Update::DeleteDocuments(ids),
            compat::UpdateMeta::Settings(settings) => Update::Settings(settings),
        };

        Self {
            update_id,
            meta,
            enqueued_at,
        }
    }
}

impl From<compat::Processed> for Processed {
    fn from(other: compat::Processed) -> Self {
        let compat::Processed {
            from,
            success,
            processed_at,
        } = other;

        Self {
            success: success.into(),
            processed_at,
            from: from.into(),
        }
    }
}

impl From<compat::UpdateResult> for UpdateResult {
    fn from(other: compat::UpdateResult) -> Self {
        match other {
            compat::UpdateResult::DocumentsAddition(r) => Self::DocumentsAddition(r),
            compat::UpdateResult::DocumentDeletion { deleted } => {
                Self::DocumentDeletion { deleted }
            }
            compat::UpdateResult::Other => Self::Other,
        }
    }
}

/// compat structure from pre-dumpv3 meilisearch
mod compat {
    use anyhow::bail;
    use chrono::{DateTime, Utc};
    use meilisearch_error::Code;
    use milli::update::{DocumentAdditionResult, IndexDocumentsMethod};
    use serde::{Deserialize, Serialize};
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
        pub enqueued_at: DateTime<Utc>,
        pub content: Option<Uuid>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Processed {
        pub success: UpdateResult,
        pub processed_at: DateTime<Utc>,
        #[serde(flatten)]
        pub from: Processing,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Processing {
        #[serde(flatten)]
        pub from: Enqueued,
        pub started_processing_at: DateTime<Utc>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Aborted {
        #[serde(flatten)]
        pub from: Enqueued,
        pub aborted_at: DateTime<Utc>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Failed {
        #[serde(flatten)]
        pub from: Processing,
        pub error: ResponseError,
        pub failed_at: DateTime<Utc>,
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
            "index_not_accessible" => Code::OpenIndex,
            "invalid_state" => Code::InvalidState,
            "missing_primary_key" => Code::MissingPrimaryKey,
            "primary_key_already_present" => Code::PrimaryKeyAlreadyPresent,
            "invalid_request" => Code::InvalidRankingRule,
            "max_fields_limit_exceeded" => Code::MaxFieldsLimitExceeded,
            "missing_document_id" => Code::MissingDocumentId,
            "invalid_facet" => Code::Facet,
            "invalid_filter" => Code::Filter,
            "invalid_sort" => Code::Sort,
            "bad_parameter" => Code::BadParameter,
            "bad_request" => Code::BadRequest,
            "document_not_found" => Code::DocumentNotFound,
            "internal" => Code::Internal,
            "invalid_geo_field" => Code::InvalidGeoField,
            "invalid_token" => Code::InvalidToken,
            "missing_authorization_header" => Code::MissingAuthorizationHeader,
            "not_found" => Code::NotFound,
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
}
