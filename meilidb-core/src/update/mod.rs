mod clear_all;
mod customs_update;
mod documents_addition;
mod documents_deletion;
mod schema_update;
mod stop_words_addition;
mod stop_words_deletion;
mod synonyms_addition;
mod synonyms_deletion;

pub use self::clear_all::{apply_clear_all, push_clear_all};
pub use self::customs_update::{apply_customs_update, push_customs_update};
pub use self::documents_addition::{apply_documents_addition, DocumentsAddition};
pub use self::documents_deletion::{apply_documents_deletion, DocumentsDeletion};
pub use self::schema_update::{apply_schema_update, push_schema_update};
pub use self::stop_words_addition::{apply_stop_words_addition, StopWordsAddition};
pub use self::stop_words_deletion::{apply_stop_words_deletion, StopWordsDeletion};
pub use self::synonyms_addition::{apply_synonyms_addition, SynonymsAddition};
pub use self::synonyms_deletion::{apply_synonyms_deletion, SynonymsDeletion};

use std::cmp;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use heed::Result as ZResult;
use log::debug;
use serde::{Deserialize, Serialize};

use crate::{store, DocumentId, MResult};
use meilidb_schema::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Update {
    ClearAll,
    Schema(Schema),
    Customs(Vec<u8>),
    DocumentsAddition(Vec<serde_json::Value>),
    DocumentsDeletion(Vec<DocumentId>),
    SynonymsAddition(BTreeMap<String, Vec<String>>),
    SynonymsDeletion(BTreeMap<String, Option<Vec<String>>>),
    StopWordsAddition(BTreeSet<String>),
    StopWordsDeletion(BTreeSet<String>),
}

impl Update {
    pub fn update_type(&self) -> UpdateType {
        match self {
            Update::ClearAll => UpdateType::ClearAll,
            Update::Schema(schema) => UpdateType::Schema {
                schema: schema.clone(),
            },
            Update::Customs(_) => UpdateType::Customs,
            Update::DocumentsAddition(addition) => UpdateType::DocumentsAddition {
                number: addition.len(),
            },
            Update::DocumentsDeletion(deletion) => UpdateType::DocumentsDeletion {
                number: deletion.len(),
            },
            Update::SynonymsAddition(addition) => UpdateType::SynonymsAddition {
                number: addition.len(),
            },
            Update::SynonymsDeletion(deletion) => UpdateType::SynonymsDeletion {
                number: deletion.len(),
            },
            Update::StopWordsAddition(addition) => UpdateType::StopWordsAddition {
                number: addition.len(),
            },
            Update::StopWordsDeletion(deletion) => UpdateType::StopWordsDeletion {
                number: deletion.len(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateType {
    ClearAll,
    Schema { schema: Schema },
    Customs,
    DocumentsAddition { number: usize },
    DocumentsDeletion { number: usize },
    SynonymsAddition { number: usize },
    SynonymsDeletion { number: usize },
    StopWordsAddition { number: usize },
    StopWordsDeletion { number: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedDuration {
    pub main: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedUpdateResult {
    pub update_id: u64,
    pub update_type: UpdateType,
    pub result: Result<(), String>,
    pub detailed_duration: DetailedDuration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueuedUpdateResult {
    pub update_id: u64,
    pub update_type: UpdateType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateStatus {
    Enqueued(EnqueuedUpdateResult),
    Processed(ProcessedUpdateResult),
    Unknown,
}

pub fn update_status(
    reader: &heed::RoTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    update_id: u64,
) -> MResult<UpdateStatus> {
    match updates_results_store.update_result(reader, update_id)? {
        Some(result) => Ok(UpdateStatus::Processed(result)),
        None => {
            if let Some(update) = updates_store.get(reader, update_id)? {
                Ok(UpdateStatus::Enqueued(EnqueuedUpdateResult {
                    update_id,
                    update_type: update.update_type(),
                }))
            } else {
                Ok(UpdateStatus::Unknown)
            }
        }
    }
}

pub fn next_update_id(
    writer: &mut heed::RwTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
) -> ZResult<u64> {
    let last_update_id = updates_store.last_update_id(writer)?;
    let last_update_id = last_update_id.map(|(n, _)| n);

    let last_update_results_id = updates_results_store.last_update_id(writer)?;
    let last_update_results_id = last_update_results_id.map(|(n, _)| n);

    let max_update_id = cmp::max(last_update_id, last_update_results_id);
    let new_update_id = max_update_id.map_or(0, |n| n + 1);

    Ok(new_update_id)
}

pub fn update_task(
    writer: &mut heed::RwTxn,
    index: store::Index,
    update_id: u64,
    update: Update,
) -> MResult<ProcessedUpdateResult> {
    debug!("Processing update number {}", update_id);

    let (update_type, result, duration) = match update {
        Update::ClearAll => {
            let start = Instant::now();

            let update_type = UpdateType::ClearAll;
            let result = apply_clear_all(
                writer,
                index.main,
                index.documents_fields,
                index.documents_fields_counts,
                index.postings_lists,
                index.docs_words,
            );

            (update_type, result, start.elapsed())
        }
        Update::Schema(schema) => {
            let start = Instant::now();

            let update_type = UpdateType::Schema {
                schema: schema.clone(),
            };
            let result = apply_schema_update(
                writer,
                &schema,
                index.main,
                index.documents_fields,
                index.documents_fields_counts,
                index.postings_lists,
                index.docs_words,
            );

            (update_type, result, start.elapsed())
        }
        Update::Customs(customs) => {
            let start = Instant::now();

            let update_type = UpdateType::Customs;
            let result = apply_customs_update(writer, index.main, &customs).map_err(Into::into);

            (update_type, result, start.elapsed())
        }
        Update::DocumentsAddition(documents) => {
            let start = Instant::now();

            let update_type = UpdateType::DocumentsAddition {
                number: documents.len(),
            };

            let result = apply_documents_addition(
                writer,
                index.main,
                index.documents_fields,
                index.documents_fields_counts,
                index.postings_lists,
                index.docs_words,
                documents,
            );

            (update_type, result, start.elapsed())
        }
        Update::DocumentsDeletion(documents) => {
            let start = Instant::now();

            let update_type = UpdateType::DocumentsDeletion {
                number: documents.len(),
            };

            let result = apply_documents_deletion(
                writer,
                index.main,
                index.documents_fields,
                index.documents_fields_counts,
                index.postings_lists,
                index.docs_words,
                documents,
            );

            (update_type, result, start.elapsed())
        }
        Update::SynonymsAddition(synonyms) => {
            let start = Instant::now();

            let update_type = UpdateType::SynonymsAddition {
                number: synonyms.len(),
            };

            let result = apply_synonyms_addition(writer, index.main, index.synonyms, synonyms);

            (update_type, result, start.elapsed())
        }
        Update::SynonymsDeletion(synonyms) => {
            let start = Instant::now();

            let update_type = UpdateType::SynonymsDeletion {
                number: synonyms.len(),
            };

            let result = apply_synonyms_deletion(writer, index.main, index.synonyms, synonyms);

            (update_type, result, start.elapsed())
        }
        Update::StopWordsAddition(stop_words) => {
            let start = Instant::now();

            let update_type = UpdateType::StopWordsAddition {
                number: stop_words.len(),
            };

            let result =
                apply_stop_words_addition(writer, index.main, index.postings_lists, stop_words);

            (update_type, result, start.elapsed())
        }
        Update::StopWordsDeletion(stop_words) => {
            let start = Instant::now();

            let update_type = UpdateType::StopWordsDeletion {
                number: stop_words.len(),
            };

            let result = apply_stop_words_deletion(
                writer,
                index.main,
                index.documents_fields,
                index.documents_fields_counts,
                index.postings_lists,
                index.docs_words,
                stop_words,
            );

            (update_type, result, start.elapsed())
        }
    };

    debug!(
        "Processed update number {} {:?} {:?}",
        update_id, update_type, result
    );

    let detailed_duration = DetailedDuration { main: duration };
    let status = ProcessedUpdateResult {
        update_id,
        update_type,
        result: result.map_err(|e| e.to_string()),
        detailed_duration,
    };

    Ok(status)
}
