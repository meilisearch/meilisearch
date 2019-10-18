mod customs_update;
mod documents_addition;
mod documents_deletion;
mod schema_update;
mod synonyms_addition;
mod synonyms_deletion;

pub use self::customs_update::{apply_customs_update, push_customs_update};
pub use self::documents_addition::{apply_documents_addition, DocumentsAddition};
pub use self::documents_deletion::{apply_documents_deletion, DocumentsDeletion};
pub use self::schema_update::{apply_schema_update, push_schema_update};
pub use self::synonyms_addition::{apply_synonyms_addition, SynonymsAddition};
pub use self::synonyms_deletion::{apply_synonyms_deletion, SynonymsDeletion};

use std::cmp;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use log::debug;
use serde::{Deserialize, Serialize};
use zlmdb::Result as ZResult;

use crate::{store, DocumentId, MResult, RankedMap};
use meilidb_schema::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Update {
    Schema(Schema),
    Customs(Vec<u8>),
    DocumentsAddition(Vec<serde_json::Value>),
    DocumentsDeletion(Vec<DocumentId>),
    SynonymsAddition(BTreeMap<String, Vec<String>>),
    SynonymsDeletion(BTreeMap<String, Option<Vec<String>>>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateType {
    Schema { schema: Schema },
    Customs,
    DocumentsAddition { number: usize },
    DocumentsDeletion { number: usize },
    SynonymsAddition { number: usize },
    SynonymsDeletion { number: usize },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DetailedDuration {
    pub main: Duration,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UpdateResult {
    pub update_id: u64,
    pub update_type: UpdateType,
    pub result: Result<(), String>,
    pub detailed_duration: DetailedDuration,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum UpdateStatus {
    Enqueued,
    Processed(UpdateResult),
    Unknown,
}

pub fn update_status(
    reader: &zlmdb::RoTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    update_id: u64,
) -> MResult<UpdateStatus> {
    match updates_results_store.update_result(reader, update_id)? {
        Some(result) => Ok(UpdateStatus::Processed(result)),
        None => {
            if updates_store.contains(reader, update_id)? {
                Ok(UpdateStatus::Enqueued)
            } else {
                Ok(UpdateStatus::Unknown)
            }
        }
    }
}

pub fn next_update_id(
    writer: &mut zlmdb::RwTxn,
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
    writer: &mut zlmdb::RwTxn,
    index: store::Index,
) -> MResult<Option<UpdateResult>> {
    let (update_id, update) = match index.updates.pop_front(writer)? {
        Some(value) => value,
        None => return Ok(None),
    };

    debug!("Processing update number {}", update_id);

    let (update_type, result, duration) = match update {
        Update::Schema(schema) => {
            let start = Instant::now();

            let update_type = UpdateType::Schema {
                schema: schema.clone(),
            };
            let result = apply_schema_update(writer, index.main, &schema);

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

            let ranked_map = match index.main.ranked_map(writer)? {
                Some(ranked_map) => ranked_map,
                None => RankedMap::default(),
            };

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
                ranked_map,
                documents,
            );

            (update_type, result, start.elapsed())
        }
        Update::DocumentsDeletion(documents) => {
            let start = Instant::now();

            let ranked_map = match index.main.ranked_map(writer)? {
                Some(ranked_map) => ranked_map,
                None => RankedMap::default(),
            };

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
                ranked_map,
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
    };

    debug!(
        "Processed update number {} {:?} {:?}",
        update_id, update_type, result
    );

    let detailed_duration = DetailedDuration { main: duration };
    let status = UpdateResult {
        update_id,
        update_type,
        result: result.map_err(|e| e.to_string()),
        detailed_duration,
    };

    index
        .updates_results
        .put_update_result(writer, update_id, &status)?;

    Ok(Some(status))
}
