mod clear_all;
mod customs_update;
mod documents_addition;
mod documents_deletion;
mod settings_update;
mod helpers;

pub use self::clear_all::{apply_clear_all, push_clear_all};
pub use self::customs_update::{apply_customs_update, push_customs_update};
pub use self::documents_addition::{apply_documents_addition, apply_documents_partial_addition, DocumentsAddition};
pub use self::documents_deletion::{apply_documents_deletion, DocumentsDeletion};
pub use self::helpers::{index_value, value_to_string, value_to_number, discover_document_id, extract_document_id};
pub use self::settings_update::{apply_settings_update, push_settings_update};

use std::cmp;
use std::time::Instant;

use chrono::{DateTime, Utc};
use fst::{IntoStreamer, Streamer};
use heed::Result as ZResult;
use indexmap::IndexMap;
use log::debug;
use sdset::Set;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use meilisearch_error::ErrorCode;
use meilisearch_types::DocumentId;

use crate::{store, MResult, RankedMap};
use crate::database::{MainT, UpdateT};
use crate::settings::SettingsUpdate;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    data: UpdateData,
    enqueued_at: DateTime<Utc>,
}

impl Update {
    fn clear_all() -> Update {
        Update {
            data: UpdateData::ClearAll,
            enqueued_at: Utc::now(),
        }
    }

    fn customs(data: Vec<u8>) -> Update {
        Update {
            data: UpdateData::Customs(data),
            enqueued_at: Utc::now(),
        }
    }

    fn documents_addition(primary_key: Option<String>, documents: Vec<IndexMap<String, Value>>) -> Update {
        Update {
            data: UpdateData::DocumentsAddition{ documents, primary_key },
            enqueued_at: Utc::now(),
        }
    }

    fn documents_partial(primary_key: Option<String>, documents: Vec<IndexMap<String, Value>>) -> Update {
        Update {
            data: UpdateData::DocumentsPartial{ documents, primary_key },
            enqueued_at: Utc::now(),
        }
    }

    fn documents_deletion(data: Vec<String>) -> Update {
        Update {
            data: UpdateData::DocumentsDeletion(data),
            enqueued_at: Utc::now(),
        }
    }

    fn settings(data: SettingsUpdate) -> Update {
        Update {
            data: UpdateData::Settings(Box::new(data)),
            enqueued_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateData {
    ClearAll,
    Customs(Vec<u8>),
    // (primary key, documents)
    DocumentsAddition {
        primary_key: Option<String>,
        documents: Vec<IndexMap<String, Value>>
    },
    DocumentsPartial {
        primary_key: Option<String>,
        documents: Vec<IndexMap<String, Value>>,
    },
    DocumentsDeletion(Vec<String>),
    Settings(Box<SettingsUpdate>)
}

impl UpdateData {
    pub fn update_type(&self) -> UpdateType {
        match self {
            UpdateData::ClearAll => UpdateType::ClearAll,
            UpdateData::Customs(_) => UpdateType::Customs,
            UpdateData::DocumentsAddition{ documents, .. } => UpdateType::DocumentsAddition {
                number: documents.len(),
            },
            UpdateData::DocumentsPartial{ documents, .. } => UpdateType::DocumentsPartial {
                number: documents.len(),
            },
            UpdateData::DocumentsDeletion(deletion) => UpdateType::DocumentsDeletion {
                number: deletion.len(),
            },
            UpdateData::Settings(update) => UpdateType::Settings {
                settings: update.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum UpdateType {
    ClearAll,
    Customs,
    DocumentsAddition { number: usize },
    DocumentsPartial { number: usize },
    DocumentsDeletion { number: usize },
    Settings { settings: Box<SettingsUpdate> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_link: Option<String>,
    pub duration: f64, // in seconds
    pub enqueued_at: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnqueuedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub enqueued_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum UpdateStatus {
    Enqueued {
        #[serde(flatten)]
        content: EnqueuedUpdateResult,
    },
    Failed {
        #[serde(flatten)]
        content: ProcessedUpdateResult,
    },
    Processed {
        #[serde(flatten)]
        content: ProcessedUpdateResult,
    },
}

pub fn update_status(
    update_reader: &heed::RoTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    update_id: u64,
) -> MResult<Option<UpdateStatus>> {
    match updates_results_store.update_result(update_reader, update_id)? {
        Some(result) => {
            if result.error.is_some() {
                Ok(Some(UpdateStatus::Failed { content: result }))
            } else {
                Ok(Some(UpdateStatus::Processed { content: result }))
            }
        },
        None => match updates_store.get(update_reader, update_id)? {
            Some(update) => Ok(Some(UpdateStatus::Enqueued {
                content: EnqueuedUpdateResult {
                    update_id,
                    update_type: update.data.update_type(),
                    enqueued_at: update.enqueued_at,
                },
            })),
            None => Ok(None),
        },
    }
}

pub fn next_update_id(
    update_writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
) -> ZResult<u64> {
    let last_update = updates_store.last_update(update_writer)?;
    let last_update = last_update.map(|(n, _)| n);

    let last_update_results_id = updates_results_store.last_update(update_writer)?;
    let last_update_results_id = last_update_results_id.map(|(n, _)| n);

    let max_update_id = cmp::max(last_update, last_update_results_id);
    let new_update_id = max_update_id.map_or(0, |n| n + 1);

    Ok(new_update_id)
}

pub fn update_task(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    update_id: u64,
    update: Update,
) -> MResult<ProcessedUpdateResult> {
    debug!("Processing update number {}", update_id);

    let Update { enqueued_at, data } = update;

    let (update_type, result, duration) = match data {
        UpdateData::ClearAll => {
            let start = Instant::now();

            let update_type = UpdateType::ClearAll;
            let result = apply_clear_all(writer, index);

            (update_type, result, start.elapsed())
        }
        UpdateData::Customs(customs) => {
            let start = Instant::now();

            let update_type = UpdateType::Customs;
            let result = apply_customs_update(writer, index.main, &customs).map_err(Into::into);

            (update_type, result, start.elapsed())
        }
        UpdateData::DocumentsAddition { documents, primary_key } => {
            let start = Instant::now();

            let update_type = UpdateType::DocumentsAddition {
                number: documents.len(),
            };

            let result = apply_documents_addition(writer, index, documents, primary_key);

            (update_type, result, start.elapsed())
        }
        UpdateData::DocumentsPartial{ documents, primary_key } => {
            let start = Instant::now();

            let update_type = UpdateType::DocumentsPartial {
                number: documents.len(),
            };

            let result = apply_documents_partial_addition(writer, index, documents, primary_key);

            (update_type, result, start.elapsed())
        }
        UpdateData::DocumentsDeletion(documents) => {
            let start = Instant::now();

            let update_type = UpdateType::DocumentsDeletion {
                number: documents.len(),
            };

            let result = apply_documents_deletion(writer, index, documents);

            (update_type, result, start.elapsed())
        }
        UpdateData::Settings(settings) => {
            let start = Instant::now();

            let update_type = UpdateType::Settings {
                settings: settings.clone(),
            };

            let result = apply_settings_update(
                writer,
                index,
                *settings,
            );

            (update_type, result, start.elapsed())
        }
    };

    debug!(
        "Processed update number {} {:?} {:?}",
        update_id, update_type, result
    );

    let status = ProcessedUpdateResult {
        update_id,
        update_type,
        error: result.as_ref().map_err(|e| e.to_string()).err(),
        error_code: result.as_ref().map_err(|e| e.error_name()).err(),
        error_type: result.as_ref().map_err(|e| e.error_type()).err(),
        error_link: result.as_ref().map_err(|e| e.error_url()).err(),
        duration: duration.as_secs_f64(),
        enqueued_at,
        processed_at: Utc::now(),
    };

    Ok(status)
}

fn compute_short_prefixes<A>(
    writer: &mut heed::RwTxn<MainT>,
    words_fst: &fst::Set<A>,
    index: &store::Index,
) -> MResult<()>
where A: AsRef<[u8]>,
{
    // clear the prefixes
    let pplc_store = index.prefix_postings_lists_cache;
    pplc_store.clear(writer)?;

    for prefix_len in 1..=2 {
        // compute prefixes and store those in the PrefixPostingsListsCache store.
        let mut previous_prefix: Option<([u8; 4], Vec<_>)> = None;
        let mut stream = words_fst.into_stream();
        while let Some(input) = stream.next() {

            // We skip the prefixes that are shorter than the current length
            // we want to cache (<). We must ignore the input when it is exactly the
            // same word as the prefix because if we match exactly on it we need
            // to consider it as an exact match and not as a prefix (=).
            if input.len() <= prefix_len { continue }

            if let Some(postings_list) = index.postings_lists.postings_list(writer, input)?.map(|p| p.matches.into_owned()) {
                let prefix = &input[..prefix_len];

                let mut arr_prefix = [0; 4];
                arr_prefix[..prefix_len].copy_from_slice(prefix);

                match previous_prefix {
                    Some((ref mut prev_prefix, ref mut prev_pl)) if *prev_prefix != arr_prefix => {
                        prev_pl.sort_unstable();
                        prev_pl.dedup();

                        if let Ok(prefix) = std::str::from_utf8(&prev_prefix[..prefix_len]) {
                            debug!("writing the prefix of {:?} of length {}", prefix, prev_pl.len());
                        }

                        let pls = Set::new_unchecked(&prev_pl);
                        pplc_store.put_prefix_postings_list(writer, *prev_prefix, &pls)?;

                        *prev_prefix = arr_prefix;
                        prev_pl.clear();
                        prev_pl.extend_from_slice(&postings_list);
                    },
                    Some((_, ref mut prev_pl)) => prev_pl.extend_from_slice(&postings_list),
                    None => previous_prefix = Some((arr_prefix, postings_list.to_vec())),
                }
            }
        }

        // write the last prefix postings lists
        if let Some((prev_prefix, mut prev_pl)) = previous_prefix.take() {
            prev_pl.sort_unstable();
            prev_pl.dedup();

            let pls = Set::new_unchecked(&prev_pl);
            pplc_store.put_prefix_postings_list(writer, prev_prefix, &pls)?;
        }
    }

    Ok(())
}

fn cache_document_ids_sorted(
    writer: &mut heed::RwTxn<MainT>,
    ranked_map: &RankedMap,
    index: &store::Index,
    document_ids: &mut [DocumentId],
) -> MResult<()> {
    crate::bucket_sort::placeholder_document_sort(document_ids, index, writer, ranked_map)?;
    index.main.put_sorted_document_ids_cache(writer, &document_ids)
}
