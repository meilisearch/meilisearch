mod clear_all;
mod customs_update;
mod documents_addition;
mod documents_deletion;
mod schema_update;
mod stop_words_addition;
mod stop_words_deletion;
mod synonyms_update;

pub use self::clear_all::{apply_clear_all, push_clear_all};
pub use self::customs_update::{apply_customs_update, push_customs_update};
pub use self::documents_addition::{
    apply_documents_addition, apply_documents_partial_addition, DocumentsAddition,
};
pub use self::documents_deletion::{apply_documents_deletion, DocumentsDeletion};
pub use self::schema_update::{apply_schema_update, push_schema_update};
pub use self::stop_words_addition::{apply_stop_words_addition, StopWordsAddition};
pub use self::stop_words_deletion::{apply_stop_words_deletion, StopWordsDeletion};
pub use self::synonyms_update::{apply_synonyms_update, SynonymsUpdate};

use std::cmp;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

use chrono::{DateTime, Utc};
use heed::Result as ZResult;
use log::debug;
use serde::{Deserialize, Serialize};
use fst::{IntoStreamer, Streamer};
use sdset::Set;

use crate::{store, DocumentId, MResult};
use crate::database::{MainT, UpdateT};
use meilisearch_schema::Schema;

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

    fn schema(data: Schema) -> Update {
        Update {
            data: UpdateData::Schema(data),
            enqueued_at: Utc::now(),
        }
    }

    fn customs(data: Vec<u8>) -> Update {
        Update {
            data: UpdateData::Customs(data),
            enqueued_at: Utc::now(),
        }
    }

    fn documents_addition(data: Vec<HashMap<String, serde_json::Value>>) -> Update {
        Update {
            data: UpdateData::DocumentsAddition(data),
            enqueued_at: Utc::now(),
        }
    }

    fn documents_partial(data: Vec<HashMap<String, serde_json::Value>>) -> Update {
        Update {
            data: UpdateData::DocumentsPartial(data),
            enqueued_at: Utc::now(),
        }
    }

    fn documents_deletion(data: Vec<DocumentId>) -> Update {
        Update {
            data: UpdateData::DocumentsDeletion(data),
            enqueued_at: Utc::now(),
        }
    }

    fn synonyms_update(data: BTreeMap<String, Vec<String>>) -> Update {
        Update {
            data: UpdateData::SynonymsUpdate(data),
            enqueued_at: Utc::now(),
        }
    }

    fn stop_words_addition(data: BTreeSet<String>) -> Update {
        Update {
            data: UpdateData::StopWordsAddition(data),
            enqueued_at: Utc::now(),
        }
    }

    fn stop_words_deletion(data: BTreeSet<String>) -> Update {
        Update {
            data: UpdateData::StopWordsDeletion(data),
            enqueued_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateData {
    ClearAll,
    Schema(Schema),
    Customs(Vec<u8>),
    DocumentsAddition(Vec<HashMap<String, serde_json::Value>>),
    DocumentsPartial(Vec<HashMap<String, serde_json::Value>>),
    DocumentsDeletion(Vec<DocumentId>),
    SynonymsUpdate(BTreeMap<String, Vec<String>>),
    StopWordsAddition(BTreeSet<String>),
    StopWordsDeletion(BTreeSet<String>),
}

impl UpdateData {
    pub fn update_type(&self) -> UpdateType {
        match self {
            UpdateData::ClearAll => UpdateType::ClearAll,
            UpdateData::Schema(_) => UpdateType::Schema,
            UpdateData::Customs(_) => UpdateType::Customs,
            UpdateData::DocumentsAddition(addition) => UpdateType::DocumentsAddition {
                number: addition.len(),
            },
            UpdateData::DocumentsPartial(addition) => UpdateType::DocumentsPartial {
                number: addition.len(),
            },
            UpdateData::DocumentsDeletion(deletion) => UpdateType::DocumentsDeletion {
                number: deletion.len(),
            },
            UpdateData::SynonymsUpdate(addition) => UpdateType::SynonymsUpdate {
                number: addition.len(),
            },
            UpdateData::StopWordsAddition(addition) => UpdateType::StopWordsAddition {
                number: addition.len(),
            },
            UpdateData::StopWordsDeletion(deletion) => UpdateType::StopWordsDeletion {
                number: deletion.len(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum UpdateType {
    ClearAll,
    Schema,
    Customs,
    DocumentsAddition { number: usize },
    DocumentsPartial { number: usize },
    DocumentsDeletion { number: usize },
    SynonymsUpdate { number: usize },
    StopWordsAddition { number: usize },
    StopWordsDeletion { number: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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

pub fn update_task<'a, 'b>(
    writer: &'a mut heed::RwTxn<'b, MainT>,
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
        UpdateData::Schema(schema) => {
            let start = Instant::now();

            let update_type = UpdateType::Schema;
            let result = apply_schema_update(writer, &schema, index);

            (update_type, result, start.elapsed())
        }
        UpdateData::Customs(customs) => {
            let start = Instant::now();

            let update_type = UpdateType::Customs;
            let result = apply_customs_update(writer, index.main, &customs).map_err(Into::into);

            (update_type, result, start.elapsed())
        }
        UpdateData::DocumentsAddition(documents) => {
            let start = Instant::now();

            let update_type = UpdateType::DocumentsAddition {
                number: documents.len(),
            };

            let result = apply_documents_addition(writer, index, documents);

            (update_type, result, start.elapsed())
        }
        UpdateData::DocumentsPartial(documents) => {
            let start = Instant::now();

            let update_type = UpdateType::DocumentsPartial {
                number: documents.len(),
            };

            let result = apply_documents_partial_addition(writer, index, documents);

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
        UpdateData::SynonymsUpdate(synonyms) => {
            let start = Instant::now();

            let update_type = UpdateType::SynonymsUpdate {
                number: synonyms.len(),
            };

            let result = apply_synonyms_update(writer, index.main, index.synonyms, synonyms);

            (update_type, result, start.elapsed())
        }
        UpdateData::StopWordsAddition(stop_words) => {
            let start = Instant::now();

            let update_type = UpdateType::StopWordsAddition {
                number: stop_words.len(),
            };

            let result =
                apply_stop_words_addition(writer, index.main, index.postings_lists, stop_words);

            (update_type, result, start.elapsed())
        }
        UpdateData::StopWordsDeletion(stop_words) => {
            let start = Instant::now();

            let update_type = UpdateType::StopWordsDeletion {
                number: stop_words.len(),
            };

            let result = apply_stop_words_deletion(writer, index, stop_words);

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
        error: result.map_err(|e| e.to_string()).err(),
        duration: duration.as_secs_f64(),
        enqueued_at,
        processed_at: Utc::now(),
    };

    Ok(status)
}

fn compute_short_prefixes(writer: &mut heed::RwTxn<MainT>, index: &store::Index) -> MResult<()> {
    // retrieve the words fst to compute all those prefixes
    let words_fst = match index.main.words_fst(writer)? {
        Some(fst) => fst,
        None => return Ok(()),
    };

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
