mod documents_addition;
mod documents_deletion;
mod schema_update;

pub use self::documents_addition::{DocumentsAddition, apply_documents_addition};
pub use self::documents_deletion::{DocumentsDeletion, apply_documents_deletion};
pub use self::schema_update::apply_schema_update;

use std::time::{Duration, Instant};

use log::debug;
use serde::{Serialize, Deserialize};

use crate::{store, Error, MResult, DocumentId, RankedMap};
use crate::error::UnsupportedOperation;
use meilidb_schema::Schema;

#[derive(Debug, Serialize, Deserialize)]
pub enum Update {
    SchemaUpdate(Schema),
    DocumentsAddition(Vec<rmpv::Value>),
    DocumentsDeletion(Vec<DocumentId>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateType {
    SchemaUpdate { schema: Schema },
    DocumentsAddition { number: usize },
    DocumentsDeletion { number: usize },
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

pub fn update_status<T: rkv::Readable>(
    reader: &T,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    update_id: u64,
) -> MResult<UpdateStatus>
{
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

pub fn biggest_update_id(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
) -> MResult<Option<u64>>
{
    let last_update_id = updates_store.last_update_id(writer)?;
    let last_update_id = last_update_id.map(|(n, _)| n);

    let last_update_results_id = updates_results_store.last_update_id(writer)?;
    let last_update_results_id = last_update_results_id.map(|(n, _)| n);

    let max = last_update_id.max(last_update_results_id);

    Ok(max)
}

pub fn push_schema_update(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    schema: Schema,
) -> MResult<u64>
{
    let last_update_id = biggest_update_id(writer, updates_store, updates_results_store)?;
    let last_update_id = last_update_id.map_or(0, |n| n + 1);

    let update = Update::SchemaUpdate(schema);
    let update_id = updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn push_documents_addition<D: serde::Serialize>(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    addition: Vec<D>,
) -> MResult<u64>
{
    let mut values = Vec::with_capacity(addition.len());
    for add in addition {
        let vec = rmp_serde::to_vec_named(&add)?;
        let add = rmp_serde::from_read(&vec[..])?;
        values.push(add);
    }

    let last_update_id = biggest_update_id(writer, updates_store, updates_results_store)?;
    let last_update_id = last_update_id.map_or(0, |n| n + 1);

    let update = Update::DocumentsAddition(values);
    let update_id = updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn push_documents_deletion(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    deletion: Vec<DocumentId>,
) -> MResult<u64>
{
    let last_update_id = biggest_update_id(writer, updates_store, updates_results_store)?;
    let last_update_id = last_update_id.map_or(0, |n| n + 1);

    let update = Update::DocumentsDeletion(deletion);
    let update_id = updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn update_task(
    writer: &mut rkv::Writer,
    index: store::Index,
    mut callback: Option<impl FnOnce(UpdateResult)>,
) -> MResult<bool>
{
    let (update_id, update) = match index.updates.pop_front(writer)? {
        Some(value) => value,
        None => return Ok(false),
    };

    let (update_type, result, duration) = match update {
        Update::SchemaUpdate(schema) => {
            let start = Instant::now();
            let update_type = UpdateType::SchemaUpdate { schema: schema.clone() };
            let result = apply_schema_update(writer, index.main, &schema);

            (update_type, result, start.elapsed())
        },
        Update::DocumentsAddition(documents) => {
            let start = Instant::now();

            let ranked_map = match index.main.ranked_map(writer)? {
                Some(ranked_map) => ranked_map,
                None => RankedMap::default(),
            };

            let update_type = UpdateType::DocumentsAddition { number: documents.len() };

            let result = apply_documents_addition(
                writer,
                index.main,
                index.documents_fields,
                index.postings_lists,
                index.docs_words,
                ranked_map,
                documents,
            );

            (update_type, result, start.elapsed())
        },
        Update::DocumentsDeletion(documents) => {
            let start = Instant::now();

            let ranked_map = match index.main.ranked_map(writer)? {
                Some(ranked_map) => ranked_map,
                None => RankedMap::default(),
            };

            let update_type = UpdateType::DocumentsDeletion { number: documents.len() };

            let result = apply_documents_deletion(
                writer,
                index.main,
                index.documents_fields,
                index.postings_lists,
                index.docs_words,
                ranked_map,
                documents,
            );

            (update_type, result, start.elapsed())
        },
    };

    debug!("Processed update number {} {:?} {:?}", update_id, update_type, result);

    let detailed_duration = DetailedDuration { main: duration };
    let status = UpdateResult {
        update_id,
        update_type,
        result: result.map_err(|e| e.to_string()),
        detailed_duration,
    };

    index.updates_results.put_update_result(writer, update_id, &status)?;

    if let Some(callback) = callback.take() {
        (callback)(status);
    }

    Ok(true)
}
