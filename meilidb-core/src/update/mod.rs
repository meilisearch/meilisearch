mod documents_addition;
mod documents_deletion;

pub use self::documents_addition::{DocumentsAddition, apply_documents_addition};
pub use self::documents_deletion::{DocumentsDeletion, apply_documents_deletion};

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use serde::{Serialize, Deserialize};
use crate::{store, Error, MResult, DocumentId, RankedMap};

#[derive(Serialize, Deserialize)]
pub enum Update {
    DocumentsAddition(Vec<rmpv::Value>),
    DocumentsDeletion(Vec<DocumentId>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum UpdateType {
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

pub fn push_documents_addition<D: serde::Serialize>(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    addition: Vec<D>,
) -> Result<u64, Error>
{
    let mut values = Vec::with_capacity(addition.len());
    for add in addition {
        let vec = rmp_serde::to_vec_named(&add)?;
        let add = rmp_serde::from_read(&vec[..])?;
        values.push(add);
    }

    let update = Update::DocumentsAddition(values);
    Ok(updates_store.push_back(writer, &update)?)
}

pub fn push_documents_deletion(
    writer: &mut rkv::Writer,
    updates_store: store::Updates,
    deletion: Vec<DocumentId>,
) -> Result<u64, Error>
{
    let update = Update::DocumentsDeletion(deletion);
    Ok(updates_store.push_back(writer, &update)?)
}

pub fn update_task(
    rkv: Arc<RwLock<rkv::Rkv>>,
    index: store::Index,
    mut callback: Option<impl FnOnce(UpdateResult)>,
) -> MResult<()>
{
    let rkv = rkv.read().unwrap();
    let mut writer = rkv.write()?;

    if let Some((update_id, update)) = index.updates.pop_back(&mut writer)? {
        let (update_type, result, duration) = match update {
            Update::DocumentsAddition(documents) => {
                let update_type = UpdateType::DocumentsAddition { number: documents.len() };

                let schema = match index.main.schema(&writer)? {
                    Some(schema) => schema,
                    None => return Err(Error::SchemaMissing),
                };
                let ranked_map = match index.main.ranked_map(&writer)? {
                    Some(ranked_map) => ranked_map,
                    None => RankedMap::default(),
                };

                let start = Instant::now();
                let result = apply_documents_addition(
                    &mut writer,
                    index.main,
                    index.documents_fields,
                    index.postings_lists,
                    index.docs_words,
                    &schema,
                    ranked_map,
                    documents,
                );

                (update_type, result, start.elapsed())
            },
            Update::DocumentsDeletion(documents) => {
                let update_type = UpdateType::DocumentsDeletion { number: documents.len() };

                let schema = match index.main.schema(&writer)? {
                    Some(schema) => schema,
                    None => return Err(Error::SchemaMissing),
                };
                let ranked_map = match index.main.ranked_map(&writer)? {
                    Some(ranked_map) => ranked_map,
                    None => RankedMap::default(),
                };

                let start = Instant::now();
                let result = apply_documents_deletion(
                    &mut writer,
                    index.main,
                    index.documents_fields,
                    index.postings_lists,
                    index.docs_words,
                    &schema,
                    ranked_map,
                    documents,
                );

                (update_type, result, start.elapsed())
            },
        };

        let detailed_duration = DetailedDuration { main: duration };
        let status = UpdateResult {
            update_id,
            update_type,
            result: result.map_err(|e| e.to_string()),
            detailed_duration,
        };

        index.updates_results.put_update_result(&mut writer, update_id, &status)?;

        if let Some(callback) = callback.take() {
            (callback)(status);
        }
    }

    writer.commit()?;

    Ok(())
}
