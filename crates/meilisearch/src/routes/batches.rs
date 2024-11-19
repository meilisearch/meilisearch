use actix_web::{
    web::{self, Data},
    HttpResponse,
};
use deserr::actix_web::AwebQueryParameter;
use index_scheduler::{IndexScheduler, Query};
use meilisearch_types::{
    batch_view::BatchView, batches::BatchId, deserr::DeserrQueryParamError, error::ResponseError,
    keys::actions,
};
use serde::Serialize;

use crate::extractors::{authentication::GuardedData, sequential_extractor::SeqHandler};

use super::{tasks::TasksFilterQuery, ActionPolicy};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(SeqHandler(get_batches))))
        .service(web::resource("/{batch_id}").route(web::get().to(SeqHandler(get_batch))));
}

async fn get_batch(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    batch_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let batch_uid_string = batch_uid.into_inner();

    let batch_uid: BatchId = match batch_uid_string.parse() {
        Ok(id) => id,
        Err(_e) => {
            return Err(
                index_scheduler::Error::InvalidBatchUid { batch_uid: batch_uid_string }.into()
            )
        }
    };

    let query = index_scheduler::Query { uids: Some(vec![batch_uid]), ..Query::default() };
    let filters = index_scheduler.filters();
    let (batches, _) = index_scheduler.get_batches_from_authorized_indexes(query, filters)?;

    if let Some(batch) = batches.first() {
        let task_view = BatchView::from_batch(batch);
        Ok(HttpResponse::Ok().json(task_view))
    } else {
        Err(index_scheduler::Error::BatchNotFound(batch_uid).into())
    }
}

#[derive(Debug, Serialize)]
pub struct AllBatches {
    results: Vec<BatchView>,
    total: u64,
    limit: u32,
    from: Option<u32>,
    next: Option<u32>,
}

async fn get_batches(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TasksFilterQuery, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    let mut params = params.into_inner();
    // We +1 just to know if there is more after this "page" or not.
    params.limit.0 = params.limit.0.saturating_add(1);
    let limit = params.limit.0;
    let query = params.into_query();

    let filters = index_scheduler.filters();
    let (tasks, total) = index_scheduler.get_batches_from_authorized_indexes(query, filters)?;
    let mut results: Vec<_> = tasks.iter().map(BatchView::from_batch).collect();

    // If we were able to fetch the number +1 tasks we asked
    // it means that there is more to come.
    let next = if results.len() == limit as usize { results.pop().map(|t| t.uid) } else { None };

    let from = results.first().map(|t| t.uid);
    let tasks = AllBatches { results, limit: limit.saturating_sub(1), total, from, next };

    Ok(HttpResponse::Ok().json(tasks))
}
