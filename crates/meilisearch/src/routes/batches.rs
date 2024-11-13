use actix_web::{
    web::{self, Data},
    HttpResponse,
};
use index_scheduler::{IndexScheduler, Query};
use meilisearch_types::{
    batches::BatchId, error::ResponseError, keys::actions, task_view::TaskView,
};

use crate::extractors::{authentication::GuardedData, sequential_extractor::SeqHandler};

use super::ActionPolicy;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg
        // .service(
        //     web::resource("")
        //         .route(web::get().to(SeqHandler(get_tasks)))
        // )
        .service(web::resource("/{batch_id}").route(web::get().to(SeqHandler(get_batch))));
}

async fn get_task(
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
    let (tasks, _) = index_scheduler.get_tasks_from_authorized_indexes(query, filters)?;

    if let Some(task) = tasks.first() {
        let task_view = TaskView::from_task(task);
        Ok(HttpResponse::Ok().json(task_view))
    } else {
        Err(index_scheduler::Error::TaskNotFound(batch_uid).into())
    }
}
