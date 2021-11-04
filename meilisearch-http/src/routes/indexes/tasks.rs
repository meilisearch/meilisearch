use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use meilisearch_lib::MeiliSearch;
use meilisearch_tasks::task_store::TaskFilter;
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::task::{TaskListResponse, TaskResponse};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(get_all_tasks_status)))
        .service(web::resource("{task_id}").route(web::get().to(get_task_status)));
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateParam {
    index_uid: String,
    task_id: u64,
}

pub async fn get_task_status(
    meilisearch: GuardedData<Private, MeiliSearch>,
    index_uid: web::Path<UpdateParam>,
) -> Result<HttpResponse, ResponseError> {
    let params = index_uid.into_inner();
    let mut filter = TaskFilter::default();
    filter.filter_index(params.index_uid);
    let task: TaskResponse = meilisearch
        .get_task(params.task_id, Some(filter))
        .await?
        .into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Ok().json(task))
}

pub async fn get_all_tasks_status(
    meilisearch: GuardedData<Private, MeiliSearch>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let mut filter = TaskFilter::default();
    filter.filter_index(index_uid.into_inner());
    let tasks: TaskListResponse = meilisearch
        .list_tasks(Some(filter), None, None)
        .await?
        .into_iter()
        .map(TaskResponse::from)
        .collect::<Vec<_>>()
        .into();

    debug!("returns: {:?}", tasks);
    Ok(HttpResponse::Ok().json(tasks))
}
