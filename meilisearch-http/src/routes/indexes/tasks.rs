use actix_web::{web, HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use meilisearch_error::ResponseError;
use meilisearch_lib::MeiliSearch;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::task::{TaskListView, TaskView};

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
    meilisearch: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, MeiliSearch>,
    index_uid: web::Path<UpdateParam>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(
        "Index Tasks Seen".to_string(),
        json!({ "per_task_uid": true }),
        Some(&req),
    );

    let UpdateParam { index_uid, task_id } = index_uid.into_inner();

    let task: TaskView = meilisearch.get_index_task(index_uid, task_id).await?.into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Ok().json(task))
}

pub async fn get_all_tasks_status(
    meilisearch: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, MeiliSearch>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(
        "Index Tasks Seen".to_string(),
        json!({ "per_task_uid": false }),
        Some(&req),
    );

    let tasks: TaskListView = meilisearch
        .list_index_task(index_uid.into_inner(), None, None)
        .await?
        .into_iter()
        .map(TaskView::from)
        .collect::<Vec<_>>()
        .into();

    debug!("returns: {:?}", tasks);
    Ok(HttpResponse::Ok().json(tasks))
}
