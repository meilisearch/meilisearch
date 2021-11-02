use actix_web::{web, HttpResponse};
use meilisearch_lib::MeiliSearch;
use meilisearch_tasks::task::TaskId;
use serde::Serialize;

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::task::TaskResponse;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(get_tasks)))
        .service(web::resource("/{task_id}").route(web::get().to(get_task)));
}

#[derive(Debug, Serialize)]
struct TaskListResponse {
    results: Vec<TaskResponse>,
}

async fn get_tasks(
    meilisearch: GuardedData<Private, MeiliSearch>,
) -> Result<HttpResponse, ResponseError> {
    let tasks = meilisearch.list_tasks().await?;
    let  response = TaskListResponse {
        results: tasks.into_iter().map(TaskResponse::from).collect(),
    };
    Ok(HttpResponse::Ok().json(response))
}

async fn get_task(
    meilisearch: GuardedData<Private, MeiliSearch>,
    task_id: web::Path<TaskId>,
) -> Result<HttpResponse, ResponseError> {
    let task: TaskResponse = meilisearch.get_task(task_id.into_inner()).await?.into();

    Ok(HttpResponse::Ok().json(task))
}
