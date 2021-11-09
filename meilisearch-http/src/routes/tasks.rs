use actix_web::{web, HttpResponse};
use meilisearch_lib::tasks::task::TaskId;
use meilisearch_lib::MeiliSearch;

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::task::{TaskListResponse, TaskResponse};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(get_tasks)))
        .service(web::resource("/{task_id}").route(web::get().to(get_task)));
}

async fn get_tasks(
    meilisearch: GuardedData<Private, MeiliSearch>,
) -> Result<HttpResponse, ResponseError> {
    let tasks: TaskListResponse = meilisearch
        .list_tasks(None, None, None)
        .await?
        .into_iter()
        .map(TaskResponse::from)
        .collect::<Vec<_>>()
        .into();

    Ok(HttpResponse::Ok().json(tasks))
}

async fn get_task(
    meilisearch: GuardedData<Private, MeiliSearch>,
    task_id: web::Path<TaskId>,
) -> Result<HttpResponse, ResponseError> {
    let task: TaskResponse = meilisearch
        .get_task(task_id.into_inner(), None)
        .await?
        .into();

    Ok(HttpResponse::Ok().json(task))
}
