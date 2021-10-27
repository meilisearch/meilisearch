use actix_web::{web, HttpResponse};
use log::debug;
use meilisearch_lib::MeiliSearch;
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg
        .service(web::resource("").route(web::get().to(get_tasks)))
        .service(web::resource("/{task_uid}").route(web::get().to(get_task)));
}

async fn get_tasks(
    meilisearch: GuardedData<Private, MeiliSearch>,
) -> Result<HttpResponse, ResponseError> {
    Ok(HttpResponse::Ok().body("hello world"))
}

async fn get_task(
    meilisearch: GuardedData<Private, MeiliSearch>,
    task_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    Ok(HttpResponse::Ok().body(format!("Goodbye world: {}", task_uid)))
}
