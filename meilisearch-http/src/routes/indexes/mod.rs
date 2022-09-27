use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::{IndexScheduler, KindWithContent};
use log::debug;
use meilisearch_types::error::ResponseError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use index_scheduler::task::TaskView;

use super::Pagination;

pub mod documents;
pub mod search;
pub mod settings;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(list_indexes))
            .route(web::post().to(SeqHandler(create_index))),
    )
    .service(
        web::scope("/{index_uid}")
            .service(
                web::resource("")
                    .route(web::get().to(SeqHandler(get_index)))
                    .route(web::patch().to(SeqHandler(update_index)))
                    .route(web::delete().to(SeqHandler(delete_index))),
            )
            .service(web::resource("/stats").route(web::get().to(SeqHandler(get_index_stats))))
            .service(web::scope("/documents").configure(documents::configure))
            .service(web::scope("/search").configure(search::configure))
            .service(web::scope("/settings").configure(settings::configure)),
    );
}

pub async fn list_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    paginate: web::Query<Pagination>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &index_scheduler.filters().search_rules;
    let indexes: Vec<_> = index_scheduler.indexes()?;
    let nb_indexes = indexes.len();
    let iter = indexes
        .into_iter()
        .filter(|index| search_rules.is_index_authorized(&index.name));
    /*
    TODO: TAMO: implements me. It's missing a kind of IndexView or something
    let ret = paginate
        .into_inner()
        .auto_paginate_unsized(nb_indexes, iter);
    */
    let ret = todo!();

    debug!("returns: {:?}", ret);
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexCreateRequest {
    uid: String,
    primary_key: Option<String>,
}

pub async fn create_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_CREATE }>, Data<IndexScheduler>>,
    body: web::Json<IndexCreateRequest>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let IndexCreateRequest { primary_key, uid } = body.into_inner();

    analytics.publish(
        "Index Created".to_string(),
        json!({ "primary_key": primary_key }),
        Some(&req),
    );

    let task = KindWithContent::IndexCreation {
        index_uid: uid,
        primary_key,
    };
    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
pub struct UpdateIndexRequest {
    uid: Option<String>,
    primary_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateIndexResponse {
    name: String,
    uid: String,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    created_at: OffsetDateTime,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    updated_at: OffsetDateTime,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    primary_key: OffsetDateTime,
}

pub async fn get_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let meta = index_scheduler.index(&index_uid)?;
    debug!("returns: {:?}", meta);

    // TODO: TAMO: do this as well
    todo!()
    // Ok(HttpResponse::Ok().json(meta))
}

pub async fn update_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_UPDATE }>, Data<IndexScheduler>>,
    path: web::Path<String>,
    body: web::Json<UpdateIndexRequest>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", body);
    let body = body.into_inner();
    analytics.publish(
        "Index Updated".to_string(),
        json!({ "primary_key": body.primary_key}),
        Some(&req),
    );

    let task = KindWithContent::IndexUpdate {
        index_uid: path.into_inner(),
        primary_key: body.primary_key,
    };

    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn delete_index(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let task = KindWithContent::IndexDeletion {
        index_uid: index_uid.into_inner(),
    };
    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    Ok(HttpResponse::Accepted().json(task))
}

pub async fn get_index_stats(
    index_scheduler: GuardedData<ActionPolicy<{ actions::STATS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(
        "Stats Seen".to_string(),
        json!({ "per_index_uid": true }),
        Some(&req),
    );
    let index = index_scheduler.index(&index_uid)?;
    // TODO: TAMO: Bring the index_stats in meilisearch-http
    // let response = index.get_index_stats()?;
    let response = todo!();

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}
