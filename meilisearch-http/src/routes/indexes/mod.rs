use actix_web::{web, HttpRequest, HttpResponse};
use log::debug;
use meilisearch_error::ResponseError;
use meilisearch_lib::index_controller::Update;
use meilisearch_lib::MeiliSearch;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::task::SummarizedTaskView;

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
                    .route(web::put().to(SeqHandler(update_index)))
                    .route(web::delete().to(SeqHandler(delete_index))),
            )
            .service(web::resource("/stats").route(web::get().to(SeqHandler(get_index_stats))))
            .service(web::scope("/documents").configure(documents::configure))
            .service(web::scope("/search").configure(search::configure))
            .service(web::scope("/settings").configure(settings::configure)),
    );
}

const PAGINATION_DEFAULT_LIMIT: fn() -> usize = || 20;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Paginate {
    #[serde(default)]
    offset: usize,
    #[serde(default = "PAGINATION_DEFAULT_LIMIT")]
    limit: usize,
}

pub async fn list_indexes(
    data: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, MeiliSearch>,
    paginate: web::Query<Paginate>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &data.filters().search_rules;
    let indexes: Vec<_> = data.list_indexes().await?;
    let nb_indexes = indexes.len();
    let indexes: Vec<_> = indexes
        .into_iter()
        .filter(|i| search_rules.is_index_authorized(&i.uid))
        .skip(paginate.offset)
        .take(paginate.limit)
        .collect();

    debug!("returns: {:?}", indexes);
    Ok(HttpResponse::Ok().json(json!({
        "results": indexes,
        "offset": paginate.offset,
        "limit": paginate.limit,
        "total": nb_indexes,
    })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexCreateRequest {
    uid: String,
    primary_key: Option<String>,
}

pub async fn create_index(
    meilisearch: GuardedData<ActionPolicy<{ actions::INDEXES_CREATE }>, MeiliSearch>,
    body: web::Json<IndexCreateRequest>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let IndexCreateRequest {
        primary_key, uid, ..
    } = body.into_inner();

    analytics.publish(
        "Index Created".to_string(),
        json!({ "primary_key": primary_key }),
        Some(&req),
    );

    let update = Update::CreateIndex { primary_key };
    let task: SummarizedTaskView = meilisearch.register_update(uid, update).await?.into();

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
    meilisearch: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, MeiliSearch>,
    path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let meta = meilisearch.get_index(path.into_inner()).await?;
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

pub async fn update_index(
    meilisearch: GuardedData<ActionPolicy<{ actions::INDEXES_UPDATE }>, MeiliSearch>,
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

    let update = Update::UpdateIndex {
        primary_key: body.primary_key,
    };

    let task: SummarizedTaskView = meilisearch
        .register_update(path.into_inner(), update)
        .await?
        .into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn delete_index(
    meilisearch: GuardedData<ActionPolicy<{ actions::INDEXES_DELETE }>, MeiliSearch>,
    path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let uid = path.into_inner();
    let update = Update::DeleteIndex;
    let task: SummarizedTaskView = meilisearch.register_update(uid, update).await?.into();

    Ok(HttpResponse::Accepted().json(task))
}

pub async fn get_index_stats(
    meilisearch: GuardedData<ActionPolicy<{ actions::STATS_GET }>, MeiliSearch>,
    path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let response = meilisearch.get_index_stats(path.into_inner()).await?;

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}
