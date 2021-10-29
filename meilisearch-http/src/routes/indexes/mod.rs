use actix_web::{web, HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use meilisearch_lib::index_controller::IndexSettings;
use meilisearch_lib::MeiliSearch;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::analytics::Analytics;
use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::routes::IndexParam;

pub mod documents;
pub mod search;
pub mod settings;
pub mod updates;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(list_indexes))
            .route(web::post().to(create_index)),
    )
    .service(
        web::scope("/{index_uid}")
            .service(
                web::resource("")
                    .route(web::get().to(get_index))
                    .route(web::put().to(update_index))
                    .route(web::delete().to(delete_index)),
            )
            .service(web::resource("/stats").route(web::get().to(get_index_stats)))
            .service(web::scope("/documents").configure(documents::configure))
            .service(web::scope("/search").configure(search::configure))
            .service(web::scope("/updates").configure(updates::configure))
            .service(web::scope("/settings").configure(settings::configure)),
    );
}

pub async fn list_indexes(
    data: GuardedData<Private, MeiliSearch>,
) -> Result<HttpResponse, ResponseError> {
    let indexes = data.list_indexes().await?;
    debug!("returns: {:?}", indexes);
    Ok(HttpResponse::Ok().json(indexes))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexCreateRequest {
    uid: String,
    primary_key: Option<String>,
}

pub async fn create_index(
    meilisearch: GuardedData<Private, MeiliSearch>,
    body: web::Json<IndexCreateRequest>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let body = body.into_inner();

    analytics.publish(
        "Index Created".to_string(),
        json!({ "primary_key": body.primary_key}),
        Some(&req),
    );
    let meta = meilisearch.create_index(body.uid, body.primary_key).await?;
    Ok(HttpResponse::Created().json(meta))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateIndexRequest {
    uid: Option<String>,
    primary_key: Option<String>,
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

pub async fn get_index(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let meta = meilisearch.get_index(path.index_uid.clone()).await?;
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

pub async fn update_index(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
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
    let settings = IndexSettings {
        uid: body.uid,
        primary_key: body.primary_key,
    };
    let meta = meilisearch
        .update_index(path.into_inner().index_uid, settings)
        .await?;
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

pub async fn delete_index(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    meilisearch.delete_index(path.index_uid.clone()).await?;
    Ok(HttpResponse::NoContent().finish())
}

pub async fn get_index_stats(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let response = meilisearch.get_index_stats(path.index_uid.clone()).await?;

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}
