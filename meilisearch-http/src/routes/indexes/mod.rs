use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use meilisearch_lib::MeiliSearch;
use meilisearch_lib::index_controller::IndexSettings;
use serde::{Deserialize, Serialize};

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
            //.route(web::post().to(create_index)),
    )
    .service(
        web::scope("/{index_uid}")
            .service(
                web::resource("")
                    .route(web::get().to(get_index))
                    .route(web::put().to(update_index))
                    //.route(web::delete().to(delete_index)),
            )
            .service(web::resource("/stats").route(web::get().to(get_index_stats)))
            .service(web::scope("/documents").configure(documents::configure))
            .service(web::scope("/search").configure(search::configure))
            .service(web::scope("/updates").configure(updates::configure))
            //.service(web::scope("/settings").configure(settings::configure)),
    );
}

pub async fn list_indexes(data: GuardedData<Private, MeiliSearch>) -> Result<HttpResponse, ResponseError> {
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

//pub async fn create_index(
    //data: GuardedData<Private, MeiliSearch>,
    //body: web::Json<IndexCreateRequest>,
//) -> Result<HttpResponse, ResponseError> {
    //let body = body.into_inner();
    //let meta = data.create_index(body.uid, body.primary_key).await?;
    //Ok(HttpResponse::Created().json(meta))
//}

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
    data: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let meta = data.get_index(path.index_uid.clone()).await?;
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

pub async fn update_index(
    data: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
    body: web::Json<UpdateIndexRequest>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", body);
    let body = body.into_inner();
    let settings = IndexSettings {
        uid: body.uid,
        primary_key: body.primary_key,
    };
    let meta = data
        .update_index(path.into_inner().index_uid, settings)
        .await?;
    debug!("returns: {:?}", meta);
    Ok(HttpResponse::Ok().json(meta))
}

//pub async fn delete_index(
    //data: GuardedData<Private, MeiliSearch>,
    //path: web::Path<IndexParam>,
//) -> Result<HttpResponse, ResponseError> {
    //data.delete_index(path.index_uid.clone()).await?;
    //Ok(HttpResponse::NoContent().finish())
//}

pub async fn get_index_stats(
    data: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.get_index_stats(path.index_uid.clone()).await?;

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}
