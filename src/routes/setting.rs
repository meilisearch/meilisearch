use std::collections::{BTreeMap, BTreeSet};

use actix_web::{delete, get, post};
use actix_web::{web, HttpResponse};
use meilisearch_core::{MainReader, UpdateWriter};
use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState, DEFAULT_RANKING_RULES};
use meilisearch_schema::Schema;

use crate::Data;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::routes::{IndexParam, IndexUpdateResponse};

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(update_all)
        .service(get_all)
        .service(delete_all)
        .service(get_rules)
        .service(update_rules)
        .service(delete_rules)
        .service(get_distinct)
        .service(update_distinct)
        .service(delete_distinct)
        .service(get_searchable)
        .service(update_searchable)
        .service(delete_searchable)
        .service(get_displayed)
        .service(update_displayed)
        .service(delete_displayed)
        .service(get_attributes_for_faceting)
        .service(delete_attributes_for_faceting)
        .service(update_attributes_for_faceting);
}

#[post("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn update_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn get_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn update_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn delete_rules(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn get_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn update_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<String>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn delete_distinct(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn get_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn update_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn delete_searchable(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn get_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn update_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<BTreeSet<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn delete_displayed(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn get_attributes_for_faceting(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn update_attributes_for_faceting(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn delete_attributes_for_faceting(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}
