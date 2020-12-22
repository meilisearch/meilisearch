use std::collections::BTreeSet;

use actix_web::{delete, get, post};
use actix_web::{web, HttpResponse};

use crate::Data;
use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::updates::Settings;

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
    _data: web::Data<Data>,
    _path: web::Path<String>,
    _body: web::Json<Settings>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn get_all(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete("/indexes/{index_uid}/settings", wrap = "Authentication::Private")]
async fn delete_all(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn get_rules(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn update_rules(
    _data: web::Data<Data>,
    _path: web::Path<String>,
    _body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/ranking-rules",
    wrap = "Authentication::Private"
)]
async fn delete_rules(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn get_distinct(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn update_distinct(
    _data: web::Data<Data>,
    _path: web::Path<String>,
    _body: web::Json<Option<String>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn delete_distinct(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn get_searchable(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn update_searchable(
    _data: web::Data<Data>,
    _path: web::Path<String>,
    _body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn delete_searchable(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn get_displayed(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn update_displayed(
    _data: web::Data<Data>,
    _path: web::Path<String>,
    _body: web::Json<Option<BTreeSet<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/displayed-attributes",
    wrap = "Authentication::Private"
)]
async fn delete_displayed(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[get(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn get_attributes_for_faceting(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn update_attributes_for_faceting(
    _data: web::Data<Data>,
    _path: web::Path<String>,
    _body: web::Json<Option<Vec<String>>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/attributes-for-faceting",
    wrap = "Authentication::Private"
)]
async fn delete_attributes_for_faceting(
    _data: web::Data<Data>,
    _path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}
