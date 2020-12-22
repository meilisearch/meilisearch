use actix_web::{web, HttpResponse};
use actix_web::{delete, get, post};
use std::collections::BTreeSet;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get).service(update).service(delete);
}

#[get(
    "/indexes/{index_uid}/settings/stop-words",
    wrap = "Authentication::Private"
)]
async fn get(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[post(
    "/indexes/{index_uid}/settings/stop-words",
    wrap = "Authentication::Private"
)]
async fn update(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _body: web::Json<BTreeSet<String>>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[delete(
    "/indexes/{index_uid}/settings/stop-words",
    wrap = "Authentication::Private"
)]
async fn delete(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}
