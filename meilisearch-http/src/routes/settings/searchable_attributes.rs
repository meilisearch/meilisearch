use actix_web::{web, HttpResponse};
use actix_web_macros::get;

use super::get_indexed_attributes;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::make_update_delete_routes;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/searchable-attributes",
    wrap = "Authentication::Private"
)]
async fn get(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .load()
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;
    let reader = data.db.load().main_read_txn()?;
    let schema = index.main.schema(&reader)?;
    let searchable_attributes: Option<Vec<String>> = schema.as_ref().map(get_indexed_attributes);

    Ok(HttpResponse::Ok().json(searchable_attributes))
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/searchable-attributes",
    Vec<String>,
    searchable_attributes
);
