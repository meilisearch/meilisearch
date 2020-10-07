use std::collections::HashSet;

use actix_web::{web, HttpResponse, get};

use crate::data::get_displayed_attributes;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::make_update_delete_routes;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/displayed-attributes",
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

    let displayed_attributes = schema.as_ref().map(get_displayed_attributes);

    Ok(HttpResponse::Ok().json(displayed_attributes))
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/displayed-attributes",
    HashSet<String>,
    displayed_attributes
);
