use crate::make_update_delete_routes;
use actix_web::{web, HttpResponse};
use actix_web_macros::get;

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/distinct-attribute",
    wrap = "Authentication::Private"
)]
async fn get(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&index_uid.as_ref())
        .ok_or(Error::index_not_found(&index_uid.as_ref()))?;
    let reader = data.db.main_read_txn()?;
    let distinct_attribute_id = index.main.distinct_attribute(&reader)?;
    let schema = index.main.schema(&reader)?;
    let distinct_attribute = match (schema, distinct_attribute_id) {
        (Some(schema), Some(id)) => schema.name(id).map(str::to_string),
        _ => None,
    };

    Ok(HttpResponse::Ok().json(distinct_attribute))
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/distinct-attribute",
    String,
    distinct_attribute
);
