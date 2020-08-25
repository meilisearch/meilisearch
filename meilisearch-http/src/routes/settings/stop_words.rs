use std::collections::BTreeSet;

use crate::make_update_delete_routes;
use actix_web::{web, HttpResponse};
use actix_web_macros::get;

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/stop-words",
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
    let stop_words = index.main.stop_words(&reader)?;

    Ok(HttpResponse::Ok().json(stop_words))
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/stop-words",
    BTreeSet<String>,
    stop_words
);
