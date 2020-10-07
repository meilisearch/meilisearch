use std::collections::BTreeMap;

use actix_web::{web, HttpResponse, get};
use indexmap::IndexMap;

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::make_update_delete_routes;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/synonyms",
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

    let synonyms_list = index.main.synonyms(&reader)?;

    let mut synonyms = IndexMap::new();
    let index_synonyms = &index.synonyms;
    for synonym in synonyms_list {
        let list = index_synonyms.synonyms(&reader, synonym.as_bytes())?;
        synonyms.insert(synonym, list);
    }

    Ok(HttpResponse::Ok().json(synonyms))
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/synonyms",
    BTreeMap<String, Vec<String>>,
    synonyms
);
