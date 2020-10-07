use crate::make_update_delete_routes;
use actix_web::{web, HttpResponse, get};
use meilisearch_core::settings::DEFAULT_RANKING_RULES;

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::Data;

#[get(
    "/indexes/{index_uid}/settings/ranking-rules",
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

    let ranking_rules = index
        .main
        .ranking_rules(&reader)?
        .unwrap_or(DEFAULT_RANKING_RULES.to_vec())
        .into_iter()
        .map(|r| r.to_string())
        .collect::<Vec<String>>();

    Ok(HttpResponse::Ok().json(ranking_rules))
}

make_update_delete_routes!(
    "/indexes/{index_uid}/settings/ranking-rules",
    Vec<String>,
    ranking_rules
);
