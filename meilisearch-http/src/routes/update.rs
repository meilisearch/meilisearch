use actix_web::*;
use meilisearch_core::UpdateStatus;

use crate::error::ResponseError;
use crate::Data;

#[get("/indexes/{index_uid}/updates/{update_id}")]
pub async fn get_update_status(
    data: web::Data<Data>,
    path: web::Path<(String, u64)>,
) -> Result<web::Json<UpdateStatus>> {

    let index = data.db.open_index(path.0.clone())
        .ok_or(ResponseError::IndexNotFound(path.0.clone()))?;

    let reader = data.db.update_read_txn()
        .map_err(|_| ResponseError::CreateTransaction)?;

    let status = index.update_status(&reader, path.1)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;

    match status {
        Some(status) => Ok(web::Json(status)),
        None => Err(ResponseError::UpdateNotFound(path.1))?
    }
}

#[get("/indexes/{index_uid}/updates")]
pub async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<String>,
) -> Result<web::Json<Vec<UpdateStatus>>> {

    let index = data.db.open_index(path.clone())
        .ok_or(ResponseError::IndexNotFound(path.clone()))?;

    let reader = data.db.update_read_txn()
        .map_err(|_| ResponseError::CreateTransaction)?;

    let response = index.all_updates_status(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(web::Json(response))
}
