use std::collections::{BTreeSet, HashSet};

use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post, put};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::Value;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::{IndexParam, IndexUpdateResponse};
use crate::Data;

type Document = IndexMap<String, Value>;

#[derive(Deserialize)]
struct DocumentParam {
    index_uid: String,
    document_id: String,
}

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(get_document)
        .service(delete_document)
        .service(get_all_documents)
        .service(add_documents)
        .service(update_documents)
        .service(delete_documents)
        .service(clear_all_documents);
}

#[get(
    "/indexes/{index_uid}/documents/{document_id}",
    wrap = "Authentication::Public"
)]
async fn get_document(
    data: web::Data<Data>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let document_id = meilisearch_core::serde::compute_document_id(&path.document_id);

    let reader = data.db.main_read_txn()?;

    let response = index
        .document(&reader, None, document_id)?
        .ok_or(ResponseError::document_not_found(&path.document_id))?;

    Ok(HttpResponse::Ok().json(response))
}

#[delete(
    "/indexes/{index_uid}/documents/{document_id}",
    wrap = "Authentication::Private"
)]
async fn delete_document(
    data: web::Data<Data>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;
    let document_id = meilisearch_core::serde::compute_document_id(&path.document_id);

    let mut update_writer = data.db.update_write_txn()?;

    let mut documents_deletion = index.documents_deletion();
    documents_deletion.delete_document_by_id(document_id);

    let update_id = documents_deletion.finalize(&mut update_writer)?;

    update_writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

#[get("/indexes/{index_uid}/documents", wrap = "Authentication::Public")]
async fn get_all_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<BrowseQuery>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20);

    let reader = data.db.main_read_txn()?;

    let documents_ids: Result<BTreeSet<_>, _> = index
        .documents_fields_counts
        .documents_ids(&reader)?
        .skip(offset)
        .take(limit)
        .collect();

    let documents_ids = documents_ids?;

    let attributes: Option<HashSet<&str>> = params
        .attributes_to_retrieve
        .as_ref()
        .map(|a| a.split(',').collect());

    let mut response = Vec::new();
    for document_id in documents_ids {
        if let Ok(Some(document)) =
            index.document::<Document>(&reader, attributes.as_ref(), document_id)
        {
            response.push(document);
        }
    }

    Ok(HttpResponse::Ok().json(response))
}

fn find_primary_key(document: &IndexMap<String, Value>) -> Option<String> {
    for key in document.keys() {
        if key.to_lowercase().contains("id") {
            return Some(key.to_string());
        }
    }
    None
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

async fn update_multiple_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
    is_partial: bool,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;

    let mut schema = index
        .main
        .schema(&reader)?
        .ok_or(ResponseError::internal("Impossible to retrieve the schema"))?;

    if schema.primary_key().is_none() {
        let id = match &params.primary_key {
            Some(id) => id.to_string(),
            None => body
                .first()
                .and_then(find_primary_key)
                .ok_or(ResponseError::bad_request("Could not infer a primary key"))?,
        };

        let mut writer = data.db.main_write_txn()?;

        schema
            .set_primary_key(&id)
            .map_err(ResponseError::bad_request)?;
        index.main.put_schema(&mut writer, &schema)?;
        writer.commit()?;
    }

    let mut document_addition = if is_partial {
        index.documents_partial_addition()
    } else {
        index.documents_addition()
    };

    for document in body.into_inner() {
        document_addition.update_document(document);
    }

    let mut update_writer = data.db.update_write_txn()?;
    let update_id = document_addition.finalize(&mut update_writer)?;
    update_writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[post("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn add_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    update_multiple_documents(data, path, params, body, false).await
}

#[put("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn update_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    update_multiple_documents(data, path, params, body, true).await
}

#[post(
    "/indexes/{index_uid}/documents/delete-batch",
    wrap = "Authentication::Private"
)]
async fn delete_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let mut writer = data.db.update_write_txn()?;

    let mut documents_deletion = index.documents_deletion();

    for document_id in body.into_inner() {
        if let Some(document_id) = meilisearch_core::serde::value_to_string(&document_id) {
            documents_deletion
                .delete_document_by_id(meilisearch_core::serde::compute_document_id(document_id));
        }
    }

    let update_id = documents_deletion.finalize(&mut writer)?;

    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn clear_all_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let mut writer = data.db.update_write_txn()?;

    let update_id = index.clear_all(&mut writer)?;

    writer.commit()?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}
