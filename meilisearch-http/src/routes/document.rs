use std::collections::HashSet;
use std::collections::BTreeSet;

use actix_web as aweb;
use actix_web::{delete, get, post, put, web, HttpResponse};
use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::Value;

use crate::error::ResponseError;
use crate::routes::{IndexParam, IndexUpdateResponse};
use crate::Data;

type Document = IndexMap<String, Value>;

#[derive(Default, Deserialize)]
pub struct DocumentParam {
    index_uid: String,
    document_id: String,
}

#[get("/indexes/{index_uid}/documents/{document_id}")]
pub async fn get_document(
    data: web::Data<Data>,
    path: web::Path<DocumentParam>,
) -> aweb::Result<HttpResponse> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let document_id = meilisearch_core::serde::compute_document_id(path.document_id.clone());

    let reader = data
        .db
        .main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let response = index
        .document::<Document>(&reader, None, document_id)
        .map_err(|_| ResponseError::DocumentNotFound(path.document_id.clone()))?
        .ok_or(ResponseError::DocumentNotFound(path.document_id.clone()))?;

    Ok(HttpResponse::Ok().json(response))
}

#[delete("/indexes/{index_uid}/documents/{document_id}")]
pub async fn delete_document(
    data: web::Data<Data>,
    path: web::Path<DocumentParam>,
) -> aweb::Result<HttpResponse> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;
    let document_id = meilisearch_core::serde::compute_document_id(path.document_id.clone());

    let mut update_writer = data
        .db
        .update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let mut documents_deletion = index.documents_deletion();
    documents_deletion.delete_document_by_id(document_id);

    let update_id = documents_deletion
        .finalize(&mut update_writer)
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    update_writer
        .commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

#[get("/indexes/{index_uid}/documents")]
pub async fn get_all_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<BrowseQuery>,
) -> aweb::Result<HttpResponse> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20);

    let reader = data
        .db
        .main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let documents_ids: Result<BTreeSet<_>, _> = index
        .documents_fields_counts
        .documents_ids(&reader)
        .map_err(|_| ResponseError::Internal(path.index_uid.clone()))?
        .skip(offset)
        .take(limit)
        .collect();

    let documents_ids = documents_ids.map_err(|err| ResponseError::Internal(err.to_string()))?;

    let attributes: Option<HashSet<&str>> = params
        .attributes_to_retrieve.as_ref()
        .map(|a| a.split(',').collect());

    let mut response = Vec::<Document>::new();
    for document_id in documents_ids {
        if let Ok(Some(document)) = index.document(&reader, attributes.as_ref(), document_id) {
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

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

async fn update_multiple_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
    is_partial: bool,
) -> aweb::Result<HttpResponse> {
    let index = data
        .db
        .open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let reader = data
        .db
        .main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let mut schema = index
        .main
        .schema(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .ok_or(ResponseError::Internal(
            "Impossible to retrieve the schema".to_string(),
        ))?;

    if schema.primary_key().is_none() {
        let id = match params.primary_key.clone() {
            Some(id) => id,
            None => body.first().and_then(|docs| find_primary_key(docs)).ok_or(
                ResponseError::BadRequest("Could not infer a primary key".to_string()),
            )?,
        };

        let mut writer = data
            .db
            .main_write_txn()
            .map_err(|err| ResponseError::Internal(err.to_string()))?;

        schema
            .set_primary_key(&id)
            .map_err(|e| ResponseError::Internal(e.to_string()))?;
        index
            .main
            .put_schema(&mut writer, &schema)
            .map_err(|e| ResponseError::Internal(e.to_string()))?;
        writer
            .commit()
            .map_err(|err| ResponseError::Internal(err.to_string()))?;
    }

    let mut document_addition = if is_partial {
        index.documents_partial_addition()
    } else {
        index.documents_addition()
    };

    for document in body.into_inner() {
        document_addition.update_document(document);
    }

    let mut update_writer = data
        .db
        .update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;
    let update_id = document_addition
        .finalize(&mut update_writer)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;
    update_writer
        .commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[post("/indexes/{index_uid}/documents")]
pub async fn add_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> aweb::Result<HttpResponse> {
    update_multiple_documents(data, path, params, body, false).await
}

#[put("/indexes/{index_uid}/documents")]
pub async fn update_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> aweb::Result<HttpResponse> {
    update_multiple_documents(data, path, params, body, true).await
}

#[post("/indexes/{index_uid}/documents/delete-batch")]
pub async fn delete_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Vec<Value>>,
) -> aweb::Result<HttpResponse> {
    let index = data
        .db
        .open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let mut writer = data
        .db
        .update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let mut documents_deletion = index.documents_deletion();

    for document_id in body.into_inner() {
        if let Some(document_id) = meilisearch_core::serde::value_to_string(&document_id) {
            documents_deletion
                .delete_document_by_id(meilisearch_core::serde::compute_document_id(document_id));
        }
    }

    let update_id = documents_deletion
        .finalize(&mut writer)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;

    writer
        .commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete("/indexes/{index_uid}/documents")]
pub async fn clear_all_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> aweb::Result<HttpResponse> {
    let index = data
        .db
        .open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let mut writer = data
        .db
        .update_write_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let update_id = index
        .clear_all(&mut writer)
        .map_err(|e| ResponseError::Internal(e.to_string()))?;

    writer
        .commit()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}
