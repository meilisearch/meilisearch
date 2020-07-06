use std::collections::{BTreeSet, HashSet};

use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post, put};
use indexmap::IndexMap;
use meilisearch_core::update;
use serde::Deserialize;
use serde_json::Value;

use crate::Data;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::routes::{IndexParam, IndexUpdateResponse};

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;

    let internal_id = index.main
        .external_to_internal_docid(&reader, &path.document_id)?
        .ok_or(Error::document_not_found(&path.document_id))?;

    let document: Document = index
        .document(&reader, None, internal_id)?
        .ok_or(Error::document_not_found(&path.document_id))?;

    Ok(HttpResponse::Ok().json(document))
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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let mut documents_deletion = index.documents_deletion();
    documents_deletion.delete_document_by_external_docid(path.document_id.clone());

    let update_id = data.db.update_write(|w| documents_deletion.finalize(w))?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20);

    let reader = data.db.main_read_txn()?;
    let documents_ids: Result<BTreeSet<_>, _> = index
        .documents_fields_counts
        .documents_ids(&reader)?
        .skip(offset)
        .take(limit)
        .collect();

    let attributes: Option<HashSet<&str>> = params
        .attributes_to_retrieve
        .as_ref()
        .map(|a| a.split(',').collect());

    let mut documents = Vec::new();
    for document_id in documents_ids? {
        if let Ok(Some(document)) =
            index.document::<Document>(&reader, attributes.as_ref(), document_id)
        {
            documents.push(document);
        }
    }

    Ok(HttpResponse::Ok().json(documents))
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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;

    let mut schema = index
        .main
        .schema(&reader)?
        .ok_or(meilisearch_core::Error::SchemaMissing)?;

    if schema.primary_key().is_none() {
        let id = match &params.primary_key {
            Some(id) => id.to_string(),
            None => body
                .first()
                .and_then(find_primary_key)
                .ok_or(meilisearch_core::Error::MissingPrimaryKey)?
        };

        schema
            .set_primary_key(&id)
            .map_err(Error::bad_request)?;

        data.db.main_write(|w| index.main.put_schema(w, &schema))?;
    }

    let mut document_addition = if is_partial {
        index.documents_partial_addition()
    } else {
        index.documents_addition()
    };

    for document in body.into_inner() {
        document_addition.update_document(document);
    }

    let update_id = data.db.update_write(|w| document_addition.finalize(w))?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;


    let mut documents_deletion = index.documents_deletion();

    for document_id in body.into_inner() {
        let document_id = update::value_to_string(&document_id);
        documents_deletion.delete_document_by_external_docid(document_id);
    }

    let update_id = data.db.update_write(|w| documents_deletion.finalize(w))?;

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
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let update_id = data.db.update_write(|w| index.clear_all(w))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}
