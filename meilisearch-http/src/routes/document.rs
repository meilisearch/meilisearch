use std::collections::{BTreeSet, HashSet};

use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post, put};
use serde::Deserialize;
use serde_json::Value;
use tokio::fs::File;
use tokio::prelude::*;
use uuid::Uuid;

use crate::data::{Data, Document, UpdateDocumentsQuery};
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::raft::{Message, Raft};
use crate::routes::IndexUpdateResponse;

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

pub fn services_raft(cfg: &mut web::ServiceConfig) {
    cfg.service(get_document)
        .service(delete_document_raft)
        .service(get_all_documents)
        .service(add_documents_raft)
        .service(update_documents_raft)
        .service(delete_documents_raft)
        .service(clear_all_documents_raft);
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
        .open_index(path.index_uid.as_str())
        .ok_or(Error::index_not_found(path.index_uid.as_str()))?;

    let reader = data.db.main_read_txn()?;

    let internal_id = index
        .main
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
        .open_index(path.index_uid.as_str())
        .ok_or(Error::index_not_found(path.index_uid.as_str()))?;

    let mut documents_deletion = index.documents_deletion();
    documents_deletion.delete_document_by_external_docid(path.document_id.clone());

    let update_id = data.db.update_write(|w| documents_deletion.finalize(w))?;

    Ok(HttpResponse::Accepted().json(IndexUpdateResponse::with_id(update_id)))
}

#[delete(
    "/indexes/{index_uid}/documents/{document_id}",
    wrap = "Authentication::Private"
)]
async fn delete_document_raft(
    raft: web::Data<Raft>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let message = Message::DocumentsDeletion {
        index_uid: path.index_uid.clone(),
        ids: vec![Value::String(path.document_id.clone())],
    };
    let response = raft
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;

    Ok(HttpResponse::Accepted().json(response))
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
    index_uid: web::Path<String>,
    params: web::Query<BrowseQuery>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(index_uid.as_ref())
        .ok_or(Error::index_not_found(index_uid.as_ref()))?;

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

#[post("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn add_documents(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.update_multiple_documents(
        index_uid.as_ref(),
        params.into_inner(),
        body.into_inner(),
        false,
    )?;
    Ok(HttpResponse::Accepted().json(response))
}

/// For raft documents additions, we want to first write the addition to a shared storage. This
/// allows us to commit the additon location and let the nodes fetch the addition when they are
/// ready to do so. This keeps to log size as small as possible.
#[post("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn add_documents_raft(
    raft: web::Data<Raft>,
    index_uid: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    // write to shared path and send proposal
    let filename = format!("{}.json", Uuid::new_v4());
    let file_path = raft.shared_folder.join(&filename);
    let mut file = File::create(&file_path)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    let json =
        serde_json::to_string(&body.into_inner()).map_err(|e| Error::Internal(e.to_string()))?;
    file.write_all(json.as_bytes())
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;

    let message = Message::DocumentAddition {
        update_query: params.into_inner(),
        index_uid: index_uid.into_inner(),
        filename,
        partial: false,
    };

    let response = raft
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;

    Ok(HttpResponse::Accepted().json(response))
}

#[put("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn update_documents(
    data: web::Data<Data>,
    path: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.update_multiple_documents(
        path.as_ref(),
        params.into_inner(),
        body.into_inner(),
        true,
    )?;
    Ok(HttpResponse::Accepted().json(response))
}

#[put("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn update_documents_raft(
    raft: web::Data<Raft>,
    index_uid: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    // write to shared path and send proposal
    let filename = format!("{}.json", Uuid::new_v4());
    let file_path = raft.shared_folder.join(&filename);
    let mut file = File::create(&file_path)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
    let json =
        serde_json::to_string(&body.into_inner()).map_err(|e| Error::Internal(e.to_string()))?;
    file.write_all(json.as_bytes())
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;

    let message = Message::DocumentAddition {
        update_query: params.into_inner(),
        index_uid: index_uid.into_inner(),
        filename,
        partial: true,
    };

    let response = raft
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;

    Ok(HttpResponse::Accepted().json(response))
}

#[post(
    "/indexes/{index_uid}/documents/delete-batch",
    wrap = "Authentication::Private"
)]
async fn delete_documents(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.delete_documents(index_uid.as_ref(), body.into_inner())?;
    Ok(HttpResponse::Accepted().json(response))
}

#[post(
    "/indexes/{index_uid}/documents/delete-batch",
    wrap = "Authentication::Private"
)]
async fn delete_documents_raft(
    raft: web::Data<Raft>,
    index_uid: web::Path<String>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, ResponseError> {
    let message = Message::DocumentsDeletion {
        index_uid: index_uid.into_inner(),
        ids: body.into_inner(),
    };
    let response = raft
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;

    Ok(HttpResponse::Accepted().json(response))
}

#[delete("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn clear_all_documents(
    data: web::Data<Data>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let response = data.clear_all_documents(index_uid.as_ref())?;
    Ok(HttpResponse::Accepted().json(response))
}

#[delete("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn clear_all_documents_raft(
    raft: web::Data<Raft>,
    index_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let message = Message::ClearAllDocuments {
        index_uid: index_uid.into_inner(),
    };
    let response = raft
        .propose(message)
        .await
        .map_err(|e| Error::RaftError(e.to_string()))?;

    Ok(HttpResponse::Accepted().json(response))
}
