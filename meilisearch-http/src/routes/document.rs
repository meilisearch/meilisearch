use actix_web::web::Payload;
use actix_web::{delete, get, post, put};
use actix_web::{web, HttpResponse};
use indexmap::IndexMap;
use log::error;
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use serde::Deserialize;
use serde_json::Value;

use crate::Data;
use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;

const DEFAULT_RETRIEVE_DOCUMENTS_OFFSET: usize = 0;
const DEFAULT_RETRIEVE_DOCUMENTS_LIMIT: usize = 20;

macro_rules! guard_content_type {
    ($fn_name:ident, $guard_value:literal) => {
        fn $fn_name(head: &actix_web::dev::RequestHead) -> bool {
            if let Some(content_type) = head.headers.get("Content-Type") {
                content_type.to_str().map(|v| v.contains($guard_value)).unwrap_or(false)
            } else {
                false
            }
        }
    };
}

guard_content_type!(guard_json, "application/json");

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
        .service(add_documents_json)
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
    let index = path.index_uid.clone();
    let id = path.document_id.clone();
    match data.retrieve_document(index, id, None as Option<Vec<String>>).await {
        Ok(document) => {
            let json = serde_json::to_string(&document).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[delete(
    "/indexes/{index_uid}/documents/{document_id}",
    wrap = "Authentication::Private"
)]
async fn delete_document(
    data: web::Data<Data>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    match data.delete_documents(path.index_uid.clone(), vec![path.document_id.clone()]).await {
        Ok(result) => {
            let json = serde_json::to_string(&result).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
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
    let attributes_to_retrieve = params
        .attributes_to_retrieve
        .as_ref()
        .map(|attrs| attrs
            .split(',')
            .map(String::from)
            .collect::<Vec<_>>());

    match data.retrieve_documents(
        path.index_uid.clone(),
        params.offset.unwrap_or(DEFAULT_RETRIEVE_DOCUMENTS_OFFSET),
        params.limit.unwrap_or(DEFAULT_RETRIEVE_DOCUMENTS_LIMIT),
        attributes_to_retrieve).await {
        Ok(docs) => {
            let json = serde_json::to_string(&docs).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

/// Route used when the payload type is "application/json"
#[post(
    "/indexes/{index_uid}/documents",
    wrap = "Authentication::Private",
    guard = "guard_json"
)]
async fn add_documents_json(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
) -> Result<HttpResponse, ResponseError> {
    let addition_result = data
        .add_documents(
            path.into_inner().index_uid,
            IndexDocumentsMethod::ReplaceDocuments,
            UpdateFormat::Json,
            body,
            params.primary_key.clone(),
        ).await;

    match addition_result {
        Ok(update) => {
            let value = serde_json::to_string(&update).unwrap();
            let response = HttpResponse::Ok().body(value);
            Ok(response)
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}


/// Default route for adding documents, this should return an error and redirect to the documentation
#[post("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn add_documents_default(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Query<UpdateDocumentsQuery>,
    _body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    error!("Unknown document type");
    todo!()
}

/// Default route for adding documents, this should return an error and redirect to the documentation
#[put("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn update_documents_default(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Query<UpdateDocumentsQuery>,
    _body: web::Json<Vec<Document>>,
) -> Result<HttpResponse, ResponseError> {
    error!("Unknown document type");
    todo!()
}

#[put(
    "/indexes/{index_uid}/documents",
    wrap = "Authentication::Private",
    guard = "guard_json",
)]
async fn update_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: web::Payload,
) -> Result<HttpResponse, ResponseError> {
    let addition_result = data
        .add_documents(
            path.into_inner().index_uid,
            IndexDocumentsMethod::UpdateDocuments,
            UpdateFormat::Json,
            body,
            params.primary_key.clone(),
        ).await;

    match addition_result {
        Ok(update) => {
            let value = serde_json::to_string(&update).unwrap();
            let response = HttpResponse::Ok().body(value);
            Ok(response)
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
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
    let ids = body
        .iter()
        .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
        .collect();

    match data.delete_documents(path.index_uid.clone(), ids).await {
        Ok(result) => {
            let json = serde_json::to_string(&result).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[delete("/indexes/{index_uid}/documents", wrap = "Authentication::Private")]
async fn clear_all_documents(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    match data.clear_documents(path.index_uid.clone()).await {
        Ok(update) => {
            let json = serde_json::to_string(&update).unwrap();
            Ok(HttpResponse::Ok().body(json))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}
