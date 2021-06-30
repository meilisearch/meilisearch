use actix_web::{web, HttpResponse};
use log::debug;
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use serde::Deserialize;
use serde_json::Value;

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::payload::Payload;
use crate::routes::IndexParam;
use crate::Data;

const DEFAULT_RETRIEVE_DOCUMENTS_OFFSET: usize = 0;
const DEFAULT_RETRIEVE_DOCUMENTS_LIMIT: usize = 20;

/*
macro_rules! guard_content_type {
    ($fn_name:ident, $guard_value:literal) => {
        fn $fn_name(head: &actix_web::dev::RequestHead) -> bool {
            if let Some(content_type) = head.headers.get("Content-Type") {
                content_type
                    .to_str()
                    .map(|v| v.contains($guard_value))
                    .unwrap_or(false)
            } else {
                false
            }
        }
    };
}

guard_content_type!(guard_json, "application/json");
*/

fn guard_json(head: &actix_web::dev::RequestHead) -> bool {
    if let Some(content_type) = head.headers.get("Content-Type") {
        content_type
            .to_str()
            .map(|v| v.contains("application/json"))
            .unwrap_or(false)
    } else {
        // if no content-type is specified we still accept the data as json!
        true
    }
}

#[derive(Deserialize)]
struct DocumentParam {
    index_uid: String,
    document_id: String,
}

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/indexes/{index_uid}/documents")
            .service(
                web::resource("")
                    .route(web::get().to(get_all_documents))
                    .route(web::post().guard(guard_json).to(add_documents))
                    .route(web::put().guard(guard_json).to(update_documents))
                    .route(web::delete().to(clear_all_documents)),
            )
            // this route needs to be before the /documents/{document_id} to match properly
            .service(web::resource("/delete-batch").route(web::post().to(delete_documents)))
            .service(
                web::resource("/{document_id}")
                    .route(web::get().to(get_document))
                    .route(web::delete().to(delete_document)),
            ),
    );
}

async fn get_document(
    data: GuardedData<Public, Data>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = path.index_uid.clone();
    let id = path.document_id.clone();
    let document = data
        .retrieve_document(index, id, None as Option<Vec<String>>)
        .await?;
    debug!("returns: {:?}", document);
    Ok(HttpResponse::Ok().json(document))
}

async fn delete_document(
    data: GuardedData<Private, Data>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let update_status = data
        .delete_documents(path.index_uid.clone(), vec![path.document_id.clone()])
        .await?;
    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

async fn get_all_documents(
    data: GuardedData<Public, Data>,
    path: web::Path<IndexParam>,
    params: web::Query<BrowseQuery>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let attributes_to_retrieve = params.attributes_to_retrieve.as_ref().and_then(|attrs| {
        let mut names = Vec::new();
        for name in attrs.split(',').map(String::from) {
            if name == "*" {
                return None;
            }
            names.push(name);
        }
        Some(names)
    });

    let documents = data
        .retrieve_documents(
            path.index_uid.clone(),
            params.offset.unwrap_or(DEFAULT_RETRIEVE_DOCUMENTS_OFFSET),
            params.limit.unwrap_or(DEFAULT_RETRIEVE_DOCUMENTS_LIMIT),
            attributes_to_retrieve,
        )
        .await?;
    debug!("returns: {:?}", documents);
    Ok(HttpResponse::Ok().json(documents))
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

/// Route used when the payload type is "application/json"
/// Used to add or replace documents
async fn add_documents(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let update_status = data
        .add_documents(
            path.into_inner().index_uid,
            IndexDocumentsMethod::ReplaceDocuments,
            UpdateFormat::Json,
            body,
            params.primary_key.clone(),
        )
        .await?;

    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}

/// Route used when the payload type is "application/json"
/// Used to add or replace documents
async fn update_documents(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let update = data
        .add_documents(
            path.into_inner().index_uid,
            IndexDocumentsMethod::UpdateDocuments,
            UpdateFormat::Json,
            body,
            params.primary_key.clone(),
        )
        .await?;

    debug!("returns: {:?}", update);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update.id() })))
}

async fn delete_documents(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", body);
    let ids = body
        .iter()
        .map(|v| {
            v.as_str()
                .map(String::from)
                .unwrap_or_else(|| v.to_string())
        })
        .collect();

    let update_status = data.delete_documents(path.index_uid.clone(), ids).await?;
    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}

async fn clear_all_documents(
    data: GuardedData<Private, Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let update_status = data.clear_documents(path.index_uid.clone()).await?;
    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}
