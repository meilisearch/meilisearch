use actix_web::error::PayloadError;
use actix_web::{web, HttpResponse};
use actix_web::web::Bytes;
use futures::{Stream, StreamExt};
use log::debug;
use meilisearch_lib::MeiliSearch;
use meilisearch_lib::index_controller::{DocumentAdditionFormat, Update};
use meilisearch_lib::milli::update::IndexDocumentsMethod;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::payload::Payload;
use crate::routes::IndexParam;

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

/// This is required because Payload is not Sync nor Send
fn payload_to_stream(mut payload: Payload) -> impl Stream<Item=Result<Bytes, PayloadError>> {
    let (snd, recv) = mpsc::channel(1);
    tokio::task::spawn_local(async move {
        while let Some(data) = payload.next().await {
            let _ = snd.send(data).await;
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(recv)
}

fn guard_json(head: &actix_web::dev::RequestHead) -> bool {
    if let Some(_content_type) = head.headers.get("Content-Type") {
        // CURRENTLY AND FOR THIS RELEASE ONLY WE DECIDED TO INTERPRET ALL CONTENT-TYPES AS JSON
        true
        /*
        content_type
            .to_str()
            .map(|v| v.contains("application/json"))
            .unwrap_or(false)
        */
    } else {
        // if no content-type is specified we still accept the data as json!
        true
    }
}

#[derive(Deserialize)]
pub struct DocumentParam {
    index_uid: String,
    document_id: String,
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
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
    );
}

pub async fn get_document(
    meilisearch: GuardedData<Public, MeiliSearch>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = path.index_uid.clone();
    let id = path.document_id.clone();
    let document = meilisearch
        .document(index, id, None as Option<Vec<String>>)
        .await?;
    debug!("returns: {:?}", document);
    Ok(HttpResponse::Ok().json(document))
}

pub async fn delete_document(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam { document_id, index_uid } = path.into_inner();
    let update = Update::DeleteDocuments(vec![document_id]);
    let update_status = meilisearch.register_update(index_uid, update, false).await?;
    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

pub async fn get_all_documents(
    meilisearch: GuardedData<Public, MeiliSearch>,
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

    let documents = meilisearch
        .documents(
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
pub struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

/// Route used when the payload type is "application/json"
/// Used to add or replace documents
pub async fn add_documents(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let update = Update::DocumentAddition {
        payload: Box::new(payload_to_stream(body)),
        primary_key: params.primary_key.clone(),
        method: IndexDocumentsMethod::ReplaceDocuments,
        format: DocumentAdditionFormat::Json,
    };
    let update_status = meilisearch
        .register_update(path.into_inner().index_uid, update, true)
        .await?;

    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}

/// Route used when the payload type is "application/json"
/// Used to add or replace documents
pub async fn update_documents(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let update = Update::DocumentAddition {
        payload: Box::new(payload_to_stream(body)),
        primary_key: params.primary_key.clone(),
        method: IndexDocumentsMethod::UpdateDocuments,
        format: DocumentAdditionFormat::Json,
    };
    let update_status = meilisearch
        .register_update(path.into_inner().index_uid, update, true)
        .await?;

    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}

pub async fn delete_documents(
    meilisearch: GuardedData<Private, MeiliSearch>,
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

    let update = Update::DeleteDocuments(ids);
    let update_status = meilisearch.register_update(path.into_inner().index_uid, update, false).await?;
    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}

pub async fn clear_all_documents(
    meilisearch: GuardedData<Private, MeiliSearch>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let update = Update::ClearDocuments;
    let update_status = meilisearch.register_update(path.into_inner().index_uid, update, false).await?;
    debug!("returns: {:?}", update_status);
    Ok(HttpResponse::Accepted().json(serde_json::json!({ "updateId": update_status.id() })))
}
