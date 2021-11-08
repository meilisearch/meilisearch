use actix_web::error::PayloadError;
use actix_web::http::header::CONTENT_TYPE;
use actix_web::web::Bytes;
use actix_web::HttpMessage;
use actix_web::{web, HttpRequest, HttpResponse};
use bstr::ByteSlice;
use futures::{Stream, StreamExt};
use log::debug;
use meilisearch_error::ResponseError;
use meilisearch_lib::index_controller::{DocumentAdditionFormat, Update};
use meilisearch_lib::milli::update::IndexDocumentsMethod;
use meilisearch_lib::MeiliSearch;
use mime::Mime;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::payload::Payload;
use crate::task::SummarizedTaskView;

const DEFAULT_RETRIEVE_DOCUMENTS_OFFSET: usize = 0;
const DEFAULT_RETRIEVE_DOCUMENTS_LIMIT: usize = 20;

static ACCEPTED_CONTENT_TYPE: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "application/json".to_string(),
        "application/x-ndjson".to_string(),
        "text/csv".to_string(),
    ]
});

/// This is required because Payload is not Sync nor Send
fn payload_to_stream(mut payload: Payload) -> impl Stream<Item = Result<Bytes, PayloadError>> {
    let (snd, recv) = mpsc::channel(1);
    tokio::task::spawn_local(async move {
        while let Some(data) = payload.next().await {
            let _ = snd.send(data).await;
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(recv)
}

/// Extracts the mime type from the content type and return
/// a meilisearch error if anyhthing bad happen.
fn extract_mime_type(req: &HttpRequest) -> Result<Option<Mime>, MeilisearchHttpError> {
    match req.mime_type() {
        Ok(Some(mime)) => Ok(Some(mime)),
        Ok(None) => Ok(None),
        Err(_) => match req.headers().get(CONTENT_TYPE) {
            Some(content_type) => Err(MeilisearchHttpError::InvalidContentType(
                content_type.as_bytes().as_bstr().to_string(),
                ACCEPTED_CONTENT_TYPE.clone(),
            )),
            None => Err(MeilisearchHttpError::MissingContentType(
                ACCEPTED_CONTENT_TYPE.clone(),
            )),
        },
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
            .route(web::post().to(add_documents))
            .route(web::put().to(update_documents))
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
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, MeiliSearch>,
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
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, MeiliSearch>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam {
        document_id,
        index_uid,
    } = path.into_inner();
    let update = Update::DeleteDocuments(vec![document_id]);
    let task: SummarizedTaskView = meilisearch.register_update(index_uid, update).await?.into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

pub async fn get_all_documents(
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, MeiliSearch>,
    path: web::Path<String>,
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
            path.into_inner(),
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
    pub primary_key: Option<String>,
}

pub async fn add_documents(
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, MeiliSearch>,
    path: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let params = params.into_inner();
    let index_uid = path.into_inner();

    analytics.add_documents(
        &params,
        meilisearch.get_index(index_uid.clone()).await.is_err(),
        &req,
    );

    let task = document_addition(
        extract_mime_type(&req)?,
        meilisearch,
        index_uid,
        params.primary_key,
        body,
        IndexDocumentsMethod::ReplaceDocuments,
    )
    .await?;

    Ok(HttpResponse::Accepted().json(task))
}

pub async fn update_documents(
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, MeiliSearch>,
    path: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let index_uid = path.into_inner();

    analytics.update_documents(
        &params,
        meilisearch.get_index(index_uid.clone()).await.is_err(),
        &req,
    );

    let task = document_addition(
        extract_mime_type(&req)?,
        meilisearch,
        index_uid,
        params.into_inner().primary_key,
        body,
        IndexDocumentsMethod::UpdateDocuments,
    )
    .await?;

    Ok(HttpResponse::Accepted().json(task))
}

async fn document_addition(
    mime_type: Option<Mime>,
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, MeiliSearch>,
    index_uid: String,
    primary_key: Option<String>,
    body: Payload,
    method: IndexDocumentsMethod,
) -> Result<SummarizedTaskView, ResponseError> {
    let format = match mime_type
        .as_ref()
        .map(|m| (m.type_().as_str(), m.subtype().as_str()))
    {
        Some(("application", "json")) => DocumentAdditionFormat::Json,
        Some(("application", "x-ndjson")) => DocumentAdditionFormat::Ndjson,
        Some(("text", "csv")) => DocumentAdditionFormat::Csv,
        Some((type_, subtype)) => {
            return Err(MeilisearchHttpError::InvalidContentType(
                format!("{}/{}", type_, subtype),
                ACCEPTED_CONTENT_TYPE.clone(),
            )
            .into())
        }
        None => {
            return Err(
                MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone()).into(),
            )
        }
    };

    let update = Update::DocumentAddition {
        payload: Box::new(payload_to_stream(body)),
        primary_key,
        method,
        format,
    };

    let task = meilisearch.register_update(index_uid, update).await?.into();

    debug!("returns: {:?}", task);
    Ok(task)
}

pub async fn delete_documents(
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, MeiliSearch>,
    path: web::Path<String>,
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
    let task: SummarizedTaskView = meilisearch
        .register_update(path.into_inner(), update)
        .await?
        .into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn clear_all_documents(
    meilisearch: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, MeiliSearch>,
    path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let update = Update::ClearDocuments;
    let task: SummarizedTaskView = meilisearch
        .register_update(path.into_inner(), update)
        .await?
        .into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}
