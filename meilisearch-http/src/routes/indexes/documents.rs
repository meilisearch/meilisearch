use std::io::Cursor;

use actix_web::error::PayloadError;
use actix_web::http::header::CONTENT_TYPE;
use actix_web::web::{Bytes, Data};
use actix_web::HttpMessage;
use actix_web::{web, HttpRequest, HttpResponse};
use bstr::ByteSlice;
use document_formats::{read_csv, read_json, read_ndjson, PayloadType};
use futures::{Stream, StreamExt};
use index::{retrieve_document, retrieve_documents};
use index_scheduler::milli::update::IndexDocumentsMethod;
use index_scheduler::IndexScheduler;
use index_scheduler::{KindWithContent, TaskView};
use log::debug;
use meilisearch_types::error::ResponseError;
use meilisearch_types::star_or::StarOr;
use mime::Mime;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_cs::vec::CS;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::payload::Payload;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::{fold_star_or, PaginationView};

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
/// a meilisearch error if anything bad happen.
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
            .route(web::get().to(SeqHandler(get_all_documents)))
            .route(web::post().to(SeqHandler(add_documents)))
            .route(web::put().to(SeqHandler(update_documents)))
            .route(web::delete().to(SeqHandler(clear_all_documents))),
    )
    // this route needs to be before the /documents/{document_id} to match properly
    .service(web::resource("/delete-batch").route(web::post().to(SeqHandler(delete_documents))))
    .service(
        web::resource("/{document_id}")
            .route(web::get().to(SeqHandler(get_document)))
            .route(web::delete().to(SeqHandler(delete_document))),
    );
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GetDocument {
    fields: Option<CS<StarOr<String>>>,
}

pub async fn get_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    path: web::Path<DocumentParam>,
    params: web::Query<GetDocument>,
) -> Result<HttpResponse, ResponseError> {
    let GetDocument { fields } = params.into_inner();
    let attributes_to_retrieve = fields.and_then(fold_star_or);

    let index = index_scheduler.index(&path.index_uid)?;
    let document = retrieve_document(&index, &path.document_id, attributes_to_retrieve)?;
    debug!("returns: {:?}", document);
    Ok(HttpResponse::Ok().json(document))
}

pub async fn delete_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<DocumentParam>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam {
        document_id,
        index_uid,
    } = path.into_inner();
    let task = KindWithContent::DocumentDeletion {
        index_uid,
        documents_ids: vec![document_id],
    };
    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrowseQuery {
    #[serde(default)]
    offset: usize,
    #[serde(default = "crate::routes::PAGINATION_DEFAULT_LIMIT")]
    limit: usize,
    fields: Option<CS<StarOr<String>>>,
}

pub async fn get_all_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: web::Query<BrowseQuery>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let BrowseQuery {
        limit,
        offset,
        fields,
    } = params.into_inner();
    let attributes_to_retrieve = fields.and_then(fold_star_or);

    let index = index_scheduler.index(&index_uid)?;
    let (total, documents) = retrieve_documents(&index, offset, limit, attributes_to_retrieve)?;

    let ret = PaginationView::new(offset, limit, total as usize, documents);

    debug!("returns: {:?}", ret);
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateDocumentsQuery {
    pub primary_key: Option<String>,
}

pub async fn add_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let params = params.into_inner();

    analytics.add_documents(&params, index_scheduler.index(&index_uid).is_err(), &req);

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid.into_inner(),
        params.primary_key,
        body,
        IndexDocumentsMethod::ReplaceDocuments,
        allow_index_creation,
    )
    .await?;

    Ok(HttpResponse::Accepted().json(task))
}

pub async fn update_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    path: web::Path<String>,
    params: web::Query<UpdateDocumentsQuery>,
    body: Payload,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let index_uid = path.into_inner();

    analytics.update_documents(&params, index_scheduler.index(&index_uid).is_err(), &req);

    let allow_index_creation = index_scheduler.filters().allow_index_creation;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid,
        params.into_inner().primary_key,
        body,
        IndexDocumentsMethod::UpdateDocuments,
        allow_index_creation,
    )
    .await?;

    Ok(HttpResponse::Accepted().json(task))
}

async fn document_addition(
    mime_type: Option<Mime>,
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: String,
    primary_key: Option<String>,
    mut body: Payload,
    method: IndexDocumentsMethod,
    allow_index_creation: bool,
) -> Result<TaskView, MeilisearchHttpError> {
    let format = match mime_type
        .as_ref()
        .map(|m| (m.type_().as_str(), m.subtype().as_str()))
    {
        Some(("application", "json")) => PayloadType::Json,
        Some(("application", "x-ndjson")) => PayloadType::Ndjson,
        Some(("text", "csv")) => PayloadType::Csv,
        Some((type_, subtype)) => {
            return Err(MeilisearchHttpError::InvalidContentType(
                format!("{}/{}", type_, subtype),
                ACCEPTED_CONTENT_TYPE.clone(),
            ))
        }
        None => {
            return Err(MeilisearchHttpError::MissingContentType(
                ACCEPTED_CONTENT_TYPE.clone(),
            ))
        }
    };

    let (uuid, mut update_file) = index_scheduler.create_update_file()?;

    // push the entire stream into a `Vec`.
    // TODO: Maybe we should write it to a file to reduce the RAM consumption
    // and then reread it to convert it to obkv?
    let mut buffer = Vec::new();
    while let Some(bytes) = body.next().await {
        buffer.extend_from_slice(&bytes?);
    }
    let reader = Cursor::new(buffer);

    let documents_count =
        tokio::task::spawn_blocking(move || -> Result<_, MeilisearchHttpError> {
            let documents_count = match format {
                PayloadType::Json => read_json(reader, update_file.as_file_mut())?,
                PayloadType::Csv => read_csv(reader, update_file.as_file_mut())?,
                PayloadType::Ndjson => read_ndjson(reader, update_file.as_file_mut())?,
            };
            // we NEED to persist the file here because we moved the `udpate_file` in another task.
            update_file.persist()?;
            Ok(documents_count)
        })
        .await;

    let documents_count = match documents_count {
        Ok(Ok(documents_count)) => documents_count,
        Ok(Err(e)) => {
            index_scheduler.delete_update_file(uuid)?;
            return Err(e);
        }
        Err(e) => {
            index_scheduler.delete_update_file(uuid)?;
            return Err(e.into());
        }
    };

    let task = KindWithContent::DocumentImport {
        method,
        content_file: uuid,
        documents_count,
        primary_key,
        allow_index_creation,
        index_uid,
    };

    let scheduler = index_scheduler.clone();
    let task = match tokio::task::spawn_blocking(move || scheduler.register(task)).await? {
        Ok(task) => task,
        Err(e) => {
            index_scheduler.delete_update_file(uuid)?;
            return Err(e.into());
        }
    };

    debug!("returns: {:?}", task);
    Ok(task)
}

pub async fn delete_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
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

    let task = KindWithContent::DocumentDeletion {
        index_uid: path.into_inner(),
        documents_ids: ids,
    };
    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn clear_all_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let task = KindWithContent::DocumentClear {
        index_uid: path.into_inner(),
    };
    let task = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}
