use std::io::{ErrorKind, Write};
use std::pin::Pin;

use actix_web::http::header::CONTENT_TYPE;
use actix_web::web::Data;
use actix_web::{web, HttpMessage, HttpRequest, HttpResponse};
use bstr::ByteSlice as _;
use bytes::Bytes;
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::Deserr;
use futures::StreamExt;
use futures_util::Stream;
use index_scheduler::{IndexScheduler, TaskId};
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::document_formats::{read_csv, read_json, read_ndjson, PayloadType};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::milli::DocumentId;
use meilisearch_types::star_or::OptionStarOrList;
use meilisearch_types::tasks::KindWithContent;
use meilisearch_types::{milli, Document, Index};
use mime::Mime;
use once_cell::sync::Lazy;
use roaring::RoaringBitmap;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tracing::debug;

use crate::analytics::{Analytics, DocumentDeletionKind, DocumentFetchKind};
use crate::error::MeilisearchHttpError;
use crate::error::PayloadError::ReceivePayload;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::payload::Payload;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::{
    get_task_id, is_dry_run, PaginationView, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT,
};
use crate::search::parse_filter;
use crate::Opt;

static ACCEPTED_CONTENT_TYPE: Lazy<Vec<String>> = Lazy::new(|| {
    vec!["application/json".to_string(), "application/x-ndjson".to_string(), "text/csv".to_string()]
});

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
            None => Err(MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone())),
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
            .route(web::get().to(SeqHandler(get_documents)))
            .route(web::post().to(SeqHandler(replace_documents)))
            .route(web::put().to(SeqHandler(update_documents)))
            .route(web::delete().to(SeqHandler(clear_all_documents))),
    )
    // these routes need to be before the /documents/{document_id} to match properly
    .service(
        web::resource("/delete-batch").route(web::post().to(SeqHandler(delete_documents_batch))),
    )
    .service(web::resource("/delete").route(web::post().to(SeqHandler(delete_documents_by_filter))))
    .service(web::resource("/fetch").route(web::post().to(SeqHandler(documents_by_query_post))))
    .service(
        web::resource("/{document_id}")
            .route(web::get().to(SeqHandler(get_document)))
            .route(web::delete().to(SeqHandler(delete_document))),
    );
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct GetDocument {
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFields>)]
    fields: OptionStarOrList<String>,
}

pub async fn get_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    document_param: web::Path<DocumentParam>,
    params: AwebQueryParameter<GetDocument, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam { index_uid, document_id } = document_param.into_inner();
    debug!(parameters = ?params, "Get document");
    let index_uid = IndexUid::try_from(index_uid)?;

    analytics.get_fetch_documents(&DocumentFetchKind::PerDocumentId, &req);

    let GetDocument { fields } = params.into_inner();
    let attributes_to_retrieve = fields.merge_star_and_none();

    let index = index_scheduler.index(&index_uid)?;
    let document = retrieve_document(&index, &document_id, attributes_to_retrieve)?;
    debug!(returns = ?document, "Get document");
    Ok(HttpResponse::Ok().json(document))
}

pub async fn delete_document(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    path: web::Path<DocumentParam>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let DocumentParam { index_uid, document_id } = path.into_inner();
    let index_uid = IndexUid::try_from(index_uid)?;

    analytics.delete_documents(DocumentDeletionKind::PerDocumentId, &req);

    let task = KindWithContent::DocumentDeletion {
        index_uid: index_uid.to_string(),
        documents_ids: vec![document_id],
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();
    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct BrowseQueryGet {
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentOffset>)]
    offset: Param<usize>,
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidDocumentLimit>)]
    limit: Param<usize>,
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFields>)]
    fields: OptionStarOrList<String>,
    #[deserr(default, error = DeserrQueryParamError<InvalidDocumentFilter>)]
    filter: Option<String>,
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct BrowseQuery {
    #[deserr(default, error = DeserrJsonError<InvalidDocumentOffset>)]
    offset: usize,
    #[deserr(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidDocumentLimit>)]
    limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidDocumentFields>)]
    fields: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidDocumentFilter>)]
    filter: Option<Value>,
}

pub async fn documents_by_query_post(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<BrowseQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let body = body.into_inner();
    debug!(parameters = ?body, "Get documents POST");

    analytics.post_fetch_documents(
        &DocumentFetchKind::Normal {
            with_filter: body.filter.is_some(),
            limit: body.limit,
            offset: body.offset,
        },
        &req,
    );

    documents_by_query(&index_scheduler, index_uid, body)
}

pub async fn get_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<BrowseQueryGet, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?params, "Get documents GET");

    let BrowseQueryGet { limit, offset, fields, filter } = params.into_inner();

    let filter = match filter {
        Some(f) => match serde_json::from_str(&f) {
            Ok(v) => Some(v),
            _ => Some(Value::String(f)),
        },
        None => None,
    };

    let query = BrowseQuery {
        offset: offset.0,
        limit: limit.0,
        fields: fields.merge_star_and_none(),
        filter,
    };

    analytics.get_fetch_documents(
        &DocumentFetchKind::Normal {
            with_filter: query.filter.is_some(),
            limit: query.limit,
            offset: query.offset,
        },
        &req,
    );

    documents_by_query(&index_scheduler, index_uid, query)
}

pub struct Writer2Streamer {
    sender: tokio::sync::mpsc::Sender<Result<Bytes, anyhow::Error>>,
}

impl Write for Writer2Streamer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sender.blocking_send(Ok(buf.to_vec().into())).map_err(std::io::Error::other)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn stream(
    data: impl Serialize + Send + 'static,
) -> impl Stream<Item = Result<Bytes, anyhow::Error>> {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(1);

    tokio::task::spawn_blocking(move || {
        serde_json::to_writer(std::io::BufWriter::new(Writer2Streamer { sender }), &data)
    });
    futures_util::stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|value| (value, receiver))
    })
}

fn documents_by_query(
    index_scheduler: &IndexScheduler,
    index_uid: web::Path<String>,
    query: BrowseQuery,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let BrowseQuery { offset, limit, fields, filter } = query;

    let index = index_scheduler.index(&index_uid)?;
    let documents = retrieve_documents(index, offset, limit, filter, fields)?;

    let ret = PaginationView::new(offset, limit, documents.total_documents as usize, documents);

    debug!(returns = ?ret, "Get documents");

    Ok(HttpResponse::Ok().streaming(stream(ret)))
}

#[derive(Deserialize, Debug, Deserr)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct UpdateDocumentsQuery {
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexPrimaryKey>)]
    pub primary_key: Option<String>,
    #[deserr(default, try_from(char) = from_char_csv_delimiter -> DeserrQueryParamError<InvalidDocumentCsvDelimiter>, error = DeserrQueryParamError<InvalidDocumentCsvDelimiter>)]
    pub csv_delimiter: Option<u8>,
}

fn from_char_csv_delimiter(
    c: char,
) -> Result<Option<u8>, DeserrQueryParamError<InvalidDocumentCsvDelimiter>> {
    if c.is_ascii() {
        Ok(Some(c as u8))
    } else {
        Err(DeserrQueryParamError::new(
            format!("csv delimiter must be an ascii character. Found: `{}`", c),
            Code::InvalidDocumentCsvDelimiter,
        ))
    }
}

pub async fn replace_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<UpdateDocumentsQuery, DeserrQueryParamError>,
    body: Payload,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    debug!(parameters = ?params, "Replace documents");
    let params = params.into_inner();

    analytics.add_documents(&params, index_scheduler.index(&index_uid).is_err(), &req);

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid,
        params.primary_key,
        params.csv_delimiter,
        body,
        IndexDocumentsMethod::ReplaceDocuments,
        uid,
        dry_run,
        allow_index_creation,
    )
    .await?;
    debug!(returns = ?task, "Replace documents");

    Ok(HttpResponse::Accepted().json(task))
}

pub async fn update_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<UpdateDocumentsQuery, DeserrQueryParamError>,
    body: Payload,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let params = params.into_inner();
    debug!(parameters = ?params, "Update documents");

    analytics.update_documents(&params, index_scheduler.index(&index_uid).is_err(), &req);

    let allow_index_creation = index_scheduler.filters().allow_index_creation(&index_uid);
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = document_addition(
        extract_mime_type(&req)?,
        index_scheduler,
        index_uid,
        params.primary_key,
        params.csv_delimiter,
        body,
        IndexDocumentsMethod::UpdateDocuments,
        uid,
        dry_run,
        allow_index_creation,
    )
    .await?;
    debug!(returns = ?task, "Update documents");

    Ok(HttpResponse::Accepted().json(task))
}

#[allow(clippy::too_many_arguments)]
async fn document_addition(
    mime_type: Option<Mime>,
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_ADD }>, Data<IndexScheduler>>,
    index_uid: IndexUid,
    primary_key: Option<String>,
    csv_delimiter: Option<u8>,
    mut body: Payload,
    method: IndexDocumentsMethod,
    task_id: Option<TaskId>,
    dry_run: bool,
    allow_index_creation: bool,
) -> Result<SummarizedTaskView, MeilisearchHttpError> {
    let format = match (
        mime_type.as_ref().map(|m| (m.type_().as_str(), m.subtype().as_str())),
        csv_delimiter,
    ) {
        (Some(("application", "json")), None) => PayloadType::Json,
        (Some(("application", "x-ndjson")), None) => PayloadType::Ndjson,
        (Some(("text", "csv")), None) => PayloadType::Csv { delimiter: b',' },
        (Some(("text", "csv")), Some(delimiter)) => PayloadType::Csv { delimiter },

        (Some(("application", "json")), Some(_)) => {
            return Err(MeilisearchHttpError::CsvDelimiterWithWrongContentType(String::from(
                "application/json",
            )))
        }
        (Some(("application", "x-ndjson")), Some(_)) => {
            return Err(MeilisearchHttpError::CsvDelimiterWithWrongContentType(String::from(
                "application/x-ndjson",
            )))
        }
        (Some((type_, subtype)), _) => {
            return Err(MeilisearchHttpError::InvalidContentType(
                format!("{}/{}", type_, subtype),
                ACCEPTED_CONTENT_TYPE.clone(),
            ))
        }
        (None, _) => {
            return Err(MeilisearchHttpError::MissingContentType(ACCEPTED_CONTENT_TYPE.clone()))
        }
    };

    let (uuid, mut update_file) = index_scheduler.create_update_file(dry_run)?;

    let temp_file = match tempfile() {
        Ok(file) => file,
        Err(e) => return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e)))),
    };

    let async_file = File::from_std(temp_file);
    let mut buffer = BufWriter::new(async_file);

    let mut buffer_write_size: usize = 0;
    while let Some(result) = body.next().await {
        let byte = result?;

        if byte.is_empty() && buffer_write_size == 0 {
            return Err(MeilisearchHttpError::MissingPayload(format));
        }

        match buffer.write_all(&byte).await {
            Ok(()) => buffer_write_size += 1,
            Err(e) => return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e)))),
        }
    }

    if let Err(e) = buffer.flush().await {
        return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e))));
    }

    if buffer_write_size == 0 {
        return Err(MeilisearchHttpError::MissingPayload(format));
    }

    if let Err(e) = buffer.seek(std::io::SeekFrom::Start(0)).await {
        return Err(MeilisearchHttpError::Payload(ReceivePayload(Box::new(e))));
    }

    let read_file = buffer.into_inner().into_std().await;
    let documents_count = tokio::task::spawn_blocking(move || {
        let documents_count = match format {
            PayloadType::Json => read_json(&read_file, &mut update_file)?,
            PayloadType::Csv { delimiter } => read_csv(&read_file, &mut update_file, delimiter)?,
            PayloadType::Ndjson => read_ndjson(&read_file, &mut update_file)?,
        };
        // we NEED to persist the file here because we moved the `udpate_file` in another task.
        update_file.persist()?;
        Ok(documents_count)
    })
    .await;

    let documents_count = match documents_count {
        Ok(Ok(documents_count)) => documents_count,
        // in this case the file has not possibly be persisted.
        Ok(Err(e)) => return Err(e),
        Err(e) => {
            // Here the file MAY have been persisted or not.
            // We don't know thus we ignore the file not found error.
            match index_scheduler.delete_update_file(uuid) {
                Ok(()) => (),
                Err(index_scheduler::Error::FileStore(file_store::Error::IoError(e)))
                    if e.kind() == ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        index_uuid = %uuid,
                        "Unknown error happened while deleting a malformed update file: {e}"
                    );
                }
            }
            // We still want to return the original error to the end user.
            return Err(e.into());
        }
    };

    let task = KindWithContent::DocumentAdditionOrUpdate {
        method,
        content_file: uuid,
        documents_count,
        primary_key,
        allow_index_creation,
        index_uid: index_uid.to_string(),
    };

    let scheduler = index_scheduler.clone();
    let task = match tokio::task::spawn_blocking(move || scheduler.register(task, task_id, dry_run))
        .await?
    {
        Ok(task) => task,
        Err(e) => {
            index_scheduler.delete_update_file(uuid)?;
            return Err(e.into());
        }
    };

    Ok(task.into())
}

pub async fn delete_documents_batch(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: web::Json<Vec<Value>>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Delete documents by batch");
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    analytics.delete_documents(DocumentDeletionKind::PerBatch, &req);

    let ids = body
        .iter()
        .map(|v| v.as_str().map(String::from).unwrap_or_else(|| v.to_string()))
        .collect();

    let task =
        KindWithContent::DocumentDeletion { index_uid: index_uid.to_string(), documents_ids: ids };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete documents by batch");
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct DocumentDeletionByFilter {
    #[deserr(error = DeserrJsonError<InvalidDocumentFilter>, missing_field_error = DeserrJsonError::missing_document_filter)]
    filter: Value,
}

pub async fn delete_documents_by_filter(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<DocumentDeletionByFilter, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    debug!(parameters = ?body, "Delete documents by filter");
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let index_uid = index_uid.into_inner();
    let filter = body.into_inner().filter;

    analytics.delete_documents(DocumentDeletionKind::PerFilter, &req);

    // we ensure the filter is well formed before enqueuing it
    || -> Result<_, ResponseError> {
        Ok(crate::search::parse_filter(&filter)?.ok_or(MeilisearchHttpError::EmptyFilter)?)
    }()
    // and whatever was the error, the error code should always be an InvalidDocumentFilter
    .map_err(|err| ResponseError::from_msg(err.message, Code::InvalidDocumentFilter))?;
    let task = KindWithContent::DocumentDeletionByFilter { index_uid, filter_expr: filter };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete documents by filter");
    Ok(HttpResponse::Accepted().json(task))
}

pub async fn clear_all_documents(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DOCUMENTS_DELETE }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    analytics.delete_documents(DocumentDeletionKind::ClearAll, &req);

    let task = KindWithContent::DocumentClear { index_uid: index_uid.to_string() };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Delete all documents");
    Ok(HttpResponse::Accepted().json(task))
}

fn some_documents<'a, 't: 'a>(
    index: &'a Index,
    rtxn: &'t RoTxn,
    doc_ids: impl IntoIterator<Item = DocumentId> + 'a,
) -> Result<impl Iterator<Item = Result<Document, ResponseError>> + 'a, ResponseError> {
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

    Ok(index.iter_documents(rtxn, doc_ids)?.map(move |ret| {
        ret.map_err(ResponseError::from).and_then(|(_key, document)| -> Result<_, ResponseError> {
            Ok(milli::obkv_to_json(&all_fields, &fields_ids_map, document)?)
        })
    }))
}

pub struct DocumentsStreamer {
    attributes_to_retrieve: Option<Vec<String>>,
    documents: RoaringBitmap,
    // safety: The `rtxn` contains a reference to the index thus:
    //         - The `rtxn` MUST BE dropped before the index.
    //         - The index MUST BE `Pin`ned in RAM and never moved.
    rtxn: Option<RoTxn<'static>>,
    index: Pin<Box<Index>>,
    pub total_documents: u64,
}

impl Drop for DocumentsStreamer {
    fn drop(&mut self) {
        // safety: we drop the rtxn before the index
        self.rtxn = None;
    }
}

impl Serialize for DocumentsStreamer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let rtxn = self.rtxn.as_ref().unwrap();

        let mut seq = serializer.serialize_seq(Some(self.documents.len() as usize)).unwrap();

        let documents = some_documents(&self.index, rtxn, self.documents.iter()).unwrap();
        for document in documents {
            let document = document.unwrap();
            let document = match self.attributes_to_retrieve {
                Some(ref attributes_to_retrieve) => permissive_json_pointer::select_values(
                    &document,
                    attributes_to_retrieve.iter().map(|s| s.as_ref()),
                ),
                None => document,
            };

            seq.serialize_element(&document)?;
        }
        seq.end()
    }
}

fn retrieve_documents(
    index: Index,
    offset: usize,
    limit: usize,
    filter: Option<Value>,
    attributes_to_retrieve: Option<Vec<String>>,
) -> Result<DocumentsStreamer, ResponseError> {
    // safety: The index MUST NOT move while we hold the `rtxn` on it
    let index = Box::pin(index);
    let rtxn = index.read_txn()?;

    let filter = &filter;
    let filter = if let Some(filter) = filter {
        parse_filter(filter)
            .map_err(|err| ResponseError::from_msg(err.to_string(), Code::InvalidDocumentFilter))?
    } else {
        None
    };

    let candidates = if let Some(filter) = filter {
        filter.evaluate(&rtxn, &index).map_err(|err| match err {
            milli::Error::UserError(milli::UserError::InvalidFilter(_)) => {
                ResponseError::from_msg(err.to_string(), Code::InvalidDocumentFilter)
            }
            e => e.into(),
        })?
    } else {
        index.documents_ids(&rtxn)?
    };

    Ok(DocumentsStreamer {
        total_documents: candidates.len(),
        attributes_to_retrieve,
        documents: candidates.into_iter().skip(offset).take(limit).collect(),
        // safety: It is safe to make the lifetime in the Rtxn static because it points to the index right below.
        //         The index is `Pin`ned on the RAM and won't move even if the structure is moved.
        //         The `rtxn` is held in an `Option`, so we're able to drop it before dropping the index.
        rtxn: Some(unsafe { std::mem::transmute(rtxn) }),
        index,
    })
}

fn retrieve_document<S: AsRef<str>>(
    index: &Index,
    doc_id: &str,
    attributes_to_retrieve: Option<Vec<S>>,
) -> Result<Document, ResponseError> {
    let txn = index.read_txn()?;

    let fields_ids_map = index.fields_ids_map(&txn)?;
    let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

    let internal_id = index
        .external_documents_ids()
        .get(&txn, doc_id)?
        .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))?;

    let document = index
        .documents(&txn, std::iter::once(internal_id))?
        .into_iter()
        .next()
        .map(|(_, d)| d)
        .ok_or_else(|| MeilisearchHttpError::DocumentNotFound(doc_id.to_string()))?;

    let document = meilisearch_types::milli::obkv_to_json(&all_fields, &fields_ids_map, document)?;
    let document = match &attributes_to_retrieve {
        Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
            &document,
            attributes_to_retrieve.iter().map(|s| s.as_ref()),
        ),
        None => document,
    };

    Ok(document)
}
