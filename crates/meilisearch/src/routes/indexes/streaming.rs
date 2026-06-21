use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures::Stream;
use meilisearch_types::error::ResponseError;
use meilisearch_types::milli::index::EmbeddingsWithMetadata;
use meilisearch_types::milli::vector::parsed_vectors::ExplicitVectors;
use meilisearch_types::milli::{self, DocumentId};
use meilisearch_types::{Document, Index};
use serde::Serialize;
use serde_json::Value;

use crate::error::MeilisearchHttpError;
use crate::search::RetrieveVectors;

/// Stream that serializes items as a JSON array: `[item1,item2,...]`
pub struct StreamedJsonArray<S, T>
where
    S: Stream<Item = Result<T, ResponseError>>,
    T: Serialize,
{
    stream: S,
    state: ArrayState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArrayState {
    Start,
    FirstItem,
    OtherItems,
    End,
}

impl<S, T> StreamedJsonArray<S, T>
where
    S: Stream<Item = Result<T, ResponseError>> + Unpin,
    T: Serialize,
{
    pub fn new(stream: S) -> Self {
        Self { stream, state: ArrayState::Start }
    }
}

impl<S, T> Stream for StreamedJsonArray<S, T>
where
    S: Stream<Item = Result<T, ResponseError>> + Unpin,
    T: Serialize,
{
    type Item = Result<Bytes, ResponseError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.state {
            ArrayState::Start => {
                self.state = ArrayState::FirstItem;
                Poll::Ready(Some(Ok(Bytes::from("["))))
            }
            ArrayState::FirstItem => match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    self.state = ArrayState::OtherItems;
                    match serde_json::to_vec(&item) {
                        Ok(json) => Poll::Ready(Some(Ok(Bytes::from(json)))),
                        Err(e) => Poll::Ready(Some(Err(ResponseError::from(
                            MeilisearchHttpError::from(e),
                        )))),
                    }
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.state = ArrayState::End;
                    Poll::Ready(Some(Ok(Bytes::from("]"))))
                }
                Poll::Pending => Poll::Pending,
            },
            ArrayState::OtherItems => match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => match serde_json::to_vec(&item) {
                    Ok(json) => {
                        let mut bytes = BytesMut::with_capacity(json.len() + 1);
                        bytes.put_u8(b',');
                        bytes.extend_from_slice(&json);
                        Poll::Ready(Some(Ok(bytes.freeze())))
                    }
                    Err(e) => {
                        Poll::Ready(Some(Err(ResponseError::from(MeilisearchHttpError::from(e)))))
                    }
                },
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.state = ArrayState::End;
                    Poll::Ready(Some(Ok(Bytes::from("]"))))
                }
                Poll::Pending => Poll::Pending,
            },
            ArrayState::End => Poll::Ready(None),
        }
    }
}

/// Stream that serializes `{ ...header fields..., "results": [ ... streamed items ... ] }`
/// by stripping the closing `}` from the header object and appending the `results` array.
pub struct StreamedJsonObject<S, T, H>
where
    S: Stream<Item = Result<T, ResponseError>>,
    T: Serialize,
    H: Serialize + Unpin,
{
    header: Option<H>,
    hits_stream: StreamedJsonArray<S, T>,
    state: ObjectState,
    hits_field: &'static str,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ObjectState {
    Header,
    Hits,
    Done,
}

impl<S, T, H> StreamedJsonObject<S, T, H>
where
    S: Stream<Item = Result<T, ResponseError>> + Unpin,
    T: Serialize,
    H: Serialize + Unpin,
{
    pub fn new(header: H, hits_stream: S) -> Self {
        Self {
            header: Some(header),
            hits_stream: StreamedJsonArray::new(hits_stream),
            state: ObjectState::Header,
            hits_field: "results",
        }
    }
}

impl<S, T, H> Stream for StreamedJsonObject<S, T, H>
where
    S: Stream<Item = Result<T, ResponseError>> + Unpin,
    T: Serialize,
    H: Serialize + Unpin,
{
    type Item = Result<Bytes, ResponseError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.state {
            ObjectState::Header => {
                let header = this.header.take().unwrap();
                match serde_json::to_vec(&header) {
                    Ok(mut json) => {
                        if let Some(last) = json.last_mut() {
                            if *last == b'}' {
                                json.pop();
                                let mut bytes = BytesMut::from(json.as_slice());
                                bytes.extend_from_slice(b",\"");
                                bytes.extend_from_slice(this.hits_field.as_bytes());
                                bytes.extend_from_slice(b"\":");
                                this.state = ObjectState::Hits;
                                return Poll::Ready(Some(Ok(bytes.freeze())));
                            }
                        }
                        Poll::Ready(Some(Err(ResponseError::from_msg(
                            "Invalid header".to_string(),
                            meilisearch_types::error::Code::Internal,
                        ))))
                    }
                    Err(e) => {
                        Poll::Ready(Some(Err(ResponseError::from(MeilisearchHttpError::from(e)))))
                    }
                }
            }
            ObjectState::Hits => match Pin::new(&mut this.hits_stream).poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    this.state = ObjectState::Done;
                    Poll::Ready(Some(Ok(Bytes::from("}"))))
                }
                Poll::Pending => Poll::Pending,
            },
            ObjectState::Done => Poll::Ready(None),
        }
    }
}

/// Header fields for a paginated document list (without `results`, which is streamed separately).
#[derive(Debug, Clone, Serialize)]
pub struct DocumentsPaginationHeader {
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
}

/// Load and stream documents one-by-one so the full result set is not held in memory.
pub(crate) fn stream_documents<S>(
    index: Index,
    doc_ids: Vec<DocumentId>,
    attributes_to_retrieve: Option<Vec<S>>,
    retrieve_vectors: RetrieveVectors,
) -> impl Stream<Item = Result<Document, ResponseError>>
where
    S: AsRef<str> + Send + 'static,
{
    let (tx, rx) = flume::unbounded::<Result<Document, ResponseError>>();
    tokio::task::spawn_blocking(move || {
        let rtxn = match index.read_txn() {
            Ok(rtxn) => rtxn,
            Err(e) => {
                let _ = tx.send(Err(ResponseError::from(e)));
                return;
            }
        };

        let fields_ids_map = match index.fields_ids_map(&rtxn) {
            Ok(map) => map,
            Err(e) => {
                let _ = tx.send(Err(ResponseError::from(e)));
                return;
            }
        };
        let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

        for id in doc_ids {
            let document = match index.document(&rtxn, id) {
                Ok(doc) => doc,
                Err(e) => {
                    let _ = tx.send(Err(ResponseError::from(e)));
                    break;
                }
            };

            let mut document = match milli::obkv_to_json(&all_fields, &fields_ids_map, document) {
                Ok(doc) => doc,
                Err(e) => {
                    let _ = tx.send(Err(ResponseError::from(e)));
                    break;
                }
            };

            match retrieve_vectors {
                RetrieveVectors::Hide => {
                    document.remove("_vectors");
                }
                RetrieveVectors::Retrieve => {
                    let mut vectors = match document.remove("_vectors") {
                        Some(Value::Object(map)) => map,
                        _ => Default::default(),
                    };
                    match index.embeddings(&rtxn, id) {
                        Ok(embeddings) => {
                            for (
                                name,
                                EmbeddingsWithMetadata { embeddings, regenerate, has_fragments: _ },
                            ) in embeddings
                            {
                                let embeddings = ExplicitVectors {
                                    embeddings: Some(embeddings.into()),
                                    regenerate,
                                };
                                match serde_json::to_value(embeddings) {
                                    Ok(val) => {
                                        vectors.insert(name, val);
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(ResponseError::from(
                                            MeilisearchHttpError::from(e),
                                        )));
                                        return;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(ResponseError::from(e)));
                            break;
                        }
                    }
                    document.insert("_vectors".into(), vectors.into());
                }
            }

            let document = match &attributes_to_retrieve {
                Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
                    document,
                    attributes_to_retrieve.iter().map(|s| s.as_ref()).chain(
                        (retrieve_vectors == RetrieveVectors::Retrieve).then_some("_vectors"),
                    ),
                ),
                None => document,
            };

            if tx.send(Ok(document)).is_err() {
                break;
            }
        }
    });
    rx.into_stream()
}
