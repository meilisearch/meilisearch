use std::pin::Pin;
use std::task::{Context, Poll};
use futures::Stream;
use serde::Serialize;
use bytes::{Bytes, BytesMut, BufMut};
use meilisearch_types::error::ResponseError;
use meilisearch_types::Index;
use meilisearch_types::milli;
use meilisearch_types::milli::obkv_to_json;
use meilisearch_types::milli::index::EmbeddingsWithMetadata;
use meilisearch_types::milli::vector::parsed_vectors::ExplicitVectors;
use meilisearch_types::milli::score_details::ScoreDetails;
use crate::search::{AttributesFormat, SearchHit, make_hits};
use crate::error::MeilisearchHttpError;
use serde_json::Value;

pub struct StreamedJsonArray<S, T>
where
    S: Stream<Item = Result<T, ResponseError>>,
    T: Serialize,
{
    stream: S,
    state: State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
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
        Self { stream, state: State::Start }
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
            State::Start => {
                self.state = State::FirstItem;
                Poll::Ready(Some(Ok(Bytes::from("["))))
            }
            State::FirstItem => {
                match Pin::new(&mut self.stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => {
                        self.state = State::OtherItems;
                        match serde_json::to_vec(&item) {
                            Ok(json) => Poll::Ready(Some(Ok(Bytes::from(json)))),
                            Err(e) => Poll::Ready(Some(Err(ResponseError::from(MeilisearchHttpError::from(e))))),
                        }
                    }
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        self.state = State::End;
                        Poll::Ready(Some(Ok(Bytes::from("]"))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            State::OtherItems => {
                match Pin::new(&mut self.stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => {
                        match serde_json::to_vec(&item) {
                            Ok(json) => {
                                let mut bytes = BytesMut::with_capacity(json.len() + 1);
                                bytes.put_u8(b',');
                                bytes.extend_from_slice(&json);
                                Poll::Ready(Some(Ok(bytes.freeze())))
                            }
                            Err(e) => Poll::Ready(Some(Err(ResponseError::from(MeilisearchHttpError::from(e))))),
                        }
                    }
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        self.state = State::End;
                        Poll::Ready(Some(Ok(Bytes::from("]"))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            State::End => Poll::Ready(None),
        }
    }
}

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

    pub fn new_search(header: H, hits_stream: S) -> Self {
        Self {
            header: Some(header),
            hits_stream: StreamedJsonArray::new(hits_stream),
            state: ObjectState::Header,
            hits_field: "hits",
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
                        Poll::Ready(Some(Err(ResponseError::from_msg("Invalid header".to_string(), meilisearch_types::error::Code::Internal))))
                    }
                    Err(e) => Poll::Ready(Some(Err(ResponseError::from(MeilisearchHttpError::from(e))))),
                }
            }
            ObjectState::Hits => {
                match Pin::new(&mut this.hits_stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        this.state = ObjectState::Done;
                        Poll::Ready(Some(Ok(Bytes::from("}"))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            ObjectState::Done => Poll::Ready(None),
        }
    }
}

pub(crate) fn stream_documents<I, S>(
    index: Index,
    ids: I,
    attributes_to_retrieve: Option<Vec<S>>,
    retrieve_vectors: crate::search::RetrieveVectors,
) -> impl Stream<Item = Result<meilisearch_types::Document, ResponseError>>
where
    I: Iterator<Item = u32> + Send + 'static,
    S: AsRef<str> + Send + 'static,
{
    let (tx, rx) = flume::unbounded::<Result<meilisearch_types::Document, ResponseError>>();
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

        for id in ids {
            let doc_ptr = match index.document(&rtxn, id) {
                Ok(doc) => doc,
                Err(e) => {
                    let _ = tx.send(Err(ResponseError::from(e)));
                    break;
                }
            };
            
            let mut document = match obkv_to_json(&all_fields, &fields_ids_map, doc_ptr) {
                Ok(doc) => doc,
                Err(e) => {
                    let _ = tx.send(Err(ResponseError::from(e)));
                    break;
                }
            };

            // Handle vectors
            if let Ok(embeddings) = index.embeddings(&rtxn, id) {
                 match retrieve_vectors {
                    crate::search::RetrieveVectors::Hide => {
                        document.remove("_vectors");
                    }
                    crate::search::RetrieveVectors::Retrieve => {
                        let mut vectors = match document.remove("_vectors") {
                            Some(Value::Object(map)) => map,
                            _ => Default::default(),
                        };
                        for (name, EmbeddingsWithMetadata { embeddings, regenerate, .. }) in embeddings {
                            let embeddings = ExplicitVectors { embeddings: Some(embeddings.into()), regenerate };
                            if let Ok(val) = serde_json::to_value(embeddings) {
                                vectors.insert(name, val);
                            }
                        }
                        document.insert("_vectors".to_string(), Value::Object(vectors));
                    }
                }
            }

            // Apply projection
            let document = match &attributes_to_retrieve {
                Some(attributes_to_retrieve) => permissive_json_pointer::select_values(
                    &document,
                    attributes_to_retrieve.iter().map(|s| s.as_ref()).chain(
                        (retrieve_vectors == crate::search::RetrieveVectors::Retrieve).then_some("_vectors"),
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

pub(crate) fn stream_search_hits(
    index: Index,
    format: AttributesFormat,
    matching_words: milli::MatchingWords,
    documents_ids: Vec<u32>,
    document_scores: Vec<Vec<ScoreDetails>>,
) -> impl Stream<Item = Result<SearchHit, ResponseError>> {
    let (tx, rx) = flume::unbounded::<Result<SearchHit, ResponseError>>();
    let progress = meilisearch_types::milli::progress::Progress::default();
    tokio::task::spawn_blocking(move || {
        let rtxn = match index.read_txn() {
            Ok(rtxn) => rtxn,
            Err(e) => {
                let _ = tx.send(Err(ResponseError::from(e)));
                return;
            }
        };

        let dictionary = match index.dictionary(&rtxn) {
            Ok(dict) => dict,
            Err(e) => {
                let _ = tx.send(Err(ResponseError::from(e)));
                return;
            }
        };
        let dictionary_vec: Option<Vec<&str>> =
            dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
        
        let separators = match index.allowed_separators(&rtxn) {
            Ok(sep) => sep,
            Err(e) => {
                let _ = tx.send(Err(ResponseError::from(e)));
                return;
            }
        };
        let separators_vec: Option<Vec<&str>> =
            separators.as_ref().map(|x| x.iter().map(String::as_str).collect());

        let hits_iter = match make_hits(
            &index,
            &rtxn,
            format,
            matching_words,
            documents_ids.into_iter().zip(document_scores.iter()),
            &progress,
            dictionary_vec.as_deref(),
            separators_vec.as_deref(),
        ) {
            Ok(iter) => iter,
            Err(e) => {
                let _ = tx.send(Err(ResponseError::from(e)));
                return;
            }
        };

        for hit in hits_iter {
            if tx.send(hit.map_err(ResponseError::from)).is_err() {
                break;
            }
        }
    });
    rx.into_stream()
}
