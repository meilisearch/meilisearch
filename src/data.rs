use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use async_compression::tokio_02::write::GzipEncoder;
use futures_util::stream::StreamExt;
use tokio::io::AsyncWriteExt;
use milli::{Index, SearchResult as Results, obkv_to_json};
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use sha2::Digest;
use serde_json::{Value, Map};
use serde::{Deserialize, Serialize};
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};

use crate::option::Opt;
use crate::updates::{UpdateQueue, UpdateMeta, UpdateStatus, UpdateMetaProgress};

const DEFAULT_SEARCH_LIMIT: usize = 20;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    q: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<Vec<String>>,
    attributes_to_crop: Option<Vec<String>>,
    crop_length: Option<usize>,
    attributes_to_highlight: Option<Vec<String>>,
    filters: Option<String>,
    matches: Option<bool>,
    facet_filters: Option<Value>,
    facets_distribution: Option<Vec<String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    hits: Vec<Map<String, Value>>,
    nb_hits: usize,
    query: String,
    limit: usize,
    offset: usize,
    processing_time_ms: u128,
}

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub struct DataInner {
    pub indexes: Arc<Index>,
    pub update_queue: Arc<UpdateQueue>,
    api_keys: ApiKeys,
    options: Opt,
}

#[derive(Clone)]
pub struct ApiKeys {
    pub public: Option<String>,
    pub private: Option<String>,
    pub master: Option<String>,
}

impl ApiKeys {
    pub fn generate_missing_api_keys(&mut self) {
        if let Some(master_key) = &self.master {
            if self.private.is_none() {
                let key = format!("{}-private", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.private = Some(format!("{:x}", sha));
            }
            if self.public.is_none() {
                let key = format!("{}-public", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.public = Some(format!("{:x}", sha));
            }
        }
    }
}

impl Data {
    pub fn new(options: Opt) -> anyhow::Result<Data> {
        let db_size = options.max_mdb_size.get_bytes() as usize;
        let path = options.db_path.join("main");
        create_dir_all(&path)?;
        let indexes = Index::new(&path, Some(db_size))?;
        let indexes = Arc::new(indexes);

        let update_queue = Arc::new(UpdateQueue::new(&options, indexes.clone())?);

        let mut api_keys = ApiKeys {
            master: options.clone().master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner = DataInner { indexes, options, update_queue, api_keys };
        let inner = Arc::new(inner);

        Ok(Data { inner })
    }

    pub async fn add_documents<B, E, S>(
        &self,
        _index: S,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        mut stream: impl futures::Stream<Item=Result<B, E>> + Unpin,
    ) -> anyhow::Result<UpdateStatus<UpdateMeta, UpdateMetaProgress, String>>
    where
        B: Deref<Target = [u8]>,
        E: std::error::Error + Send + Sync + 'static,
        S: AsRef<str>,
    {
        let file = tokio::task::spawn_blocking(tempfile::tempfile).await?;
        let file = tokio::fs::File::from_std(file?);
        let mut encoder = GzipEncoder::new(file);

        while let Some(result) = stream.next().await {
            let bytes = &*result?;
            encoder.write_all(&bytes[..]).await?;
        }

        encoder.shutdown().await?;
        let mut file = encoder.into_inner();
        file.sync_all().await?;
        let file = file.into_std().await;
        let mmap = unsafe { memmap::Mmap::map(&file)? };

        let meta = UpdateMeta::DocumentsAddition { method, format };

        let queue = self.update_queue.clone();
        let meta_cloned = meta.clone();
        let update_id = tokio::task::spawn_blocking(move || queue.register_update(&meta_cloned, &mmap[..])).await??;

        Ok(UpdateStatus::Pending { update_id, meta })
    }

    pub fn search<S: AsRef<str>>(&self, _index: S, search_query: SearchQuery) -> anyhow::Result<SearchResult> {
        let start =  Instant::now();
        let index = &self.indexes;
        let rtxn = index.read_txn()?;

        let mut search = index.search(&rtxn);
        if let Some(query) = &search_query.q {
            search.query(query);
        }

        if let Some(offset) = search_query.offset {
            search.offset(offset);
        }

        let limit = search_query.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
            search.limit(limit);

        let Results { found_words, documents_ids, nb_hits, .. } = search.execute().unwrap();

        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();

        let displayed_fields = match index.displayed_fields(&rtxn).unwrap() {
            Some(fields) => Cow::Borrowed(fields),
            None => Cow::Owned(fields_ids_map.iter().map(|(id, _)| id).collect()),
        };

        let attributes_to_highlight = match search_query.attributes_to_highlight {
            Some(fields) => fields.iter().map(ToOwned::to_owned).collect(),
            None => HashSet::new(),
        };

        let stop_words = fst::Set::default();
        let highlighter = Highlighter::new(&stop_words);
        let mut documents = Vec::new();
        for (_id, obkv) in index.documents(&rtxn, documents_ids).unwrap() {
            let mut object = obkv_to_json(&displayed_fields, &fields_ids_map, obkv).unwrap();
            highlighter.highlight_record(&mut object, &found_words, &attributes_to_highlight);
            documents.push(object);
        }

        let processing_time_ms = start.elapsed().as_millis();

        let result = SearchResult {
            hits: documents,
            nb_hits,
            query: search_query.q.unwrap_or_default(),
            offset: search_query.offset.unwrap_or(0),
            limit,
            processing_time_ms,
        };

        Ok(result)
    }

    #[inline]
    pub fn http_payload_size_limit(&self) -> usize {
        self.options.http_payload_size_limit.get_bytes() as usize
    }

    #[inline]
    pub fn api_keys(&self) -> &ApiKeys {
        &self.api_keys
    }
}

struct Highlighter<'a, A> {
    analyzer: Analyzer<'a, A>,
}

impl<'a, A: AsRef<[u8]>> Highlighter<'a, A> {
    fn new(stop_words: &'a fst::Set<A>) -> Self {
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));
        Self { analyzer }
    }

    fn highlight_value(&self, value: Value, words_to_highlight: &HashSet<String>) -> Value {
        match value {
            Value::Null => Value::Null,
            Value::Bool(boolean) => Value::Bool(boolean),
            Value::Number(number) => Value::Number(number),
            Value::String(old_string) => {
                let mut string = String::new();
                let analyzed = self.analyzer.analyze(&old_string);
                for (word, token) in analyzed.reconstruct() {
                    if token.is_word() {
                        let to_highlight = words_to_highlight.contains(token.text());
                        if to_highlight { string.push_str("<mark>") }
                        string.push_str(word);
                        if to_highlight { string.push_str("</mark>") }
                    } else {
                        string.push_str(word);
                    }
                }
                Value::String(string)
            },
            Value::Array(values) => {
                Value::Array(values.into_iter()
                    .map(|v| self.highlight_value(v, words_to_highlight))
                    .collect())
            },
            Value::Object(object) => {
                Value::Object(object.into_iter()
                    .map(|(k, v)| (k, self.highlight_value(v, words_to_highlight)))
                    .collect())
            },
        }
    }

    fn highlight_record(
        &self,
        object: &mut Map<String, Value>,
        words_to_highlight: &HashSet<String>,
        attributes_to_highlight: &HashSet<String>,
    ) {
        // TODO do we need to create a string for element that are not and needs to be highlight?
        for (key, value) in object.iter_mut() {
            if attributes_to_highlight.contains(key) {
                let old_value = mem::take(value);
                *value = self.highlight_value(old_value, words_to_highlight);
            }
        }
    }
}
