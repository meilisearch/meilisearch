use std::collections::HashSet;
use std::mem;
use std::time::Instant;

use anyhow::{bail, Context};
use either::Either;
use heed::RoTxn;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
use milli::{obkv_to_json, FacetCondition, Index};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::Data;
use crate::index_controller::IndexController;

pub const DEFAULT_SEARCH_LIMIT: usize = 20;

const fn default_search_limit() -> usize {
    DEFAULT_SEARCH_LIMIT
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
pub struct SearchQuery {
    pub q: Option<String>,
    pub offset: Option<usize>,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    pub attributes_to_retrieve: Option<Vec<String>>,
    pub attributes_to_crop: Option<Vec<String>>,
    pub crop_length: Option<usize>,
    pub attributes_to_highlight: Option<HashSet<String>>,
    pub filters: Option<String>,
    pub matches: Option<bool>,
    pub facet_filters: Option<Value>,
    pub facets_distribution: Option<Vec<String>>,
}

impl SearchQuery {
    pub fn perform(&self, index: impl AsRef<Index>) -> anyhow::Result<SearchResult> {
        let index = index.as_ref();
        let before_search = Instant::now();
        let rtxn = index.read_txn()?;

        let mut search = index.search(&rtxn);

        if let Some(ref query) = self.q {
            search.query(query);
        }

        search.limit(self.limit);
        search.offset(self.offset.unwrap_or_default());

        if let Some(ref facets) = self.facet_filters {
            if let Some(facets) = parse_facets(facets, index, &rtxn)? {
                search.facet_condition(facets);
            }
        }

        let milli::SearchResult {
            documents_ids,
            found_words,
            candidates,
        } = search.execute()?;

        let mut documents = Vec::new();
        let fields_ids_map = index.fields_ids_map(&rtxn)?;

        let displayed_fields_ids = index.displayed_fields_ids(&rtxn)?;

        let attributes_to_retrieve_ids = match self.attributes_to_retrieve {
            Some(ref attrs) if attrs.iter().any(|f| f == "*") => None,
            Some(ref attrs) => attrs
                .iter()
                .filter_map(|f| fields_ids_map.id(f))
                .collect::<Vec<_>>()
                .into(),
            None => None,
        };

        let displayed_fields_ids = match (displayed_fields_ids, attributes_to_retrieve_ids) {
            (_, Some(ids)) => ids,
            (Some(ids), None) => ids,
            (None, None) => fields_ids_map.iter().map(|(id, _)| id).collect(),
        };

        let stop_words = fst::Set::default();
        let highlighter = Highlighter::new(&stop_words);

        for (_id, obkv) in index.documents(&rtxn, documents_ids)? {
            let mut object = obkv_to_json(&displayed_fields_ids, &fields_ids_map, obkv)?;
            if let Some(ref attributes_to_highlight) = self.attributes_to_highlight {
                highlighter.highlight_record(&mut object, &found_words, attributes_to_highlight);
            }
            documents.push(object);
        }

        Ok(SearchResult {
            hits: documents,
            nb_hits: candidates.len(),
            query: self.q.clone().unwrap_or_default(),
            limit: self.limit,
            offset: self.offset.unwrap_or_default(),
            processing_time_ms: before_search.elapsed().as_millis(),
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    hits: Vec<Map<String, Value>>,
    nb_hits: u64,
    query: String,
    limit: usize,
    offset: usize,
    processing_time_ms: u128,
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
                        if to_highlight {
                            string.push_str("<mark>")
                        }
                        string.push_str(word);
                        if to_highlight {
                            string.push_str("</mark>")
                        }
                    } else {
                        string.push_str(word);
                    }
                }
                Value::String(string)
            }
            Value::Array(values) => Value::Array(
                values
                    .into_iter()
                    .map(|v| self.highlight_value(v, words_to_highlight))
                    .collect(),
            ),
            Value::Object(object) => Value::Object(
                object
                    .into_iter()
                    .map(|(k, v)| (k, self.highlight_value(v, words_to_highlight)))
                    .collect(),
            ),
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

impl Data {
    pub fn search<S: AsRef<str>>(
        &self,
        index: S,
        search_query: SearchQuery,
    ) -> anyhow::Result<SearchResult> {
        match self.index_controller.index(&index)? {
            Some(index) => Ok(search_query.perform(index)?),
            None => bail!("index {:?} doesn't exists", index.as_ref()),
        }
    }

    pub async fn retrieve_documents<S>(
        &self,
        index: impl AsRef<str> + Send + Sync + 'static,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> anyhow::Result<Vec<Map<String, Value>>>
    where
        S: AsRef<str> + Send + Sync + 'static,
    {
        let index_controller = self.index_controller.clone();
        let documents: anyhow::Result<_> = tokio::task::spawn_blocking(move || {
            let index = index_controller
                .index(&index)?
                .with_context(|| format!("Index {:?} doesn't exist", index.as_ref()))?;

            let txn = index.read_txn()?;

            let fields_ids_map = index.fields_ids_map(&txn)?;

            let attributes_to_retrieve_ids = match attributes_to_retrieve {
                Some(attrs) => attrs
                    .iter()
                    .filter_map(|f| fields_ids_map.id(f.as_ref()))
                    .collect::<Vec<_>>(),
                None => fields_ids_map.iter().map(|(id, _)| id).collect(),
            };

            let iter = index.documents.range(&txn, &(..))?.skip(offset).take(limit);

            let mut documents = Vec::new();

            for entry in iter {
                let (_id, obkv) = entry?;
                let object = obkv_to_json(&attributes_to_retrieve_ids, &fields_ids_map, obkv)?;
                documents.push(object);
            }

            Ok(documents)
        })
        .await?;
        documents
    }

    pub async fn retrieve_document<S>(
        &self,
        index: impl AsRef<str> + Sync + Send + 'static,
        document_id: impl AsRef<str> + Sync + Send + 'static,
        attributes_to_retrieve: Option<Vec<S>>,
    ) -> anyhow::Result<Map<String, Value>>
    where
        S: AsRef<str> + Sync + Send + 'static,
    {
        let index_controller = self.index_controller.clone();
        let document: anyhow::Result<_> = tokio::task::spawn_blocking(move || {
            let index = index_controller
                .index(&index)?
                .with_context(|| format!("Index {:?} doesn't exist", index.as_ref()))?;
            let txn = index.read_txn()?;

            let fields_ids_map = index.fields_ids_map(&txn)?;

            let attributes_to_retrieve_ids = match attributes_to_retrieve {
                Some(attrs) => attrs
                    .iter()
                    .filter_map(|f| fields_ids_map.id(f.as_ref()))
                    .collect::<Vec<_>>(),
                None => fields_ids_map.iter().map(|(id, _)| id).collect(),
            };

            let internal_id = index
                .external_documents_ids(&txn)?
                .get(document_id.as_ref().as_bytes())
                .with_context(|| format!("Document with id {} not found", document_id.as_ref()))?;

            let document = index
                .documents(&txn, std::iter::once(internal_id))?
                .into_iter()
                .next()
                .map(|(_, d)| d);

            match document {
                Some(document) => Ok(obkv_to_json(
                    &attributes_to_retrieve_ids,
                    &fields_ids_map,
                    document,
                )?),
                None => bail!("Document with id {} not found", document_id.as_ref()),
            }
        })
        .await?;
        document
    }
}

fn parse_facets_array(
    txn: &RoTxn,
    index: &Index,
    arr: &Vec<Value>,
) -> anyhow::Result<Option<FacetCondition>> {
    let mut ands = Vec::new();
    for value in arr {
        match value {
            Value::String(s) => ands.push(Either::Right(s.clone())),
            Value::Array(arr) => {
                let mut ors = Vec::new();
                for value in arr {
                    match value {
                        Value::String(s) => ors.push(s.clone()),
                        v => bail!("Invalid facet expression, expected String, found: {:?}", v),
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => bail!(
                "Invalid facet expression, expected String or [String], found: {:?}",
                v
            ),
        }
    }

    FacetCondition::from_array(txn, index, ands)
}

fn parse_facets(
    facets: &Value,
    index: &Index,
    txn: &RoTxn,
) -> anyhow::Result<Option<FacetCondition>> {
    match facets {
        // Disabled for now
        //Value::String(expr) => Ok(Some(FacetCondition::from_str(txn, index, expr)?)),
        Value::Array(arr) => parse_facets_array(txn, index, arr),
        v => bail!(
            "Invalid facet expression, expected Array, found: {:?}",
            v
        ),
    }
}

