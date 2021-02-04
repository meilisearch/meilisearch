use std::collections::HashSet;
use std::mem;
use std::time::Instant;

use anyhow::bail;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
use milli::{Index, obkv_to_json, FacetCondition};
use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};

use crate::index_controller::IndexController;
use super::Data;

const DEFAULT_SEARCH_LIMIT: usize = 20;

const fn default_search_limit() -> usize { DEFAULT_SEARCH_LIMIT }

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
    pub facet_condition: Option<String>,
}

impl SearchQuery {
    pub fn perform(&self, index: impl AsRef<Index>) -> anyhow::Result<SearchResult>{
        let index = index.as_ref();
        let before_search = Instant::now();
        let rtxn = index.read_txn().unwrap();

        let mut search = index.search(&rtxn);

        if let Some(ref query) = self.q {
            search.query(query);
        }

        search.limit(self.limit);
        search.offset(self.offset.unwrap_or_default());

        if let Some(ref condition) = self.facet_condition {
            if !condition.trim().is_empty() {
                let condition = FacetCondition::from_str(&rtxn, &index, &condition)?;
                search.facet_condition(condition);
            }
        }

        let milli::SearchResult { documents_ids, found_words, candidates } = search.execute()?;

        let mut documents = Vec::new();
        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();

        let displayed_fields = match index.displayed_fields_ids(&rtxn).unwrap() {
            Some(fields) => fields,
            None => fields_ids_map.iter().map(|(id, _)| id).collect(),
        };

        let stop_words = fst::Set::default();
        let highlighter = Highlighter::new(&stop_words);

        for (_id, obkv) in index.documents(&rtxn, documents_ids).unwrap() {
            let mut object = obkv_to_json(&displayed_fields, &fields_ids_map, obkv).unwrap();
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

impl Data {
    pub fn search<S: AsRef<str>>(&self, index: S, search_query: SearchQuery) -> anyhow::Result<SearchResult> {
        match self.index_controller.index(&index)? {
            Some(index) => Ok(search_query.perform(index)?),
            None => bail!("index {:?} doesn't exists", index.as_ref()),
        }
    }
}
