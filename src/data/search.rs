use std::collections::HashSet;
use std::mem;
use std::time::Instant;

use serde_json::{Value, Map};
use serde::{Deserialize, Serialize};
use milli::{SearchResult as Results, obkv_to_json};
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};

use crate::error::Error;

use super::Data;

const DEFAULT_SEARCH_LIMIT: usize = 20;

const fn default_search_limit() -> usize { DEFAULT_SEARCH_LIMIT }

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[allow(dead_code)]
pub struct SearchQuery {
    q: Option<String>,
    offset: Option<usize>,
    #[serde(default = "default_search_limit")]
    limit: usize,
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
        let start =  Instant::now();
        let index = self.indexes
            .get(index)?
            .ok_or_else(|| Error::OpenIndex(format!("Index {} doesn't exists.", index.as_ref())))?;
        let Results { found_words, documents_ids, nb_hits, .. } = index.search(search_query)?;

        let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();

        let displayed_fields = match index.displayed_fields_ids(&rtxn).unwrap() {
            Some(fields) => fields,
            None => fields_ids_map.iter().map(|(id, _)| id).collect(),
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
}
