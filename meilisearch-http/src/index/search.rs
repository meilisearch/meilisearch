use std::collections::{BTreeMap, HashSet};
use std::mem;
use std::time::Instant;

use anyhow::bail;
use either::Either;
use heed::RoTxn;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig};
use milli::{facet::FacetValue, FacetCondition, MatchingWords};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::Index;

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
    pub facet_distributions: Option<Vec<String>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub hits: Vec<Map<String, Value>>,
    pub nb_hits: u64,
    pub exhaustive_nb_hits: bool, // currently this field only exist to be ISO and is always false
    pub query: String,
    pub limit: usize,
    pub offset: usize,
    pub processing_time_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_distributions: Option<BTreeMap<String, BTreeMap<FacetValue, u64>>>,
}

impl Index {
    pub fn perform_search(&self, query: SearchQuery) -> anyhow::Result<SearchResult> {
        let before_search = Instant::now();
        let rtxn = self.read_txn()?;

        let mut search = self.search(&rtxn);

        if let Some(ref query) = query.q {
            search.query(query);
        }

        search.limit(query.limit);
        search.offset(query.offset.unwrap_or_default());

        if let Some(ref facets) = query.facet_filters {
            if let Some(facets) = parse_facets(facets, self, &rtxn)? {
                search.facet_condition(facets);
            }
        }

        let milli::SearchResult {
            documents_ids,
            matching_words,
            candidates,
            ..
        } = search.execute()?;
        let mut documents = Vec::new();
        let fields_ids_map = self.fields_ids_map(&rtxn).unwrap();

        let fields_to_display =
            self.fields_to_display(&rtxn, query.attributes_to_retrieve, &fields_ids_map)?;

        let stop_words = fst::Set::default();
        let highlighter = Highlighter::new(&stop_words);

        for (_id, obkv) in self.documents(&rtxn, documents_ids)? {
            let mut object =
                milli::obkv_to_json(&fields_to_display, &fields_ids_map, obkv).unwrap();
            if let Some(ref attributes_to_highlight) = query.attributes_to_highlight {
                highlighter.highlight_record(&mut object, &matching_words, attributes_to_highlight);
            }
            documents.push(object);
        }

        let nb_hits = candidates.len();

        let facet_distributions = match query.facet_distributions {
            Some(ref fields) => {
                let mut facet_distribution = self.facets_distribution(&rtxn);
                if fields.iter().all(|f| f != "*") {
                    facet_distribution.facets(fields);
                }
                Some(facet_distribution.candidates(candidates).execute()?)
            }
            None => None,
        };

        let result = SearchResult {
            exhaustive_nb_hits: false, // not implemented, we use it to be ISO
            hits: documents,
            nb_hits,
            query: query.q.clone().unwrap_or_default(),
            limit: query.limit,
            offset: query.offset.unwrap_or_default(),
            processing_time_ms: before_search.elapsed().as_millis(),
            facet_distributions,
        };
        Ok(result)
    }
}

fn parse_facets_array(
    txn: &RoTxn,
    index: &Index,
    arr: &[Value],
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

    FacetCondition::from_array(txn, &index.0, ands)
}

pub struct Highlighter<'a, A> {
    analyzer: Analyzer<'a, A>,
}

impl<'a, A: AsRef<[u8]>> Highlighter<'a, A> {
    pub fn new(stop_words: &'a fst::Set<A>) -> Self {
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(stop_words));

        Self { analyzer }
    }

    pub fn highlight_value(&self, value: Value, words_to_highlight: &MatchingWords) -> Value {
        match value {
            Value::Null => Value::Null,
            Value::Bool(boolean) => Value::Bool(boolean),
            Value::Number(number) => Value::Number(number),
            Value::String(old_string) => {
                let mut string = String::new();
                let analyzed = self.analyzer.analyze(&old_string);
                for (word, token) in analyzed.reconstruct() {
                    if token.is_word() {
                        let to_highlight = words_to_highlight.matches(token.text());
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

    pub fn highlight_record(
        &self,
        object: &mut Map<String, Value>,
        words_to_highlight: &MatchingWords,
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

fn parse_facets(
    facets: &Value,
    index: &Index,
    txn: &RoTxn,
) -> anyhow::Result<Option<FacetCondition>> {
    match facets {
        // Disabled for now
        //Value::String(expr) => Ok(Some(FacetCondition::from_str(txn, index, expr)?)),
        Value::Array(arr) => parse_facets_array(txn, index, arr),
        v => bail!("Invalid facet expression, expected Array, found: {:?}", v),
    }
}
