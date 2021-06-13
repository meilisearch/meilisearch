use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

use anyhow::bail;
use either::Either;
use heed::RoTxn;
use indexmap::IndexMap;
use itertools::Itertools;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig, Token};
use milli::{FilterCondition, FieldId, FieldsIdsMap, MatchingWords};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Index;

pub type Document = IndexMap<String, Value>;

pub const DEFAULT_SEARCH_LIMIT: usize = 20;
const fn default_search_limit() -> usize {
    DEFAULT_SEARCH_LIMIT
}

pub const DEFAULT_CROP_LENGTH: usize = 200;
const fn default_crop_length() -> usize {
    DEFAULT_CROP_LENGTH
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    pub q: Option<String>,
    pub offset: Option<usize>,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    pub attributes_to_retrieve: Option<HashSet<String>>,
    pub attributes_to_crop: Option<Vec<String>>,
    #[serde(default = "default_crop_length")]
    pub crop_length: usize,
    pub attributes_to_highlight: Option<HashSet<String>>,
    pub matches: Option<bool>,
    pub filter: Option<Value>,
    pub facet_distributions: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    #[serde(flatten)]
    pub document: Document,
    #[serde(rename = "_formatted", skip_serializing_if = "Document::is_empty")]
    pub formatted: Document,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub nb_hits: u64,
    pub exhaustive_nb_hits: bool,
    pub query: String,
    pub limit: usize,
    pub offset: usize,
    pub processing_time_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_distributions: Option<BTreeMap<String, BTreeMap<String, u64>>>,
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

        if let Some(ref filter) = query.filter {
            if let Some(facets) = parse_facets(filter, self, &rtxn)? {
                search.filter(facets);
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

        let displayed_ids = self
            .displayed_fields_ids(&rtxn)?
            .map(|fields| fields.into_iter().collect::<HashSet<_>>())
            .unwrap_or_else(|| fields_ids_map.iter().map(|(id, _)| id).collect());

        let fids = |attrs: &HashSet<String>| {
            let mut ids = HashSet::new();
            for attr in attrs {
                if attr == "*" {
                    ids = displayed_ids.clone();
                    break;
                }

                if let Some(id) = fields_ids_map.id(attr) {
                    ids.insert(id);
                }
            }
            ids
        };

        let to_retrieve_ids = query
            .attributes_to_retrieve
            .as_ref()
            .map(fids)
            .unwrap_or_else(|| displayed_ids.clone());

        let to_highlight_ids = query
            .attributes_to_highlight
            .as_ref()
            .map(fids)
            .unwrap_or_default();

        let to_crop_ids_length = query
            .attributes_to_crop
            .as_ref()
            .map(|attributes: &Vec<String>| {
                let mut ids_length_crop = HashMap::new();
                for attribute in attributes {
                    let mut attr_name = attribute.clone();
                    let mut attr_len = Some(query.crop_length);

                    if attr_name.contains(':') {
                        let mut split = attr_name.rsplit(':');
                        attr_len = match split.next() {
                            Some(s) => s.parse::<usize>().ok(),
                            None => None,
                        };
                        attr_name = split.flat_map(|s| s.chars()).collect();
                    }

                    if attr_name == "*" {
                        let ids = displayed_ids.clone();
                        for id in ids {
                            ids_length_crop.insert(id, attr_len);
                        }
                    }

                    if let Some(id) = fields_ids_map.id(&attr_name) {
                        ids_length_crop.insert(id, attr_len);
                    }
                }
                ids_length_crop
            })
            .unwrap_or_default();

        let to_crop_ids = to_crop_ids_length
            .clone()
            .into_iter()
            .map(|(k, _)| k)
            .collect::<HashSet<_>>();

        // The attributes to retrieve are:
        // - the ones explicitly marked as to retrieve that are also in the displayed attributes
        let all_attributes: Vec<_> = to_retrieve_ids
            .intersection(&displayed_ids)
            .cloned()
            .sorted()
            .collect();

        // The formatted attributes are:
        // - The one in either highlighted attributes or cropped attributes if there are attributes
        // to retrieve
        // - All the attributes to retrieve if there are either highlighted or cropped attributes
        // the request specified that all attributes are to retrieve (i.e attributes to retrieve is
        // empty in the query)
        let all_formatted = if query.attributes_to_retrieve.is_none() {
            if query.attributes_to_highlight.is_some() || query.attributes_to_crop.is_some() {
                Cow::Borrowed(&all_attributes)
            } else {
                Cow::Owned(Vec::new())
            }
        } else {
            let attrs = (&to_crop_ids | &to_highlight_ids)
                .intersection(&displayed_ids)
                .cloned()
                .collect::<Vec<_>>();
            Cow::Owned(attrs)
        };

        let stop_words = fst::Set::default();
        let formatter =
            Formatter::new(&stop_words, (String::from("<em>"), String::from("</em>")));

        for (_id, obkv) in self.documents(&rtxn, documents_ids)? {
            let document = make_document(&all_attributes, &fields_ids_map, obkv)?;
            let formatted = compute_formatted(
                &fields_ids_map,
                obkv,
                &formatter,
                &matching_words,
                all_formatted.as_ref().as_slice(),
                &to_highlight_ids,
                &to_crop_ids_length,
            )?;
            let hit = SearchHit {
                document,
                formatted,
            };
            documents.push(hit);
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
            exhaustive_nb_hits: false, // not implemented yet
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

fn make_document(
    attributes_to_retrieve: &[FieldId],
    field_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReader,
) -> anyhow::Result<Document> {
    let mut document = Document::new();
    for attr in attributes_to_retrieve {
        if let Some(value) = obkv.get(*attr) {
            let value = serde_json::from_slice(value)?;

            // This unwrap must be safe since we got the ids from the fields_ids_map just
            // before.
            let key = field_ids_map
                .name(*attr)
                .expect("Missing field name")
                .to_string();

            document.insert(key, value);
        }
    }
    Ok(document)
}

fn compute_formatted<A: AsRef<[u8]>>(
    field_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReader,
    formatter: &Formatter<A>,
    matching_words: &impl Matcher,
    all_formatted: &[FieldId],
    to_highlight_fields: &HashSet<FieldId>,
    to_crop_fields: &HashMap<FieldId, Option<usize>>,
) -> anyhow::Result<Document> {
    let mut document = Document::new();

    for field in all_formatted {
        if let Some(value) = obkv.get(*field) {
            let mut value: Value = serde_json::from_slice(value)?;

            value = formatter.format_value(
                value,
                matching_words,
                to_crop_fields.get(field).copied().flatten(),
                to_highlight_fields.contains(field),
            );

            // This unwrap must be safe since we got the ids from the fields_ids_map just
            // before.
            let key = field_ids_map
                .name(*field)
                .expect("Missing field name")
                .to_string();

            document.insert(key, value);
        }
    }

    Ok(document)
}

/// trait to allow unit testing of `compute_formatted`
trait Matcher {
    fn matches(&self, w: &str) -> bool;
}

#[cfg(test)]
impl Matcher for HashSet<String> {
    fn matches(&self, w: &str) -> bool {
        self.contains(w)
    }
}

impl Matcher for MatchingWords {
    fn matches(&self, w: &str) -> bool {
        self.matching_bytes(w).is_some()
    }
}

struct Formatter<'a, A> {
    analyzer: Analyzer<'a, A>,
    marks: (String, String),
}

impl<'a, A: AsRef<[u8]>> Formatter<'a, A> {
    pub fn new(stop_words: &'a fst::Set<A>, marks: (String, String)) -> Self {
        let mut config = AnalyzerConfig::default();
        config.stop_words(stop_words);

        let analyzer = Analyzer::new(config);

        Self { analyzer, marks }
    }

    fn format_value(
        &self,
        value: Value,
        matcher: &impl Matcher,
        need_to_crop: Option<usize>,
        need_to_highlight: bool,
    ) -> Value {
        match value {
            Value::String(old_string) => {
                let value =
                    self.format_string(old_string, matcher, need_to_crop, need_to_highlight);
                Value::String(value)
            }
            Value::Array(values) => Value::Array(
                values
                    .into_iter()
                    .map(|v| self.format_value(v, matcher, None, need_to_highlight))
                    .collect(),
            ),
            Value::Object(object) => Value::Object(
                object
                    .into_iter()
                    .map(|(k, v)| (k, self.format_value(v, matcher, None, need_to_highlight)))
                    .collect(),
            ),
            value => value,
        }
    }

    fn format_string(
        &self,
        s: String,
        matcher: &impl Matcher,
        need_to_crop: Option<usize>,
        need_to_highlight: bool,
    ) -> String {
        let analyzed = self.analyzer.analyze(&s);

        let tokens: Box<dyn Iterator<Item = (&str, Token)>> = match need_to_crop {
            Some(crop_len) => {
                let mut buffer = VecDeque::new();
                let mut tokens = analyzed.reconstruct().peekable();
                let mut taken_before = 0;
                while let Some((word, token)) = tokens.next_if(|(_, token)| !matcher.matches(token.text())) {
                    buffer.push_back((word, token));
                    taken_before += word.chars().count();
                    while taken_before > crop_len {
                        // Around to the previous word
                        if let Some((word, _)) = buffer.front() {
                            if taken_before - word.chars().count() < crop_len {
                                break;
                            }
                        }
                        if let Some((word, _)) = buffer.pop_front() {
                            taken_before -= word.chars().count();
                        }
                    }
                }

                if let Some(token) = tokens.next() {
                    buffer.push_back(token);
                }

                let mut taken_after = 0;
                let after_iter = tokens
                    .take_while(move |(word, _)| {
                        let take = taken_after < crop_len;
                        taken_after += word.chars().count();
                        take
                    });

                let iter = buffer
                    .into_iter()
                    .chain(after_iter);

                Box::new(iter)
            }
            None => Box::new(analyzed.reconstruct()),
        };

        tokens
            .map(|(word, token)| {
                if need_to_highlight && token.is_word() && matcher.matches(token.text()) {
                    let mut new_word = String::new();
                    new_word.push_str(&self.marks.0);
                    new_word.push_str(&word);
                    new_word.push_str(&self.marks.1);
                    new_word
                } else {
                    word.to_string()
                }
            })
            .collect::<String>()
    }
}

fn parse_facets(
    facets: &Value,
    index: &Index,
    txn: &RoTxn,
) -> anyhow::Result<Option<FilterCondition>> {
    match facets {
        Value::String(expr) => Ok(Some(FilterCondition::from_str(txn, index, expr)?)),
        Value::Array(arr) => parse_facets_array(txn, index, arr),
        v => bail!("Invalid facet expression, expected Array, found: {:?}", v),
    }
}

fn parse_facets_array(
    txn: &RoTxn,
    index: &Index,
    arr: &[Value],
) -> anyhow::Result<Option<FilterCondition>> {
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

    FilterCondition::from_array(txn, &index.0, ands)
}

#[cfg(test)]
mod test {
    use std::iter::FromIterator;

    use super::*;

    #[test]
    fn no_formatted() {
        let stop_words = fst::Set::default();
        let formatter =
            Formatter::new(&stop_words, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let id = fields.insert("test").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(id, Value::String("hello".into()).to_string().as_bytes())
            .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let all_formatted = Vec::new();
        let to_highlight_ids = HashSet::new();
        let to_crop_ids = HashMap::new();

        let matching_words = MatchingWords::default();

        let value = compute_formatted(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &all_formatted,
            &to_highlight_ids,
            &to_crop_ids,
        )
        .unwrap();

        assert!(value.is_empty());
    }

    #[test]
    fn formatted_no_highlight() {
        let stop_words = fst::Set::default();
        let formatter =
            Formatter::new(&stop_words, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let id = fields.insert("test").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(id, Value::String("hello".into()).to_string().as_bytes())
            .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let all_formatted = vec![id];
        let to_highlight_ids = HashSet::new();
        let to_crop_ids = HashMap::new();

        let matching_words = MatchingWords::default();

        let value = compute_formatted(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &all_formatted,
            &to_highlight_ids,
            &to_crop_ids,
        )
        .unwrap();

        assert_eq!(value["test"], "hello");
    }

    #[test]
    fn formatted_with_highlight() {
        let stop_words = fst::Set::default();
        let formatter =
            Formatter::new(&stop_words, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let id = fields.insert("test").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(id, Value::String("hello".into()).to_string().as_bytes())
            .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let all_formatted = vec![id];
        let to_highlight_ids = HashSet::from_iter(Some(id));
        let to_crop_ids = HashMap::new();

        let matching_words = HashSet::from_iter(Some(String::from("hello")));

        let value = compute_formatted(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &all_formatted,
            &to_highlight_ids,
            &to_crop_ids,
        )
        .unwrap();

        assert_eq!(value["test"], "<em>hello</em>");
    }
}
