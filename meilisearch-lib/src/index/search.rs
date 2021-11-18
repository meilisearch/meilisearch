use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::time::Instant;

use either::Either;
use indexmap::IndexMap;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig, Token};
use milli::{AscDesc, FieldId, FieldsIdsMap, Filter, MatchingWords, SortError};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::index::error::FacetError;

use super::error::{IndexError, Result};
use super::index::Index;

pub type Document = IndexMap<String, Value>;
type MatchesInfo = BTreeMap<String, Vec<MatchInfo>>;

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct MatchInfo {
    start: usize,
    length: usize,
}

pub const DEFAULT_SEARCH_LIMIT: usize = 20;
const fn default_search_limit() -> usize {
    DEFAULT_SEARCH_LIMIT
}

pub const DEFAULT_CROP_LENGTH: usize = 200;
pub const fn default_crop_length() -> usize {
    DEFAULT_CROP_LENGTH
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    pub q: Option<String>,
    pub offset: Option<usize>,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    pub attributes_to_crop: Option<Vec<String>>,
    #[serde(default = "default_crop_length")]
    pub crop_length: usize,
    pub attributes_to_highlight: Option<HashSet<String>>,
    // Default to false
    #[serde(default = "Default::default")]
    pub matches: bool,
    pub filter: Option<Value>,
    pub sort: Option<Vec<String>>,
    pub facets_distribution: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SearchHit {
    #[serde(flatten)]
    pub document: Document,
    #[serde(rename = "_formatted", skip_serializing_if = "Document::is_empty")]
    pub formatted: Document,
    #[serde(rename = "_matchesInfo", skip_serializing_if = "Option::is_none")]
    pub matches_info: Option<MatchesInfo>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
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
    pub facets_distribution: Option<BTreeMap<String, BTreeMap<String, u64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exhaustive_facets_count: Option<bool>,
}

#[derive(Copy, Clone)]
struct FormatOptions {
    highlight: bool,
    crop: Option<usize>,
}

impl Index {
    pub fn perform_search(&self, query: SearchQuery) -> Result<SearchResult> {
        let before_search = Instant::now();
        let rtxn = self.read_txn()?;

        let mut search = self.search(&rtxn);

        if let Some(ref query) = query.q {
            search.query(query);
        }

        search.limit(query.limit);
        search.offset(query.offset.unwrap_or_default());

        if let Some(ref filter) = query.filter {
            if let Some(facets) = parse_filter(filter)? {
                search.filter(facets);
            }
        }

        if let Some(ref sort) = query.sort {
            let sort = match sort.iter().map(|s| AscDesc::from_str(s)).collect() {
                Ok(sorts) => sorts,
                Err(asc_desc_error) => {
                    return Err(IndexError::Milli(SortError::from(asc_desc_error).into()))
                }
            };

            search.sort_criteria(sort);
        }

        let milli::SearchResult {
            documents_ids,
            matching_words,
            candidates,
            ..
        } = search.execute()?;

        let fields_ids_map = self.fields_ids_map(&rtxn).unwrap();

        let displayed_ids = self
            .displayed_fields_ids(&rtxn)?
            .map(|fields| fields.into_iter().collect::<BTreeSet<_>>())
            .unwrap_or_else(|| fields_ids_map.iter().map(|(id, _)| id).collect());

        let fids = |attrs: &BTreeSet<String>| {
            let mut ids = BTreeSet::new();
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

        // The attributes to retrieve are the ones explicitly marked as to retrieve (all by default),
        // but these attributes must be also be present
        // - in the fields_ids_map
        // - in the the displayed attributes
        let to_retrieve_ids: BTreeSet<_> = query
            .attributes_to_retrieve
            .as_ref()
            .map(fids)
            .unwrap_or_else(|| displayed_ids.clone())
            .intersection(&displayed_ids)
            .cloned()
            .collect();

        let attr_to_highlight = query.attributes_to_highlight.unwrap_or_default();

        let attr_to_crop = query.attributes_to_crop.unwrap_or_default();

        // Attributes in `formatted_options` correspond to the attributes that will be in `_formatted`
        // These attributes are:
        // - the attributes asked to be highlighted or cropped (with `attributesToCrop` or `attributesToHighlight`)
        // - the attributes asked to be retrieved: these attributes will not be highlighted/cropped
        // But these attributes must be also present in displayed attributes
        let formatted_options = compute_formatted_options(
            &attr_to_highlight,
            &attr_to_crop,
            query.crop_length,
            &to_retrieve_ids,
            &fields_ids_map,
            &displayed_ids,
        );

        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);

        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut documents = Vec::new();

        let documents_iter = self.documents(&rtxn, documents_ids)?;

        for (_id, obkv) in documents_iter {
            let mut document = make_document(&to_retrieve_ids, &fields_ids_map, obkv)?;

            let matches_info = query
                .matches
                .then(|| compute_matches(&matching_words, &document, &analyzer));

            let formatted = format_fields(
                &fields_ids_map,
                obkv,
                &formatter,
                &matching_words,
                &formatted_options,
            )?;

            if let Some(sort) = query.sort.as_ref() {
                insert_geo_distance(sort, &mut document);
            }

            let hit = SearchHit {
                document,
                formatted,
                matches_info,
            };
            documents.push(hit);
        }

        let nb_hits = candidates.len();

        let facets_distribution = match query.facets_distribution {
            Some(ref fields) => {
                let mut facets_distribution = self.facets_distribution(&rtxn);
                if fields.iter().all(|f| f != "*") {
                    facets_distribution.facets(fields);
                }
                let distribution = facets_distribution.candidates(candidates).execute()?;

                Some(distribution)
            }
            None => None,
        };

        let exhaustive_facets_count = facets_distribution.as_ref().map(|_| false); // not implemented yet

        let result = SearchResult {
            exhaustive_nb_hits: false, // not implemented yet
            hits: documents,
            nb_hits,
            query: query.q.clone().unwrap_or_default(),
            limit: query.limit,
            offset: query.offset.unwrap_or_default(),
            processing_time_ms: before_search.elapsed().as_millis(),
            facets_distribution,
            exhaustive_facets_count,
        };
        Ok(result)
    }
}

fn insert_geo_distance(sorts: &[String], document: &mut Document) {
    lazy_static::lazy_static! {
        static ref GEO_REGEX: Regex =
            Regex::new(r"_geoPoint\(\s*([[:digit:].\-]+)\s*,\s*([[:digit:].\-]+)\s*\)").unwrap();
    };
    if let Some(capture_group) = sorts.iter().find_map(|sort| GEO_REGEX.captures(sort)) {
        // TODO: TAMO: milli encountered an internal error, what do we want to do?
        let base = [
            capture_group[1].parse().unwrap(),
            capture_group[2].parse().unwrap(),
        ];
        let geo_point = &document.get("_geo").unwrap_or(&json!(null));
        if let Some((lat, lng)) = geo_point["lat"].as_f64().zip(geo_point["lng"].as_f64()) {
            let distance = milli::distance_between_two_points(&base, &[lat, lng]);
            document.insert("_geoDistance".to_string(), json!(distance.round() as usize));
        }
    }
}

fn compute_matches<A: AsRef<[u8]>>(
    matcher: &impl Matcher,
    document: &Document,
    analyzer: &Analyzer<A>,
) -> MatchesInfo {
    let mut matches = BTreeMap::new();

    for (key, value) in document {
        let mut infos = Vec::new();
        compute_value_matches(&mut infos, value, matcher, analyzer);
        if !infos.is_empty() {
            matches.insert(key.clone(), infos);
        }
    }
    matches
}

fn compute_value_matches<'a, A: AsRef<[u8]>>(
    infos: &mut Vec<MatchInfo>,
    value: &Value,
    matcher: &impl Matcher,
    analyzer: &Analyzer<'a, A>,
) {
    match value {
        Value::String(s) => {
            let analyzed = analyzer.analyze(s);
            let mut start = 0;
            for (word, token) in analyzed.reconstruct() {
                if token.is_word() {
                    if let Some(length) = matcher.matches(token.text()) {
                        infos.push(MatchInfo { start, length });
                    }
                }

                start += word.len();
            }
        }
        Value::Array(vals) => vals
            .iter()
            .for_each(|val| compute_value_matches(infos, val, matcher, analyzer)),
        Value::Object(vals) => vals
            .values()
            .for_each(|val| compute_value_matches(infos, val, matcher, analyzer)),
        _ => (),
    }
}

fn compute_formatted_options(
    attr_to_highlight: &HashSet<String>,
    attr_to_crop: &[String],
    query_crop_length: usize,
    to_retrieve_ids: &BTreeSet<FieldId>,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) -> BTreeMap<FieldId, FormatOptions> {
    let mut formatted_options = BTreeMap::new();

    add_highlight_to_formatted_options(
        &mut formatted_options,
        attr_to_highlight,
        fields_ids_map,
        displayed_ids,
    );

    add_crop_to_formatted_options(
        &mut formatted_options,
        attr_to_crop,
        query_crop_length,
        fields_ids_map,
        displayed_ids,
    );

    // Should not return `_formatted` if no valid attributes to highlight/crop
    if !formatted_options.is_empty() {
        add_non_formatted_ids_to_formatted_options(&mut formatted_options, to_retrieve_ids);
    }

    formatted_options
}

fn add_highlight_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_highlight: &HashSet<String>,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) {
    for attr in attr_to_highlight {
        let new_format = FormatOptions {
            highlight: true,
            crop: None,
        };

        if attr == "*" {
            for id in displayed_ids {
                formatted_options.insert(*id, new_format);
            }
            break;
        }

        if let Some(id) = fields_ids_map.id(attr) {
            if displayed_ids.contains(&id) {
                formatted_options.insert(id, new_format);
            }
        }
    }
}

fn add_crop_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    attr_to_crop: &[String],
    crop_length: usize,
    fields_ids_map: &FieldsIdsMap,
    displayed_ids: &BTreeSet<FieldId>,
) {
    for attr in attr_to_crop {
        let mut split = attr.rsplitn(2, ':');
        let (attr_name, attr_len) = match split.next().zip(split.next()) {
            Some((len, name)) => {
                let crop_len = len.parse::<usize>().unwrap_or(crop_length);
                (name, crop_len)
            }
            None => (attr.as_str(), crop_length),
        };

        if attr_name == "*" {
            for id in displayed_ids {
                formatted_options
                    .entry(*id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions {
                        highlight: false,
                        crop: Some(attr_len),
                    });
            }
        }

        if let Some(id) = fields_ids_map.id(attr_name) {
            if displayed_ids.contains(&id) {
                formatted_options
                    .entry(id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions {
                        highlight: false,
                        crop: Some(attr_len),
                    });
            }
        }
    }
}

fn add_non_formatted_ids_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    to_retrieve_ids: &BTreeSet<FieldId>,
) {
    for id in to_retrieve_ids {
        formatted_options.entry(*id).or_insert(FormatOptions {
            highlight: false,
            crop: None,
        });
    }
}

fn make_document(
    attributes_to_retrieve: &BTreeSet<FieldId>,
    field_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReaderU16,
) -> Result<Document> {
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

fn format_fields<A: AsRef<[u8]>>(
    field_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReaderU16,
    formatter: &Formatter<A>,
    matching_words: &impl Matcher,
    formatted_options: &BTreeMap<FieldId, FormatOptions>,
) -> Result<Document> {
    let mut document = Document::new();

    for (id, format) in formatted_options {
        if let Some(value) = obkv.get(*id) {
            let mut value: Value = serde_json::from_slice(value)?;

            value = formatter.format_value(value, matching_words, *format);

            // This unwrap must be safe since we got the ids from the fields_ids_map just
            // before.
            let key = field_ids_map
                .name(*id)
                .expect("Missing field name")
                .to_string();

            document.insert(key, value);
        }
    }

    Ok(document)
}

/// trait to allow unit testing of `format_fields`
trait Matcher {
    fn matches(&self, w: &str) -> Option<usize>;
}

#[cfg(test)]
impl Matcher for BTreeMap<&str, Option<usize>> {
    fn matches(&self, w: &str) -> Option<usize> {
        self.get(w).cloned().flatten()
    }
}

impl Matcher for MatchingWords {
    fn matches(&self, w: &str) -> Option<usize> {
        self.matching_bytes(w)
    }
}

struct Formatter<'a, A> {
    analyzer: &'a Analyzer<'a, A>,
    marks: (String, String),
}

impl<'a, A: AsRef<[u8]>> Formatter<'a, A> {
    pub fn new(analyzer: &'a Analyzer<'a, A>, marks: (String, String)) -> Self {
        Self { analyzer, marks }
    }

    fn format_value(
        &self,
        value: Value,
        matcher: &impl Matcher,
        format_options: FormatOptions,
    ) -> Value {
        match value {
            Value::String(old_string) => {
                let value = self.format_string(old_string, matcher, format_options);
                Value::String(value)
            }
            Value::Array(values) => Value::Array(
                values
                    .into_iter()
                    .map(|v| {
                        self.format_value(
                            v,
                            matcher,
                            FormatOptions {
                                highlight: format_options.highlight,
                                crop: None,
                            },
                        )
                    })
                    .collect(),
            ),
            Value::Object(object) => Value::Object(
                object
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            self.format_value(
                                v,
                                matcher,
                                FormatOptions {
                                    highlight: format_options.highlight,
                                    crop: None,
                                },
                            ),
                        )
                    })
                    .collect(),
            ),
            Value::Number(number) => {
                let number_string_value =
                    self.format_string(number.to_string(), matcher, format_options);
                Value::String(number_string_value)
            }
            value => value,
        }
    }

    fn format_string(
        &self,
        s: String,
        matcher: &impl Matcher,
        format_options: FormatOptions,
    ) -> String {
        let analyzed = self.analyzer.analyze(&s);

        let tokens: Box<dyn Iterator<Item = (&str, Token)>> = match format_options.crop {
            Some(crop_len) => {
                let mut buffer = Vec::new();
                let mut tokens = analyzed.reconstruct().peekable();

                while let Some((word, token)) =
                    tokens.next_if(|(_, token)| matcher.matches(token.text()).is_none())
                {
                    buffer.push((word, token));
                }

                match tokens.next() {
                    Some(token) => {
                        let mut total_len: usize = buffer.iter().map(|(word, _)| word.len()).sum();
                        let before_iter = buffer.into_iter().skip_while(move |(word, _)| {
                            total_len -= word.len();
                            total_len >= crop_len
                        });

                        let mut taken_after = 0;
                        let after_iter = tokens.take_while(move |(word, _)| {
                            let take = taken_after < crop_len;
                            taken_after += word.chars().count();
                            take
                        });

                        let iter = before_iter.chain(Some(token)).chain(after_iter);

                        Box::new(iter)
                    }
                    // If no word matches in the attribute
                    None => {
                        let mut count = 0;
                        let iter = buffer.into_iter().take_while(move |(word, _)| {
                            let take = count < crop_len;
                            count += word.len();
                            take
                        });

                        Box::new(iter)
                    }
                }
            }
            None => Box::new(analyzed.reconstruct()),
        };

        tokens.fold(String::new(), |mut out, (word, token)| {
            // Check if we need to do highlighting or computed matches before calling
            // Matcher::match since the call is expensive.
            if format_options.highlight && token.is_word() {
                if let Some(length) = matcher.matches(token.text()) {
                    match word.get(..length).zip(word.get(length..)) {
                        Some((head, tail)) => {
                            out.push_str(&self.marks.0);
                            out.push_str(head);
                            out.push_str(&self.marks.1);
                            out.push_str(tail);
                        }
                        // if we are in the middle of a character
                        // or if all the word should be highlighted,
                        // we highlight the complete word.
                        None => {
                            out.push_str(&self.marks.0);
                            out.push_str(word);
                            out.push_str(&self.marks.1);
                        }
                    }
                    return out;
                }
            }
            out.push_str(word);
            out
        })
    }
}

fn parse_filter(facets: &Value) -> Result<Option<Filter>> {
    match facets {
        Value::String(expr) => {
            let condition = Filter::from_str(&expr)?;
            Ok(Some(condition))
        }
        Value::Array(arr) => parse_filter_array(arr),
        v => Err(FacetError::InvalidExpression(&["Array"], v.clone()).into()),
    }
}

fn parse_filter_array(arr: &[Value]) -> Result<Option<Filter>> {
    let mut ands = Vec::new();
    for value in arr {
        match value {
            Value::String(s) => ands.push(Either::Right(s.as_str())),
            Value::Array(arr) => {
                let mut ors = Vec::new();
                for value in arr {
                    match value {
                        Value::String(s) => ors.push(s.as_str()),
                        v => {
                            return Err(FacetError::InvalidExpression(&["String"], v.clone()).into())
                        }
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => {
                return Err(
                    FacetError::InvalidExpression(&["String", "[String]"], v.clone()).into(),
                )
            }
        }
    }

    Ok(Filter::from_array(ands)?)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn no_ids_no_formatted() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let id = fields.insert("test").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(id, Value::String("hello".into()).to_string().as_bytes())
            .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let formatted_options = BTreeMap::new();

        let matching_words = MatchingWords::default();

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert!(value.is_empty());
    }

    #[test]
    fn formatted_with_highlight_in_word() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("The Hobbit".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. R. R. Tolkien".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: true,
                crop: None,
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("hobbit", Some(3));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "The <em>Hob</em>bit");
        assert_eq!(value["author"], "J. R. R. Tolkien");
    }

    #[test]
    fn formatted_with_highlight_in_number() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();
        let publication_year = fields.insert("publication_year").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);

        obkv.insert(
            title,
            Value::String("The Hobbit".into()).to_string().as_bytes(),
        )
        .unwrap();

        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);

        obkv.insert(
            author,
            Value::String("J. R. R. Tolkien".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        obkv = obkv::KvWriter::new(&mut buf);

        obkv.insert(
            publication_year,
            Value::Number(1937.into()).to_string().as_bytes(),
        )
        .unwrap();

        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );
        formatted_options.insert(
            publication_year,
            FormatOptions {
                highlight: true,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("1937", Some(4));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "The Hobbit");
        assert_eq!(value["author"], "J. R. R. Tolkien");
        assert_eq!(value["publication_year"], "<em>1937</em>");
    }

    /// https://github.com/meilisearch/MeiliSearch/issues/1368
    #[test]
    fn formatted_with_highlight_emoji() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("Go💼od luck.".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("JacobLey".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: true,
                crop: None,
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        // emojis are deunicoded during tokenization
        // TODO Tokenizer should remove spaces after deunicode
        matching_words.insert("gobriefcase od", Some(11));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "<em>Go💼od</em> luck.");
        assert_eq!(value["author"], "JacobLey");
    }

    #[test]
    fn formatted_with_highlight_in_unicode_word() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(title, Value::String("étoile".into()).to_string().as_bytes())
            .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. R. R. Tolkien".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: true,
                crop: None,
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("etoile", Some(1));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "<em>étoile</em>");
        assert_eq!(value["author"], "J. R. R. Tolkien");
    }

    #[test]
    fn formatted_with_crop_2() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("Harry Potter and the Half-Blood Prince".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. K. Rowling".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: false,
                crop: Some(2),
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("potter", Some(6));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "Harry Potter and");
        assert_eq!(value["author"], "J. K. Rowling");
    }

    #[test]
    fn formatted_with_crop_10() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("Harry Potter and the Half-Blood Prince".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. K. Rowling".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: false,
                crop: Some(10),
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("potter", Some(6));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "Harry Potter and the Half");
        assert_eq!(value["author"], "J. K. Rowling");
    }

    #[test]
    fn formatted_with_crop_0() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("Harry Potter and the Half-Blood Prince".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. K. Rowling".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: false,
                crop: Some(0),
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("potter", Some(6));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "Potter");
        assert_eq!(value["author"], "J. K. Rowling");
    }

    #[test]
    fn formatted_with_crop_and_no_match() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("Harry Potter and the Half-Blood Prince".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. K. Rowling".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: false,
                crop: Some(6),
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: Some(20),
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("rowling", Some(3));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "Harry ");
        assert_eq!(value["author"], "J. K. Rowling");
    }

    #[test]
    fn formatted_with_crop_and_highlight() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("Harry Potter and the Half-Blood Prince".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. K. Rowling".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: true,
                crop: Some(1),
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("and", Some(3));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], " <em>and</em> ");
        assert_eq!(value["author"], "J. K. Rowling");
    }

    #[test]
    fn formatted_with_crop_and_highlight_in_word() {
        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);
        let formatter = Formatter::new(&analyzer, (String::from("<em>"), String::from("</em>")));

        let mut fields = FieldsIdsMap::new();
        let title = fields.insert("title").unwrap();
        let author = fields.insert("author").unwrap();

        let mut buf = Vec::new();
        let mut obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            title,
            Value::String("Harry Potter and the Half-Blood Prince".into())
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();
        obkv = obkv::KvWriter::new(&mut buf);
        obkv.insert(
            author,
            Value::String("J. K. Rowling".into()).to_string().as_bytes(),
        )
        .unwrap();
        obkv.finish().unwrap();

        let obkv = obkv::KvReader::new(&buf);

        let mut formatted_options = BTreeMap::new();
        formatted_options.insert(
            title,
            FormatOptions {
                highlight: true,
                crop: Some(9),
            },
        );
        formatted_options.insert(
            author,
            FormatOptions {
                highlight: false,
                crop: None,
            },
        );

        let mut matching_words = BTreeMap::new();
        matching_words.insert("blood", Some(3));

        let value = format_fields(
            &fields,
            obkv,
            &formatter,
            &matching_words,
            &formatted_options,
        )
        .unwrap();

        assert_eq!(value["title"], "the Half-<em>Blo</em>od Prince");
        assert_eq!(value["author"], "J. K. Rowling");
    }

    #[test]
    fn test_compute_value_matches() {
        let text = "Call me Ishmael. Some years ago—never mind how long precisely—having little or no money in my purse, and nothing particular to interest me on shore, I thought I would sail about a little and see the watery part of the world.";
        let value = serde_json::json!(text);

        let mut matcher = BTreeMap::new();
        matcher.insert("ishmael", Some(3));
        matcher.insert("little", Some(6));
        matcher.insert("particular", Some(1));

        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);

        let mut infos = Vec::new();

        compute_value_matches(&mut infos, &value, &matcher, &analyzer);

        let mut infos = infos.into_iter();
        let crop = |info: MatchInfo| &text[info.start..info.start + info.length];

        assert_eq!(crop(infos.next().unwrap()), "Ish");
        assert_eq!(crop(infos.next().unwrap()), "little");
        assert_eq!(crop(infos.next().unwrap()), "p");
        assert_eq!(crop(infos.next().unwrap()), "little");
        assert!(infos.next().is_none());
    }

    #[test]
    fn test_compute_match() {
        let value = serde_json::from_str(r#"{
            "color": "Green",
            "name": "Lucas Hess",
            "gender": "male",
            "address": "412 Losee Terrace, Blairstown, Georgia, 2825",
            "about": "Mollit ad in exercitation quis Laboris . Anim est ut consequat fugiat duis magna aliquip velit nisi. Commodo eiusmod est consequat proident consectetur aliqua enim fugiat. Aliqua adipisicing laboris elit proident enim veniam laboris mollit. Incididunt fugiat minim ad nostrud deserunt tempor in. Id irure officia labore qui est labore nulla nisi. Magna sit quis tempor esse consectetur amet labore duis aliqua consequat.\r\n"
  }"#).unwrap();
        let mut matcher = BTreeMap::new();
        matcher.insert("green", Some(3));
        matcher.insert("mollit", Some(6));
        matcher.insert("laboris", Some(7));

        let stop_words = fst::Set::default();
        let mut config = AnalyzerConfig::default();
        config.stop_words(&stop_words);
        let analyzer = Analyzer::new(config);

        let matches = compute_matches(&matcher, &value, &analyzer);
        assert_eq!(
            format!("{:?}", matches),
            r##"{"about": [MatchInfo { start: 0, length: 6 }, MatchInfo { start: 31, length: 7 }, MatchInfo { start: 191, length: 7 }, MatchInfo { start: 225, length: 7 }, MatchInfo { start: 233, length: 6 }], "color": [MatchInfo { start: 0, length: 3 }]}"##
        );
    }

    #[test]
    fn test_insert_geo_distance() {
        let value: Document = serde_json::from_str(
            r#"{
      "_geo": {
        "lat": 50.629973371633746,
        "lng": 3.0569447399419567
      },
      "city": "Lille",
      "id": "1"
    }"#,
        )
        .unwrap();

        let sorters = &["_geoPoint(50.629973371633746,3.0569447399419567):desc".to_string()];
        let mut document = value.clone();
        insert_geo_distance(sorters, &mut document);
        assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

        let sorters = &["_geoPoint(50.629973371633746, 3.0569447399419567):asc".to_string()];
        let mut document = value.clone();
        insert_geo_distance(sorters, &mut document);
        assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

        let sorters =
            &["_geoPoint(   50.629973371633746   ,  3.0569447399419567   ):desc".to_string()];
        let mut document = value.clone();
        insert_geo_distance(sorters, &mut document);
        assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

        let sorters = &[
            "prix:asc",
            "villeneuve:desc",
            "_geoPoint(50.629973371633746, 3.0569447399419567):asc",
            "ubu:asc",
        ]
        .map(|s| s.to_string());
        let mut document = value.clone();
        insert_geo_distance(sorters, &mut document);
        assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

        // only the first geoPoint is used to compute the distance
        let sorters = &[
            "chien:desc",
            "_geoPoint(50.629973371633746, 3.0569447399419567):asc",
            "pangolin:desc",
            "_geoPoint(100.0, -80.0):asc",
            "chat:asc",
        ]
        .map(|s| s.to_string());
        let mut document = value.clone();
        insert_geo_distance(sorters, &mut document);
        assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

        // there was no _geoPoint so nothing is inserted in the document
        let sorters = &["chien:asc".to_string()];
        let mut document = value;
        insert_geo_distance(sorters, &mut document);
        assert_eq!(document.get("_geoDistance"), None);
    }
}
