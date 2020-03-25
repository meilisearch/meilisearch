use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::From;
use std::error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use log::error;
use meilisearch_core::criterion::*;
use meilisearch_core::settings::RankingRule;
use meilisearch_core::{Highlight, Index, MainT, RankedMap};
use meilisearch_schema::{FieldId, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use siphasher::sip::SipHasher;

#[derive(Debug)]
pub enum Error {
    SearchDocuments(String),
    RetrieveDocument(u64, String),
    DocumentNotFound(u64),
    CropFieldWrongType(String),
    AttributeNotFoundOnDocument(String),
    AttributeNotFoundOnSchema(String),
    MissingFilterValue,
    UnknownFilteredAttribute,
    Internal(String),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;

        match self {
            SearchDocuments(err) => write!(f, "impossible to search documents; {}", err),
            RetrieveDocument(id, err) => write!(
                f,
                "impossible to retrieve the document with id: {}; {}",
                id, err
            ),
            DocumentNotFound(id) => write!(f, "document {} not found", id),
            CropFieldWrongType(field) => {
                write!(f, "the field {} cannot be cropped it's not a string", field)
            }
            AttributeNotFoundOnDocument(field) => {
                write!(f, "field {} is not found on document", field)
            }
            AttributeNotFoundOnSchema(field) => write!(f, "field {} is not found on schema", field),
            MissingFilterValue => f.write_str("a filter doesn't have a value to compare it with"),
            UnknownFilteredAttribute => {
                f.write_str("a filter is specifying an unknown schema attribute")
            }
            Internal(err) => write!(f, "internal error; {}", err),
        }
    }
}

impl From<meilisearch_core::Error> for Error {
    fn from(error: meilisearch_core::Error) -> Self {
        Error::Internal(error.to_string())
    }
}

impl From<heed::Error> for Error {
    fn from(error: heed::Error) -> Self {
        Error::Internal(error.to_string())
    }
}

pub trait IndexSearchExt {
    fn new_search(&self, query: String) -> SearchBuilder;
}

impl IndexSearchExt for Index {
    fn new_search(&self, query: String) -> SearchBuilder {
        SearchBuilder {
            index: self,
            query,
            offset: 0,
            limit: 20,
            attributes_to_crop: None,
            attributes_to_retrieve: None,
            attributes_to_highlight: None,
            filters: None,
            timeout: Duration::from_millis(30),
            matches: false,
        }
    }
}

pub struct SearchBuilder<'a> {
    index: &'a Index,
    query: String,
    offset: usize,
    limit: usize,
    attributes_to_crop: Option<HashMap<String, usize>>,
    attributes_to_retrieve: Option<HashSet<String>>,
    attributes_to_highlight: Option<HashSet<String>>,
    filters: Option<String>,
    timeout: Duration,
    matches: bool,
}

impl<'a> SearchBuilder<'a> {
    pub fn offset(&mut self, value: usize) -> &SearchBuilder {
        self.offset = value;
        self
    }

    pub fn limit(&mut self, value: usize) -> &SearchBuilder {
        self.limit = value;
        self
    }

    pub fn attributes_to_crop(&mut self, value: HashMap<String, usize>) -> &SearchBuilder {
        self.attributes_to_crop = Some(value);
        self
    }

    pub fn attributes_to_retrieve(&mut self, value: HashSet<String>) -> &SearchBuilder {
        self.attributes_to_retrieve = Some(value);
        self
    }

    pub fn add_retrievable_field(&mut self, value: String) -> &SearchBuilder {
        let attributes_to_retrieve = self.attributes_to_retrieve.get_or_insert(HashSet::new());
        attributes_to_retrieve.insert(value);
        self
    }

    pub fn attributes_to_highlight(&mut self, value: HashSet<String>) -> &SearchBuilder {
        self.attributes_to_highlight = Some(value);
        self
    }

    pub fn filters(&mut self, value: String) -> &SearchBuilder {
        self.filters = Some(value);
        self
    }

    pub fn timeout(&mut self, value: Duration) -> &SearchBuilder {
        self.timeout = value;
        self
    }

    pub fn get_matches(&mut self) -> &SearchBuilder {
        self.matches = true;
        self
    }

    pub fn search(&self, reader: &heed::RoTxn<MainT>) -> Result<SearchResult, Error> {
        let schema = self.index.main.schema(reader);
        let schema = schema.map_err(|e| Error::Internal(e.to_string()))?;
        let schema = match schema {
            Some(schema) => schema,
            None => return Err(Error::Internal(String::from("missing schema"))),
        };

        let ranked_map = self.index.main.ranked_map(reader);
        let ranked_map = ranked_map.map_err(|e| Error::Internal(e.to_string()))?;
        let ranked_map = ranked_map.unwrap_or_default();

        // Change criteria
        let mut query_builder = match self.get_criteria(reader, &ranked_map, &schema)? {
            Some(criteria) => self.index.query_builder_with_criteria(criteria),
            None => self.index.query_builder(),
        };

        if let Some(filters) = &self.filters {
            let mut split = filters.split(':');
            match (split.next(), split.next()) {
                (Some(_), None) | (Some(_), Some("")) => return Err(Error::MissingFilterValue),
                (Some(attr), Some(value)) => {
                    let ref_reader = reader;
                    let ref_index = &self.index;
                    let value = value.trim().to_lowercase();

                    let attr = match schema.id(attr) {
                        Some(attr) => attr,
                        None => return Err(Error::UnknownFilteredAttribute),
                    };

                    query_builder.with_filter(move |id| {
                        let attr = attr;
                        let index = ref_index;
                        let reader = ref_reader;

                        match index.document_attribute::<Value>(reader, id, attr) {
                            Ok(Some(Value::String(s))) => s.to_lowercase() == value,
                            Ok(Some(Value::Bool(b))) => {
                                (value == "true" && b) || (value == "false" && !b)
                            }
                            Ok(Some(Value::Array(a))) => {
                                a.into_iter().any(|s| s.as_str() == Some(&value))
                            }
                            _ => false,
                        }
                    });
                }
                (_, _) => (),
            }
        }

        query_builder.with_fetch_timeout(self.timeout);

        if let Some(field) = self.index.main.distinct_attribute(reader)? {
            if let Some(field_id) = schema.id(&field) {
                query_builder.with_distinct(1, move |id| {
                    match self.index.document_attribute_bytes(reader, id, field_id) {
                        Ok(Some(bytes)) => {
                            let mut s = SipHasher::new();
                            bytes.hash(&mut s);
                            Some(s.finish())
                        }
                        _ => None,
                    }
                });
            }
        }

        let start = Instant::now();
        let result = query_builder.query(reader, &self.query, self.offset..(self.offset + self.limit));
        let time_ms = start.elapsed().as_millis() as usize;
        let (docs, nb_hits) = result.map_err(|e| Error::SearchDocuments(e.to_string()))?;

        let mut hits = Vec::with_capacity(self.limit);
        for doc in docs {
            // retrieve the content of document in kv store
            let mut fields: Option<HashSet<&str>> = None;
            if let Some(attributes_to_retrieve) = &self.attributes_to_retrieve {
                let mut set = HashSet::new();
                for field in attributes_to_retrieve {
                    set.insert(field.as_str());
                }
                fields = Some(set);
            }

            let document: IndexMap<String, Value> = self
                .index
                .document(reader, fields.as_ref(), doc.id)
                .map_err(|e| Error::RetrieveDocument(doc.id.0, e.to_string()))?
                .ok_or(Error::DocumentNotFound(doc.id.0))?;

            let has_attributes_to_highlight = self.attributes_to_highlight.is_some();
            let has_attributes_to_crop = self.attributes_to_crop.is_some();

            let mut formatted = if has_attributes_to_highlight || has_attributes_to_crop {
                document.clone()
            } else {
                IndexMap::new()
            };
            let mut matches = doc.highlights.clone();

            // Crops fields if needed
            if let Some(fields) = &self.attributes_to_crop {
                crop_document(&mut formatted, &mut matches, &schema, fields);
            }

            // Transform to readable matches
            let matches = calculate_matches(matches, self.attributes_to_retrieve.clone(), &schema);

            if let Some(attributes_to_highlight) = &self.attributes_to_highlight {
                formatted = calculate_highlights(&formatted, &matches, attributes_to_highlight);
            }

            let matches_info = if self.matches { Some(matches) } else { None };

            let hit = SearchHit {
                document,
                formatted,
                matches_info,
            };

            hits.push(hit);
        }

        let results = SearchResult {
            hits,
            offset: self.offset,
            limit: self.limit,
            nb_hits,
            exhaustive_nb_hits: false,
            processing_time_ms: time_ms,
            query: self.query.to_string(),
        };

        Ok(results)
    }

    pub fn get_criteria(
        &self,
        reader: &heed::RoTxn<MainT>,
        ranked_map: &'a RankedMap,
        schema: &Schema,
    ) -> Result<Option<Criteria<'a>>, Error> {
        let ranking_rules = self.index.main.ranking_rules(reader)?;

        if let Some(ranking_rules) = ranking_rules {
            let mut builder = CriteriaBuilder::with_capacity(7 + ranking_rules.len());
            for rule in ranking_rules {
                match rule {
                    RankingRule::Typo => builder.push(Typo),
                    RankingRule::Words => builder.push(Words),
                    RankingRule::Proximity => builder.push(Proximity),
                    RankingRule::Attribute => builder.push(Attribute),
                    RankingRule::WordsPosition => builder.push(WordsPosition),
                    RankingRule::Exactness => builder.push(Exactness),
                    RankingRule::Asc(field) => {
                        match SortByAttr::lower_is_better(&ranked_map, &schema, &field) {
                            Ok(rule) => builder.push(rule),
                            Err(err) => error!("Error during criteria builder; {:?}", err),
                        }
                    }
                    RankingRule::Desc(field) => {
                        match SortByAttr::higher_is_better(&ranked_map, &schema, &field) {
                            Ok(rule) => builder.push(rule),
                            Err(err) => error!("Error during criteria builder; {:?}", err),
                        }
                    }
                }
            }
            builder.push(DocumentId);
            return Ok(Some(builder.build()));
        }

        Ok(None)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MatchPosition {
    pub start: usize,
    pub length: usize,
}

impl Ord for MatchPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.start.cmp(&other.start) {
            Ordering::Equal => self.length.cmp(&other.length),
            _ => self.start.cmp(&other.start),
        }
    }
}

pub type HighlightInfos = HashMap<String, Value>;
pub type MatchesInfos = HashMap<String, Vec<MatchPosition>>;
// pub type RankingInfos = HashMap<String, u64>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    #[serde(flatten)]
    pub document: IndexMap<String, Value>,
    #[serde(rename = "_formatted", skip_serializing_if = "IndexMap::is_empty")]
    pub formatted: IndexMap<String, Value>,
    #[serde(rename = "_matchesInfo", skip_serializing_if = "Option::is_none")]
    pub matches_info: Option<MatchesInfos>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub offset: usize,
    pub limit: usize,
    pub nb_hits: usize,
    pub exhaustive_nb_hits: bool,
    pub processing_time_ms: usize,
    pub query: String,
}

fn crop_text(
    text: &str,
    matches: impl IntoIterator<Item = Highlight>,
    context: usize,
) -> (String, Vec<Highlight>) {
    let mut matches = matches.into_iter().peekable();

    let char_index = matches.peek().map(|m| m.char_index as usize).unwrap_or(0);
    let start = char_index.saturating_sub(context);
    let text = text.chars().skip(start).take(context * 2).collect();

    let matches = matches
        .take_while(|m| (m.char_index as usize) + (m.char_length as usize) <= start + (context * 2))
        .map(|match_| Highlight {
            char_index: match_.char_index - start as u16,
            ..match_
        })
        .collect();

    (text, matches)
}

fn crop_document(
    document: &mut IndexMap<String, Value>,
    matches: &mut Vec<Highlight>,
    schema: &Schema,
    fields: &HashMap<String, usize>,
) {
    matches.sort_unstable_by_key(|m| (m.char_index, m.char_length));

    for (field, length) in fields {
        let attribute = match schema.id(field) {
            Some(attribute) => attribute,
            None => continue,
        };

        let selected_matches = matches
            .iter()
            .filter(|m| FieldId::new(m.attribute) == attribute)
            .cloned();

        if let Some(Value::String(ref mut original_text)) = document.get_mut(field) {
            let (cropped_text, cropped_matches) =
                crop_text(original_text, selected_matches, *length);

            *original_text = cropped_text;

            matches.retain(|m| FieldId::new(m.attribute) != attribute);
            matches.extend_from_slice(&cropped_matches);
        }
    }
}

fn calculate_matches(
    matches: Vec<Highlight>,
    attributes_to_retrieve: Option<HashSet<String>>,
    schema: &Schema,
) -> MatchesInfos {
    let mut matches_result: HashMap<String, Vec<MatchPosition>> = HashMap::new();
    for m in matches.iter() {
        if let Some(attribute) = schema.name(FieldId::new(m.attribute)) {
            if let Some(attributes_to_retrieve) = attributes_to_retrieve.clone() {
                if !attributes_to_retrieve.contains(attribute) {
                    continue;
                }
            }
            if !schema.displayed_name().contains(attribute) {
                continue;
            }
            if let Some(pos) = matches_result.get_mut(attribute) {
                pos.push(MatchPosition {
                    start: m.char_index as usize,
                    length: m.char_length as usize,
                });
            } else {
                let mut positions = Vec::new();
                positions.push(MatchPosition {
                    start: m.char_index as usize,
                    length: m.char_length as usize,
                });
                matches_result.insert(attribute.to_string(), positions);
            }
        }
    }
    for (_, val) in matches_result.iter_mut() {
        val.sort_unstable();
        val.dedup();
    }
    matches_result
}

fn calculate_highlights(
    document: &IndexMap<String, Value>,
    matches: &MatchesInfos,
    attributes_to_highlight: &HashSet<String>,
) -> IndexMap<String, Value> {
    let mut highlight_result = document.clone();

    for (attribute, matches) in matches.iter() {
        if attributes_to_highlight.contains(attribute) {
            if let Some(Value::String(value)) = document.get(attribute) {
                let value: Vec<_> = value.chars().collect();
                let mut highlighted_value = String::new();
                let mut index = 0;
                for m in matches {
                    if m.start >= index {
                        let before = value.get(index..m.start);
                        let highlighted = value.get(m.start..(m.start + m.length));
                        if let (Some(before), Some(highlighted)) = (before, highlighted) {
                            highlighted_value.extend(before);
                            highlighted_value.push_str("<em>");
                            highlighted_value.extend(highlighted);
                            highlighted_value.push_str("</em>");
                            index = m.start + m.length;
                        } else {
                            error!("value: {:?}; index: {:?}, match: {:?}", value, index, m);
                        }
                    }
                }
                highlighted_value.extend(value[index..].iter());
                highlight_result.insert(attribute.to_string(), Value::String(highlighted_value));
            };
        }
    }

    highlight_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculate_highlights() {
        let data = r#"{
            "title": "Fondation (Isaac ASIMOV)",
            "description": "En ce début de trentième millénaire, l'Empire n'a jamais été aussi puissant, aussi étendu à travers toute la galaxie. C'est dans sa capitale, Trantor, que l'éminent savant Hari Seldon invente la psychohistoire, une science toute nouvelle, à base de psychologie et de mathématiques, qui lui permet de prédire l'avenir... C'est-à-dire l'effondrement de l'Empire d'ici cinq siècles et au-delà, trente mille années de chaos et de ténèbres. Pour empêcher cette catastrophe et sauver la civilisation, Seldon crée la Fondation."
        }"#;

        let document: IndexMap<String, Value> = serde_json::from_str(data).unwrap();
        let mut attributes_to_highlight = HashSet::new();
        attributes_to_highlight.insert("title".to_string());
        attributes_to_highlight.insert("description".to_string());

        let mut matches = HashMap::new();

        let mut m = Vec::new();
        m.push(MatchPosition {
            start: 0,
            length: 9,
        });
        matches.insert("title".to_string(), m);

        let mut m = Vec::new();
        m.push(MatchPosition {
            start: 510,
            length: 9,
        });
        matches.insert("description".to_string(), m);
        let result = super::calculate_highlights(&document, &matches, &attributes_to_highlight);

        let mut result_expected = IndexMap::new();
        result_expected.insert(
            "title".to_string(),
            Value::String("<em>Fondation</em> (Isaac ASIMOV)".to_string()),
        );
        result_expected.insert("description".to_string(), Value::String("En ce début de trentième millénaire, l'Empire n'a jamais été aussi puissant, aussi étendu à travers toute la galaxie. C'est dans sa capitale, Trantor, que l'éminent savant Hari Seldon invente la psychohistoire, une science toute nouvelle, à base de psychologie et de mathématiques, qui lui permet de prédire l'avenir... C'est-à-dire l'effondrement de l'Empire d'ici cinq siècles et au-delà, trente mille années de chaos et de ténèbres. Pour empêcher cette catastrophe et sauver la civilisation, Seldon crée la <em>Fondation</em>.".to_string()));

        assert_eq!(result, result_expected);
    }
}
