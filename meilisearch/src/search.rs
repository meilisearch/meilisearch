use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::time::Instant;

use deserr::DeserializeFromValue;
use either::Either;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::settings::DEFAULT_PAGINATION_MAX_TOTAL_HITS;
use meilisearch_types::{milli, Document};
use milli::tokenizer::TokenizerBuilder;
use milli::{
    AscDesc, FieldId, FieldsIdsMap, Filter, FormatOptions, Index, MatchBounds, MatcherBuilder,
    SortError, TermsMatchingStrategy, DEFAULT_VALUES_PER_FACET,
};
use regex::Regex;
use serde::Serialize;
use serde_json::{json, Value};

use crate::error::MeilisearchHttpError;

type MatchesPosition = BTreeMap<String, Vec<MatchBounds>>;

pub const DEFAULT_SEARCH_OFFSET: fn() -> usize = || 0;
pub const DEFAULT_SEARCH_LIMIT: fn() -> usize = || 20;
pub const DEFAULT_CROP_LENGTH: fn() -> usize = || 10;
pub const DEFAULT_CROP_MARKER: fn() -> String = || "…".to_string();
pub const DEFAULT_HIGHLIGHT_PRE_TAG: fn() -> String = || "<em>".to_string();
pub const DEFAULT_HIGHLIGHT_POST_TAG: fn() -> String = || "</em>".to_string();

#[derive(Debug, Clone, Default, PartialEq, Eq, DeserializeFromValue)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SearchQuery {
    #[deserr(default, error = DeserrJsonError<InvalidSearchQ>)]
    pub q: Option<String>,
    #[deserr(default = DEFAULT_SEARCH_OFFSET(), error = DeserrJsonError<InvalidSearchOffset>)]
    pub offset: usize,
    #[deserr(default = DEFAULT_SEARCH_LIMIT(), error = DeserrJsonError<InvalidSearchLimit>)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchPage>)]
    pub page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHitsPerPage>)]
    pub hits_per_page: Option<usize>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToRetrieve>)]
    pub attributes_to_retrieve: Option<BTreeSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToCrop>)]
    pub attributes_to_crop: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropLength>, default = DEFAULT_CROP_LENGTH())]
    pub crop_length: usize,
    #[deserr(default, error = DeserrJsonError<InvalidSearchAttributesToHighlight>)]
    pub attributes_to_highlight: Option<HashSet<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchShowMatchesPosition>, default)]
    pub show_matches_position: bool,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFilter>)]
    pub filter: Option<Value>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchSort>)]
    pub sort: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchFacets>)]
    pub facets: Option<Vec<String>>,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPreTag>, default = DEFAULT_HIGHLIGHT_PRE_TAG())]
    pub highlight_pre_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchHighlightPostTag>, default = DEFAULT_HIGHLIGHT_POST_TAG())]
    pub highlight_post_tag: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchCropMarker>, default = DEFAULT_CROP_MARKER())]
    pub crop_marker: String,
    #[deserr(default, error = DeserrJsonError<InvalidSearchMatchingStrategy>, default)]
    pub matching_strategy: MatchingStrategy,
}

impl SearchQuery {
    pub fn is_finite_pagination(&self) -> bool {
        self.page.or(self.hits_per_page).is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, DeserializeFromValue)]
#[deserr(rename_all = camelCase)]
pub enum MatchingStrategy {
    /// Remove query words from last to first
    Last,
    /// All query words are mandatory
    All,
}

impl Default for MatchingStrategy {
    fn default() -> Self {
        Self::Last
    }
}

impl From<MatchingStrategy> for TermsMatchingStrategy {
    fn from(other: MatchingStrategy) -> Self {
        match other {
            MatchingStrategy::Last => Self::Last,
            MatchingStrategy::All => Self::All,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SearchHit {
    #[serde(flatten)]
    pub document: Document,
    #[serde(rename = "_formatted", skip_serializing_if = "Document::is_empty")]
    pub formatted: Document,
    #[serde(rename = "_matchesPosition", skip_serializing_if = "Option::is_none")]
    pub matches_position: Option<MatchesPosition>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub query: String,
    pub processing_time_ms: u128,
    #[serde(flatten)]
    pub hits_info: HitsInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_distribution: Option<BTreeMap<String, BTreeMap<String, u64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet_stats: Option<BTreeMap<String, FacetStats>>,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum HitsInfo {
    #[serde(rename_all = "camelCase")]
    Pagination { hits_per_page: usize, page: usize, total_pages: usize, total_hits: usize },
    #[serde(rename_all = "camelCase")]
    OffsetLimit { limit: usize, offset: usize, estimated_total_hits: usize },
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct FacetStats {
    pub min: f64,
    pub max: f64,
}

pub fn perform_search(
    index: &Index,
    query: SearchQuery,
) -> Result<SearchResult, MeilisearchHttpError> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;

    let mut search = index.search(&rtxn);

    if let Some(ref query) = query.q {
        search.query(query);
    }

    let is_finite_pagination = query.is_finite_pagination();
    search.terms_matching_strategy(query.matching_strategy.into());

    let max_total_hits = index
        .pagination_max_total_hits(&rtxn)
        .map_err(milli::Error::from)?
        .unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS);

    search.exhaustive_number_hits(is_finite_pagination);

    // compute the offset on the limit depending on the pagination mode.
    let (offset, limit) = if is_finite_pagination {
        let limit = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        let page = query.page.unwrap_or(1);

        // page 0 gives a limit of 0 forcing Meilisearch to return no document.
        page.checked_sub(1).map_or((0, 0), |p| (limit * p, limit))
    } else {
        (query.offset, query.limit)
    };

    // Make sure that a user can't get more documents than the hard limit,
    // we align that on the offset too.
    let offset = min(offset, max_total_hits);
    let limit = min(limit, max_total_hits.saturating_sub(offset));

    search.offset(offset);
    search.limit(limit);

    if let Some(ref filter) = query.filter {
        if let Some(facets) = parse_filter(filter)? {
            search.filter(facets);
        }
    }

    if let Some(ref sort) = query.sort {
        let sort = match sort.iter().map(|s| AscDesc::from_str(s)).collect() {
            Ok(sorts) => sorts,
            Err(asc_desc_error) => {
                return Err(milli::Error::from(SortError::from(asc_desc_error)).into())
            }
        };

        search.sort_criteria(sort);
    }

    let milli::SearchResult { documents_ids, matching_words, candidates, .. } = search.execute()?;

    let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();

    let displayed_ids = index
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

    let tokenizer = TokenizerBuilder::default().build();

    let mut formatter_builder = MatcherBuilder::new(matching_words, tokenizer);
    formatter_builder.crop_marker(query.crop_marker);
    formatter_builder.highlight_prefix(query.highlight_pre_tag);
    formatter_builder.highlight_suffix(query.highlight_post_tag);

    let mut documents = Vec::new();

    let documents_iter = index.documents(&rtxn, documents_ids)?;

    for (_id, obkv) in documents_iter {
        // First generate a document with all the displayed fields
        let displayed_document = make_document(&displayed_ids, &fields_ids_map, obkv)?;

        // select the attributes to retrieve
        let attributes_to_retrieve = to_retrieve_ids
            .iter()
            .map(|&fid| fields_ids_map.name(fid).expect("Missing field name"));
        let mut document =
            permissive_json_pointer::select_values(&displayed_document, attributes_to_retrieve);

        let (matches_position, formatted) = format_fields(
            &displayed_document,
            &fields_ids_map,
            &formatter_builder,
            &formatted_options,
            query.show_matches_position,
            &displayed_ids,
        )?;

        if let Some(sort) = query.sort.as_ref() {
            insert_geo_distance(sort, &mut document);
        }

        let hit = SearchHit { document, formatted, matches_position };
        documents.push(hit);
    }

    let number_of_hits = min(candidates.len() as usize, max_total_hits);
    let hits_info = if is_finite_pagination {
        let hits_per_page = query.hits_per_page.unwrap_or_else(DEFAULT_SEARCH_LIMIT);
        // If hit_per_page is 0, then pages can't be computed and so we respond 0.
        let total_pages = (number_of_hits + hits_per_page.saturating_sub(1))
            .checked_div(hits_per_page)
            .unwrap_or(0);

        HitsInfo::Pagination {
            hits_per_page,
            page: query.page.unwrap_or(1),
            total_pages,
            total_hits: number_of_hits,
        }
    } else {
        HitsInfo::OffsetLimit { limit: query.limit, offset, estimated_total_hits: number_of_hits }
    };

    let (facet_distribution, facet_stats) = match query.facets {
        Some(ref fields) => {
            let mut facet_distribution = index.facets_distribution(&rtxn);

            let max_values_by_facet = index
                .max_values_per_facet(&rtxn)
                .map_err(milli::Error::from)?
                .unwrap_or(DEFAULT_VALUES_PER_FACET);
            facet_distribution.max_values_per_facet(max_values_by_facet);

            if fields.iter().all(|f| f != "*") {
                facet_distribution.facets(fields);
            }
            let distribution = facet_distribution.candidates(candidates).execute()?;
            let stats = facet_distribution.compute_stats()?;
            (Some(distribution), Some(stats))
        }
        None => (None, None),
    };

    let facet_stats = facet_stats.map(|stats| {
        stats.into_iter().map(|(k, (min, max))| (k, FacetStats { min, max })).collect()
    });

    let result = SearchResult {
        hits: documents,
        hits_info,
        query: query.q.clone().unwrap_or_default(),
        processing_time_ms: before_search.elapsed().as_millis(),
        facet_distribution,
        facet_stats,
    };
    Ok(result)
}

fn insert_geo_distance(sorts: &[String], document: &mut Document) {
    lazy_static::lazy_static! {
        static ref GEO_REGEX: Regex =
            Regex::new(r"_geoPoint\(\s*([[:digit:].\-]+)\s*,\s*([[:digit:].\-]+)\s*\)").unwrap();
    };
    if let Some(capture_group) = sorts.iter().find_map(|sort| GEO_REGEX.captures(sort)) {
        // TODO: TAMO: milli encountered an internal error, what do we want to do?
        let base = [capture_group[1].parse().unwrap(), capture_group[2].parse().unwrap()];
        let geo_point = &document.get("_geo").unwrap_or(&json!(null));
        if let Some((lat, lng)) = geo_point["lat"].as_f64().zip(geo_point["lng"].as_f64()) {
            let distance = milli::distance_between_two_points(&base, &[lat, lng]);
            document.insert("_geoDistance".to_string(), json!(distance.round() as usize));
        }
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
        let new_format = FormatOptions { highlight: true, crop: None };

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
                    .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
            }
        }

        if let Some(id) = fields_ids_map.id(attr_name) {
            if displayed_ids.contains(&id) {
                formatted_options
                    .entry(id)
                    .and_modify(|f| f.crop = Some(attr_len))
                    .or_insert(FormatOptions { highlight: false, crop: Some(attr_len) });
            }
        }
    }
}

fn add_non_formatted_ids_to_formatted_options(
    formatted_options: &mut BTreeMap<FieldId, FormatOptions>,
    to_retrieve_ids: &BTreeSet<FieldId>,
) {
    for id in to_retrieve_ids {
        formatted_options.entry(*id).or_insert(FormatOptions { highlight: false, crop: None });
    }
}

fn make_document(
    displayed_attributes: &BTreeSet<FieldId>,
    field_ids_map: &FieldsIdsMap,
    obkv: obkv::KvReaderU16,
) -> Result<Document, MeilisearchHttpError> {
    let mut document = serde_json::Map::new();

    // recreate the original json
    for (key, value) in obkv.iter() {
        let value = serde_json::from_slice(value)?;
        let key = field_ids_map.name(key).expect("Missing field name").to_string();

        document.insert(key, value);
    }

    // select the attributes to retrieve
    let displayed_attributes = displayed_attributes
        .iter()
        .map(|&fid| field_ids_map.name(fid).expect("Missing field name"));

    let document = permissive_json_pointer::select_values(&document, displayed_attributes);
    Ok(document)
}

fn format_fields<A: AsRef<[u8]>>(
    document: &Document,
    field_ids_map: &FieldsIdsMap,
    builder: &MatcherBuilder<'_, A>,
    formatted_options: &BTreeMap<FieldId, FormatOptions>,
    compute_matches: bool,
    displayable_ids: &BTreeSet<FieldId>,
) -> Result<(Option<MatchesPosition>, Document), MeilisearchHttpError> {
    let mut matches_position = compute_matches.then(BTreeMap::new);
    let mut document = document.clone();

    // select the attributes to retrieve
    let displayable_names =
        displayable_ids.iter().map(|&fid| field_ids_map.name(fid).expect("Missing field name"));
    permissive_json_pointer::map_leaf_values(&mut document, displayable_names, |key, value| {
        // To get the formatting option of each key we need to see all the rules that applies
        // to the value and merge them together. eg. If a user said he wanted to highlight `doggo`
        // and crop `doggo.name`. `doggo.name` needs to be highlighted + cropped while `doggo.age` is only
        // highlighted.
        let format = formatted_options
            .iter()
            .filter(|(field, _option)| {
                let name = field_ids_map.name(**field).unwrap();
                milli::is_faceted_by(name, key) || milli::is_faceted_by(key, name)
            })
            .map(|(_, option)| *option)
            .reduce(|acc, option| acc.merge(option));
        let mut infos = Vec::new();

        *value = format_value(std::mem::take(value), builder, format, &mut infos, compute_matches);

        if let Some(matches) = matches_position.as_mut() {
            if !infos.is_empty() {
                matches.insert(key.to_owned(), infos);
            }
        }
    });

    let selectors = formatted_options
        .keys()
        // This unwrap must be safe since we got the ids from the fields_ids_map just
        // before.
        .map(|&fid| field_ids_map.name(fid).unwrap());
    let document = permissive_json_pointer::select_values(&document, selectors);

    Ok((matches_position, document))
}

fn format_value<A: AsRef<[u8]>>(
    value: Value,
    builder: &MatcherBuilder<'_, A>,
    format_options: Option<FormatOptions>,
    infos: &mut Vec<MatchBounds>,
    compute_matches: bool,
) -> Value {
    match value {
        Value::String(old_string) => {
            let mut matcher = builder.build(&old_string);
            if compute_matches {
                let matches = matcher.matches();
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::String(old_string),
            }
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|v| {
                    format_value(
                        v,
                        builder,
                        format_options.map(|format_options| FormatOptions {
                            highlight: format_options.highlight,
                            crop: None,
                        }),
                        infos,
                        compute_matches,
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
                        format_value(
                            v,
                            builder,
                            format_options.map(|format_options| FormatOptions {
                                highlight: format_options.highlight,
                                crop: None,
                            }),
                            infos,
                            compute_matches,
                        ),
                    )
                })
                .collect(),
        ),
        Value::Number(number) => {
            let s = number.to_string();

            let mut matcher = builder.build(&s);
            if compute_matches {
                let matches = matcher.matches();
                infos.extend_from_slice(&matches[..]);
            }

            match format_options {
                Some(format_options) => {
                    let value = matcher.format(format_options);
                    Value::String(value.into_owned())
                }
                None => Value::Number(number),
            }
        }
        value => value,
    }
}

fn parse_filter(facets: &Value) -> Result<Option<Filter>, MeilisearchHttpError> {
    match facets {
        Value::String(expr) => {
            let condition = Filter::from_str(expr)?;
            Ok(condition)
        }
        Value::Array(arr) => parse_filter_array(arr),
        v => Err(MeilisearchHttpError::InvalidExpression(&["String", "Array"], v.clone())),
    }
}

fn parse_filter_array(arr: &[Value]) -> Result<Option<Filter>, MeilisearchHttpError> {
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
                            return Err(MeilisearchHttpError::InvalidExpression(
                                &["String"],
                                v.clone(),
                            ))
                        }
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => {
                return Err(MeilisearchHttpError::InvalidExpression(
                    &["String", "[String]"],
                    v.clone(),
                ))
            }
        }
    }

    Ok(Filter::from_array(ands)?)
}

#[cfg(test)]
mod test {
    use super::*;

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
