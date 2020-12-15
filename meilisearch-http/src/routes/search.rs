use std::collections::{HashMap, HashSet, BTreeSet};

use actix_web::{get, post, web, HttpResponse};
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, FacetCountError, ResponseError};
use crate::helpers::meilisearch::{IndexSearchExt, SearchResult};
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

use meilisearch_core::facets::FacetFilter;
use meilisearch_schema::{FieldId, Schema};

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(search_with_post).service(search_with_url_query);
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    q: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
    attributes_to_crop: Option<String>,
    crop_length: Option<usize>,
    attributes_to_highlight: Option<String>,
    filters: Option<String>,
    matches: Option<bool>,
    facet_filters: Option<String>,
    facets_distribution: Option<String>,
}

#[get("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_url_query(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    let search_result = params.search(&path.index_uid, data)?;
    Ok(HttpResponse::Ok().json(search_result))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQueryPost {
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

impl From<SearchQueryPost> for SearchQuery {
    fn from(other: SearchQueryPost) -> SearchQuery {
        SearchQuery {
            q: other.q,
            offset: other.offset,
            limit: other.limit,
            attributes_to_retrieve: other.attributes_to_retrieve.map(|attrs| attrs.join(",")),
            attributes_to_crop: other.attributes_to_crop.map(|attrs| attrs.join(",")),
            crop_length: other.crop_length,
            attributes_to_highlight: other.attributes_to_highlight.map(|attrs| attrs.join(",")),
            filters: other.filters,
            matches: other.matches,
            facet_filters: other.facet_filters.map(|f| f.to_string()),
            facets_distribution: other.facets_distribution.map(|f| format!("{:?}", f)),
        }
    }
}

#[post("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_post(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Json<SearchQueryPost>,
) -> Result<HttpResponse, ResponseError> {
    let query: SearchQuery = params.0.into();
    let search_result = query.search(&path.index_uid, data)?;
    Ok(HttpResponse::Ok().json(search_result))
}

impl SearchQuery {
    fn search(
        &self,
        index_uid: &str,
        data: web::Data<Data>,
    ) -> Result<SearchResult, ResponseError> {
        let index = data
            .db
            .open_index(index_uid)
            .ok_or(Error::index_not_found(index_uid))?;

        let reader = data.db.main_read_txn()?;
        let schema = index
            .main
            .schema(&reader)?
            .ok_or(Error::internal("Impossible to retrieve the schema"))?;

        let query = self
            .q
            .clone()
            .and_then(|q| if q.is_empty() { None } else { Some(q) });

        let mut search_builder = index.new_search(query);

        if let Some(offset) = self.offset {
            search_builder.offset(offset);
        }
        if let Some(limit) = self.limit {
            search_builder.limit(limit);
        }

        let available_attributes = schema.displayed_names();
        let mut restricted_attributes: BTreeSet<&str>;
        match &self.attributes_to_retrieve {
            Some(attributes_to_retrieve) => {
                let attributes_to_retrieve: HashSet<&str> =
                    attributes_to_retrieve.split(',').collect();
                if attributes_to_retrieve.contains("*") {
                    restricted_attributes = available_attributes.clone();
                } else {
                    restricted_attributes = BTreeSet::new();
                    search_builder.attributes_to_retrieve(HashSet::new());
                    for attr in attributes_to_retrieve {
                        if available_attributes.contains(attr) {
                            restricted_attributes.insert(attr);
                            search_builder.add_retrievable_field(attr.to_string());
                        } else {
                            warn!("The attributes {:?} present in attributesToRetrieve parameter doesn't exist", attr);
                        }
                    }
                }
            }
            None => {
                restricted_attributes = available_attributes.clone();
            }
        }

        if let Some(ref facet_filters) = self.facet_filters {
            let attrs = index
                .main
                .attributes_for_faceting(&reader)?
                .unwrap_or_default();
            search_builder.add_facet_filters(FacetFilter::from_str(
                facet_filters,
                &schema,
                &attrs,
            )?);
        }

        if let Some(facets) = &self.facets_distribution {
            match index.main.attributes_for_faceting(&reader)? {
                Some(ref attrs) => {
                    let field_ids = prepare_facet_list(&facets, &schema, attrs)?;
                    search_builder.add_facets(field_ids);
                }
                None => return Err(FacetCountError::NoFacetSet.into()),
            }
        }

        if let Some(attributes_to_crop) = &self.attributes_to_crop {
            let default_length = self.crop_length.unwrap_or(200);
            let mut final_attributes: HashMap<String, usize> = HashMap::new();

            for attribute in attributes_to_crop.split(',') {
                let mut attribute = attribute.split(':');
                let attr = attribute.next();
                let length = attribute
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(default_length);
                match attr {
                    Some("*") => {
                        for attr in &restricted_attributes {
                            final_attributes.insert(attr.to_string(), length);
                        }
                    }
                    Some(attr) => {
                        if available_attributes.contains(attr) {
                            final_attributes.insert(attr.to_string(), length);
                        } else {
                            warn!("The attributes {:?} present in attributesToCrop parameter doesn't exist", attr);
                        }
                    }
                    None => (),
                }
            }
            search_builder.attributes_to_crop(final_attributes);
        }

        if let Some(attributes_to_highlight) = &self.attributes_to_highlight {
            let mut final_attributes: HashSet<String> = HashSet::new();
            for attribute in attributes_to_highlight.split(',') {
                if attribute == "*" {
                    for attr in &restricted_attributes {
                        final_attributes.insert(attr.to_string());
                    }
                } else if available_attributes.contains(attribute) {
                    final_attributes.insert(attribute.to_string());
                } else {
                    warn!("The attributes {:?} present in attributesToHighlight parameter doesn't exist", attribute);
                }
            }

            search_builder.attributes_to_highlight(final_attributes);
        }

        if let Some(filters) = &self.filters {
            search_builder.filters(filters.to_string());
        }

        if let Some(matches) = self.matches {
            if matches {
                search_builder.get_matches();
            }
        }
        search_builder.search(&reader)
    }
}

/// Parses the incoming string into an array of attributes for which to return a count. It returns
/// a Vec of attribute names ascociated with their id.
///
/// An error is returned if the array is malformed, or if it contains attributes that are
/// unexisting, or not set as facets.
fn prepare_facet_list(
    facets: &str,
    schema: &Schema,
    facet_attrs: &[FieldId],
) -> Result<Vec<(FieldId, String)>, FacetCountError> {
    let json_array = serde_json::from_str(facets)?;
    match json_array {
        Value::Array(vals) => {
            let wildcard = Value::String("*".to_string());
            if vals.iter().any(|f| f == &wildcard) {
                let attrs = facet_attrs
                    .iter()
                    .filter_map(|&id| schema.name(id).map(|n| (id, n.to_string())))
                    .collect();
                return Ok(attrs);
            }
            let mut field_ids = Vec::with_capacity(facet_attrs.len());
            for facet in vals {
                match facet {
                    Value::String(facet) => {
                        if let Some(id) = schema.id(&facet) {
                            if !facet_attrs.contains(&id) {
                                return Err(FacetCountError::AttributeNotSet(facet));
                            }
                            field_ids.push((id, facet));
                        }
                    }
                    bad_val => return Err(FacetCountError::unexpected_token(bad_val, &["String"])),
                }
            }
            Ok(field_ids)
        }
        bad_val => Err(FacetCountError::unexpected_token(bad_val, &["[String]"])),
    }
}
