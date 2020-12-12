use std::collections::{HashMap, HashSet};

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
    todo!()
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
    todo!()
}
