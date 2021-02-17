use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};

use actix_web::{get, post, web, HttpResponse};
use serde::Deserialize;

use crate::data::{SearchQuery, DEFAULT_SEARCH_LIMIT};
use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(search_with_post).service(search_with_url_query);
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQueryGet {
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
    facet_distributions: Option<String>,
}

impl TryFrom<SearchQueryGet> for SearchQuery {
    type Error = anyhow::Error;

    fn try_from(other: SearchQueryGet) -> anyhow::Result<Self> {
        let attributes_to_retrieve = other
            .attributes_to_retrieve
            .map(|attrs| attrs.split(",").map(String::from).collect::<Vec<_>>());

        let attributes_to_crop = other
            .attributes_to_crop
            .map(|attrs| attrs.split(",").map(String::from).collect::<Vec<_>>());

        let attributes_to_highlight = other
            .attributes_to_highlight
            .map(|attrs| attrs.split(",").map(String::from).collect::<HashSet<_>>());

        let facet_distributions = other
            .facet_distributions
            .map(|attrs| attrs.split(",").map(String::from).collect::<Vec<_>>());

        let facet_filters = match other.facet_filters {
            Some(ref f) => Some(serde_json::from_str(f)?),
            None => None,
        };

        Ok(Self {
            q: other.q,
            offset: other.offset,
            limit: other.limit.unwrap_or(DEFAULT_SEARCH_LIMIT),
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length: other.crop_length,
            attributes_to_highlight,
            filters: other.filters,
            matches: other.matches,
            facet_filters,
            facet_distributions,
        })
    }
}

#[get("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_url_query(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<SearchQueryGet>,
) -> Result<HttpResponse, ResponseError> {
    let query: SearchQuery = match params.into_inner().try_into() {
        Ok(q) => q,
        Err(e) => {
            return Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    };
    let search_result = data.search(&path.index_uid, query);
    match search_result {
        Ok(docs) => {
            let docs = serde_json::to_string(&docs).unwrap();
            Ok(HttpResponse::Ok().body(docs))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[post("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_post(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Json<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    let search_result = data.search(&path.index_uid, params.into_inner());
    match search_result {
        Ok(docs) => {
            let docs = serde_json::to_string(&docs).unwrap();
            Ok(HttpResponse::Ok().body(docs))
        }
        Err(e) => {
            Ok(HttpResponse::BadRequest().body(serde_json::json!({ "error": e.to_string() })))
        }
    }
}
