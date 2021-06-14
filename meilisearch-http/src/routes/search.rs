use std::collections::{BTreeSet, HashSet};
use std::convert::{TryFrom, TryInto};

use actix_web::{get, post, web, HttpResponse};
use serde_json::Value;
use serde::Deserialize;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::index::{SearchQuery, DEFAULT_SEARCH_LIMIT};
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
    crop_length: usize,
    attributes_to_highlight: Option<String>,
    filter: Option<String>,
    matches: Option<bool>,
    facet_distributions: Option<String>,
}

impl TryFrom<SearchQueryGet> for SearchQuery {
    type Error = Box<dyn std::error::Error>;

    fn try_from(other: SearchQueryGet) -> Result<Self, Self::Error> {
        let attributes_to_retrieve = other
            .attributes_to_retrieve
            .map(|attrs| attrs.split(',').map(String::from).collect::<BTreeSet<_>>());

        let attributes_to_crop = other
            .attributes_to_crop
            .map(|attrs| attrs.split(',').map(String::from).collect::<Vec<_>>());

        let attributes_to_highlight = other
            .attributes_to_highlight
            .map(|attrs| attrs.split(',').map(String::from).collect::<HashSet<_>>());

        let facet_distributions = other
            .facet_distributions
            .map(|attrs| attrs.split(',').map(String::from).collect::<Vec<_>>());

        let filter = match other.filter {
            Some(f) => {
                match serde_json::from_str(&f) {
                    Ok(v) => Some(v),
                    _ => Some(Value::String(f)),
                }
            },
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
            filter,
            matches: other.matches,
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
            return Ok(
                HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() }))
            )
        }
    };
    let search_result = data.search(path.into_inner().index_uid, query).await;
    match search_result {
        Ok(docs) => Ok(HttpResponse::Ok().json(docs)),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}

#[post("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_post(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Json<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    let search_result = data
        .search(path.into_inner().index_uid, params.into_inner())
        .await;
    match search_result {
        Ok(docs) => Ok(HttpResponse::Ok().json(docs)),
        Err(e) => {
            Ok(HttpResponse::BadRequest().json(serde_json::json!({ "error": e.to_string() })))
        }
    }
}
