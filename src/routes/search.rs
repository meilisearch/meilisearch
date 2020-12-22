use actix_web::{get, post, web, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

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
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Query<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQueryPost {
    _q: Option<String>,
    _offset: Option<usize>,
    _limit: Option<usize>,
    _attributes_to_retrieve: Option<Vec<String>>,
    _attributes_to_crop: Option<Vec<String>>,
    _crop_length: Option<usize>,
    _attributes_to_highlight: Option<Vec<String>>,
    _filters: Option<String>,
    _matches: Option<bool>,
    _facet_filters: Option<Value>,
    _facets_distribution: Option<Vec<String>>,
}

#[post("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_post(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
    _params: web::Json<SearchQueryPost>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}
