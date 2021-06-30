use std::collections::{BTreeSet, HashSet};

use actix_web::{web, HttpResponse};
use log::debug;
use serde::Deserialize;
use serde_json::Value;

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::index::{default_crop_length, SearchQuery, DEFAULT_SEARCH_LIMIT};
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/indexes/{index_uid}/search")
            .route(web::get().to(search_with_url_query))
            .route(web::post().to(search_with_post)),
    );
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQueryGet {
    q: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
    attributes_to_crop: Option<String>,
    #[serde(default = "default_crop_length")]
    crop_length: usize,
    attributes_to_highlight: Option<String>,
    filter: Option<String>,
    #[serde(default = "Default::default")]
    matches: bool,
    facets_distribution: Option<String>,
}

impl From<SearchQueryGet> for SearchQuery {
    fn from(other: SearchQueryGet) -> Self {
        let attributes_to_retrieve = other
            .attributes_to_retrieve
            .map(|attrs| attrs.split(',').map(String::from).collect::<BTreeSet<_>>());

        let attributes_to_crop = other
            .attributes_to_crop
            .map(|attrs| attrs.split(',').map(String::from).collect::<Vec<_>>());

        let attributes_to_highlight = other
            .attributes_to_highlight
            .map(|attrs| attrs.split(',').map(String::from).collect::<HashSet<_>>());

        let facets_distribution = other
            .facets_distribution
            .map(|attrs| attrs.split(',').map(String::from).collect::<Vec<_>>());

        let filter = match other.filter {
            Some(f) => match serde_json::from_str(&f) {
                Ok(v) => Some(v),
                _ => Some(Value::String(f)),
            },
            None => None,
        };

        Self {
            q: other.q,
            offset: other.offset,
            limit: other.limit.unwrap_or(DEFAULT_SEARCH_LIMIT),
            attributes_to_retrieve,
            attributes_to_crop,
            crop_length: other.crop_length,
            attributes_to_highlight,
            filter,
            matches: other.matches,
            facets_distribution,
        }
    }
}

async fn search_with_url_query(
    data: GuardedData<Admin, Data>,
    path: web::Path<IndexParam>,
    params: web::Query<SearchQueryGet>,
) -> Result<HttpResponse, ResponseError> {
    debug!("called with params: {:?}", params);
    let query = params.into_inner().into();
    let search_result = data.search(path.into_inner().index_uid, query).await?;
    debug!("returns: {:?}", search_result);
    Ok(HttpResponse::Ok().json(search_result))
}

async fn search_with_post(
    data: GuardedData<Public, Data>,
    path: web::Path<IndexParam>,
    params: web::Json<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    debug!("search called with params: {:?}", params);
    let search_result = data
        .search(path.into_inner().index_uid, params.into_inner())
        .await?;
    debug!("returns: {:?}", search_result);
    Ok(HttpResponse::Ok().json(search_result))
}
