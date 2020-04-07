use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use log::warn;
use meilisearch_core::Index;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use actix_web::*;

use crate::error::ResponseError;
use crate::helpers::meilisearch::{Error, IndexSearchExt, SearchHit, SearchResult};
// use crate::helpers::tide::RequestExt;
// use crate::helpers::tide::ACL::*;
use crate::Data;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SearchQuery {
    q: String,
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
    attributes_to_crop: Option<String>,
    crop_length: Option<usize>,
    attributes_to_highlight: Option<String>,
    filters: Option<String>,
    timeout_ms: Option<u64>,
    matches: Option<bool>,
}

#[get("/indexes/{index_uid}/search")]
pub async fn search_with_url_query(
    data: web::Data<Data>,
    path: web::Path<String>,
    params: web::Query<SearchQuery>,
) -> Result<web::Json<SearchResult>> {

    let index = data.db.open_index(path.clone())
        .ok_or(ResponseError::IndexNotFound(path.clone()))?;

    let reader = data.db.main_read_txn()
        .map_err(|_| ResponseError::CreateTransaction)?;

    let schema = index
        .main
        .schema(&reader)
        .map_err(|_| ResponseError::Schema)?
        .ok_or(ResponseError::Schema)?;

    let mut search_builder = index.new_search(params.q.clone());

    if let Some(offset) = params.offset {
        search_builder.offset(offset);
    }
    if let Some(limit) = params.limit {
        search_builder.limit(limit);
    }

    let available_attributes = schema.displayed_name();
    let mut restricted_attributes: HashSet<&str>;
    match &params.attributes_to_retrieve {
        Some(attributes_to_retrieve) => {
            let attributes_to_retrieve: HashSet<&str> = attributes_to_retrieve.split(',').collect();
            if attributes_to_retrieve.contains("*") {
                restricted_attributes = available_attributes.clone();
            } else {
                restricted_attributes = HashSet::new();
                for attr in attributes_to_retrieve {
                    if available_attributes.contains(attr) {
                        restricted_attributes.insert(attr);
                        search_builder.add_retrievable_field(attr.to_string());
                    } else {
                        warn!("The attributes {:?} present in attributesToCrop parameter doesn't exist", attr);
                    }
                }
            }
        },
        None => {
            restricted_attributes = available_attributes.clone();
        }
    }

    if let Some(attributes_to_crop) = &params.attributes_to_crop {
        let default_length = params.crop_length.unwrap_or(200);
        let mut final_attributes: HashMap<String, usize> = HashMap::new();

        for attribute in attributes_to_crop.split(',') {
            let mut attribute = attribute.split(':');
            let attr = attribute.next();
            let length = attribute.next().and_then(|s| s.parse().ok()).unwrap_or(default_length);
            match attr {
                Some("*") => {
                    for attr in &restricted_attributes {
                        final_attributes.insert(attr.to_string(), length);
                    }
                },
                Some(attr) => {
                    if available_attributes.contains(attr) {
                        final_attributes.insert(attr.to_string(), length);
                    } else {
                        warn!("The attributes {:?} present in attributesToCrop parameter doesn't exist", attr);
                    }
                },
                None => (),
            }
        }

        search_builder.attributes_to_crop(final_attributes);
    }

    if let Some(attributes_to_highlight) = &params.attributes_to_highlight {
        let mut final_attributes: HashSet<String> = HashSet::new();
        for attribute in attributes_to_highlight.split(',') {
            if attribute == "*" {
                for attr in &restricted_attributes {
                    final_attributes.insert(attr.to_string());
                }
            } else {
                if available_attributes.contains(attribute) {
                    final_attributes.insert(attribute.to_string());
                } else {
                    warn!("The attributes {:?} present in attributesToHighlight parameter doesn't exist", attribute);
                }
            }
        }

        search_builder.attributes_to_highlight(final_attributes);
    }

    if let Some(filters) = &params.filters {
        search_builder.filters(filters.to_string());
    }

    if let Some(timeout_ms) = params.timeout_ms {
        search_builder.timeout(Duration::from_millis(timeout_ms));
    }

    if let Some(matches) = params.matches {
        if matches {
            search_builder.get_matches();
        }
    }

    let response = match search_builder.search(&reader) {
        Ok(response) => response,
        Err(Error::Internal(message)) => return Err(ResponseError::Internal(message))?,
        Err(others) => return Err(ResponseError::BadRequest(others.to_string()))?,
    };

    Ok(web::Json(response))
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SearchMultiBody {
    indexes: HashSet<String>,
    query: String,
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<HashSet<String>>,
    searchable_attributes: Option<HashSet<String>>,
    attributes_to_crop: Option<HashMap<String, usize>>,
    attributes_to_highlight: Option<HashSet<String>>,
    filters: Option<String>,
    timeout_ms: Option<u64>,
    matches: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchMultiBodyResponse {
    hits: HashMap<String, Vec<SearchHit>>,
    offset: usize,
    hits_per_page: usize,
    processing_time_ms: usize,
    query: String,
}

#[post("/indexes/search")]
pub async fn search_multi_index(
    data: web::Data<Data>,
    body: web::Json<SearchMultiBody>,
) -> Result<web::Json<SearchMultiBodyResponse>> {

    let mut index_list = body.clone().indexes;

    for index in index_list.clone() {
        if index == "*" {
            index_list = data.db.indexes_uids().into_iter().collect();
            break;
        }
    }

    let mut offset = 0;
    let mut count = 20;
    let query = body.query.clone();

    if let Some(body_offset) = body.offset {
        if let Some(limit) = body.limit {
            offset = body_offset;
            count = limit;
        }
    }


    let par_body = body.clone();
    let responses_per_index: Vec<(String, SearchResult)> = index_list
        .into_par_iter()
        .map(move |index_uid| {
            let index = data.db.open_index(&index_uid).unwrap();

            let mut search_builder = index.new_search(par_body.query.clone());

            search_builder.offset(offset);
            search_builder.limit(count);

            if let Some(attributes_to_retrieve) = par_body.attributes_to_retrieve.clone() {
                search_builder.attributes_to_retrieve(attributes_to_retrieve);
            }
            if let Some(attributes_to_crop) = par_body.attributes_to_crop.clone() {
                search_builder.attributes_to_crop(attributes_to_crop);
            }
            if let Some(attributes_to_highlight) = par_body.attributes_to_highlight.clone() {
                search_builder.attributes_to_highlight(attributes_to_highlight);
            }
            if let Some(filters) = par_body.filters.clone() {
                search_builder.filters(filters);
            }
            if let Some(timeout_ms) = par_body.timeout_ms {
                search_builder.timeout(Duration::from_millis(timeout_ms));
            }
            if let Some(matches) = par_body.matches {
                if matches {
                    search_builder.get_matches();
                }
            }

            let reader = data.db.main_read_txn().unwrap();
            let response = search_builder.search(&reader).unwrap();

            (index_uid, response)
        })
        .collect();

    let mut hits_map = HashMap::new();

    let mut max_query_time = 0;

    for (index_uid, response) in responses_per_index {
        if response.processing_time_ms > max_query_time {
            max_query_time = response.processing_time_ms;
        }
        hits_map.insert(index_uid, response.hits);
    }

    let response = SearchMultiBodyResponse {
        hits: hits_map,
        offset,
        hits_per_page: count,
        processing_time_ms: max_query_time,
        query,
    };

    Ok(web::Json(response))
}
