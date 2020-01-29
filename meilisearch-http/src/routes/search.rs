use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use meilisearch_core::Index;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tide::{Request, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::meilisearch::{Error, IndexSearchExt, SearchHit};
use crate::helpers::tide::RequestExt;
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

pub async fn search_with_url_query(ctx: Request<Data>) -> SResult<Response> {
    // ctx.is_allowed(DocumentsRead)?;

    let index = ctx.index()?;
    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let schema = index
        .main
        .schema(&reader)?
        .ok_or(ResponseError::open_index("No Schema found"))?;

    let query: SearchQuery = ctx
        .query()
        .map_err(|_| ResponseError::bad_request("invalid query parameter"))?;

    let mut search_builder = index.new_search(query.q.clone());

    if let Some(offset) = query.offset {
        search_builder.offset(offset);
    }
    if let Some(limit) = query.limit {
        search_builder.limit(limit);
    }

    if let Some(attributes_to_retrieve) = query.attributes_to_retrieve {
        for attr in attributes_to_retrieve.split(',') {
            search_builder.add_retrievable_field(attr.to_string());
        }
    }

    if let Some(attributes_to_crop) = query.attributes_to_crop {
        let crop_length = query.crop_length.unwrap_or(200);
        if attributes_to_crop == "*" {
            let attributes_to_crop = schema
                .displayed_name()
                .iter()
                .map(|attr| (attr.to_string(), crop_length))
                .collect();
            search_builder.attributes_to_crop(attributes_to_crop);
        } else {
            let attributes_to_crop = attributes_to_crop
                .split(',')
                .map(|r| (r.to_string(), crop_length))
                .collect();
            search_builder.attributes_to_crop(attributes_to_crop);
        }
    }

    if let Some(attributes_to_highlight) = query.attributes_to_highlight {
        let attributes_to_highlight = if attributes_to_highlight == "*" {
            schema
                .displayed_name()
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            attributes_to_highlight
                .split(',')
                .map(|s| s.to_string())
                .collect()
        };

        search_builder.attributes_to_highlight(attributes_to_highlight);
    }

    if let Some(filters) = query.filters {
        search_builder.filters(filters);
    }

    if let Some(timeout_ms) = query.timeout_ms {
        search_builder.timeout(Duration::from_millis(timeout_ms));
    }

    if let Some(matches) = query.matches {
        if matches {
            search_builder.get_matches();
        }
    }

    let response = match search_builder.search(&reader) {
        Ok(response) => response,
        Err(Error::Internal(message)) => return Err(ResponseError::Internal(message)),
        Err(others) => return Err(ResponseError::bad_request(others)),
    };

    Ok(tide::Response::new(200).body_json(&response).unwrap())
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

pub async fn search_multi_index(mut ctx: Request<Data>) -> SResult<Response> {
    // ctx.is_allowed(DocumentsRead)?;
    let body = ctx
        .body_json::<SearchMultiBody>()
        .await
        .map_err(ResponseError::bad_request)?;

    let mut index_list = body.clone().indexes;

    for index in index_list.clone() {
        if index == "*" {
            index_list = ctx.state().db.indexes_uids().into_iter().collect();
            break;
        }
    }

    let mut offset = 0;
    let mut count = 20;

    if let Some(body_offset) = body.offset {
        if let Some(limit) = body.limit {
            offset = body_offset;
            count = limit;
        }
    }

    let offset = offset;
    let count = count;
    let db = &ctx.state().db;
    let par_body = body.clone();
    let responses_per_index: Vec<SResult<_>> = index_list
        .into_par_iter()
        .map(move |index_uid| {
            let index: Index = db
                .open_index(&index_uid)
                .ok_or(ResponseError::index_not_found(&index_uid))?;

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

            let reader = db.main_read_txn()?;
            let response = search_builder.search(&reader)?;
            Ok((index_uid, response))
        })
        .collect();

    let mut hits_map = HashMap::new();

    let mut max_query_time = 0;

    for response in responses_per_index {
        if let Ok((index_uid, response)) = response {
            if response.processing_time_ms > max_query_time {
                max_query_time = response.processing_time_ms;
            }
            hits_map.insert(index_uid, response.hits);
        }
    }

    let response = SearchMultiBodyResponse {
        hits: hits_map,
        offset,
        hits_per_page: count,
        processing_time_ms: max_query_time,
        query: body.query,
    };

    Ok(tide::Response::new(200).body_json(&response).unwrap())
}
