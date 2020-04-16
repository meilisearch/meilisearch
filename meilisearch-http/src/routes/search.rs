use std::collections::{HashSet, HashMap};
use std::time::Duration;

use log::warn;
use meilisearch_core::Index;
use actix_web as aweb;
use actix_web::{get, web};
use serde::{Deserialize};

use crate::error::ResponseError;
use crate::helpers::meilisearch::{Error, IndexSearchExt, SearchResult};
use crate::routes::IndexParam;
use crate::Data;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
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
    path: web::Path<IndexParam>,
    params: web::Query<SearchQuery>,
) -> aweb::Result<web::Json<SearchResult>> {
    let index = data
        .db
        .open_index(path.index_uid.clone())
        .ok_or(ResponseError::IndexNotFound(path.index_uid.clone()))?;

    let reader = data
        .db
        .main_read_txn()
        .map_err(|err| ResponseError::Internal(err.to_string()))?;

    let schema = index
        .main
        .schema(&reader)
        .map_err(|err| ResponseError::Internal(err.to_string()))?
        .ok_or(ResponseError::Internal(
            "Impossible to retrieve the schema".to_string(),
        ))?;

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
        Err(Error::Internal(message)) => return Err(ResponseError::Internal(message).into()),
        Err(others) => return Err(ResponseError::BadRequest(others.to_string()).into()),
    };

    Ok(web::Json(response))
}
