use std::collections::{HashSet, HashMap};

use log::warn;
use actix_web::web;
use actix_web::HttpResponse;
use actix_web_macros::get;
use serde::Deserialize;
use serde_json::Value;

use crate::error::ResponseError;
use crate::helpers::meilisearch::IndexSearchExt;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

use meilisearch_core::facets::FacetFilter;
use meilisearch_schema::{Schema, FieldId};

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(search_with_url_query);
}

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
    matches: Option<bool>,
    facet_filters: Option<String>,
    facets: Option<String>
}

#[get("/indexes/{index_uid}/search", wrap = "Authentication::Public")]
async fn search_with_url_query(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    params: web::Query<SearchQuery>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(ResponseError::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;

    let schema = index
        .main
        .schema(&reader)?
        .ok_or(ResponseError::internal("Impossible to retrieve the schema"))?;

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

    if let Some(ref facet_filters) = params.facet_filters {
        match index.main.attributes_for_faceting(&reader)? {
            Some(ref attrs) => { search_builder.add_facet_filters(FacetFilter::from_str(facet_filters, &schema, attrs)?); },
            None => return Err(ResponseError::FacetExpression("can't filter on facets, as no facet is set".to_string()))
        }
    }

    if let Some(ref facets) = params.facets {
        match index.main.attributes_for_faceting(&reader)? {
            Some(ref attrs) => { search_builder.add_facets(prepare_facet_list(facets, &schema, attrs)?); },
            None => return Err(ResponseError::FacetExpression("can't return facets count, as no facet is set".to_string()))
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

    if let Some(matches) = params.matches {
        if matches {
            search_builder.get_matches();
        }
    }

    Ok(HttpResponse::Ok().json(search_builder.search(&reader)?))
}

fn prepare_facet_list<'fa>(facets: &str, schema: &Schema, facet_attrs: &'fa [FieldId]) -> Result<Vec<FieldId>, ResponseError> {
    let facet_array = serde_json::from_str(facets).expect("do error handling"); // TODO
    match facet_array {
        Value::Array(facet_array) => {
            let wild_card = Value::String("*".to_string());
            if facet_array.iter().any(|it| it == &wild_card) {
                return Ok(Vec::from(facet_attrs)); // TODO can make cow?
            }
            let mut fields = Vec::with_capacity(facet_attrs.len());
            for v in facet_array {
                match v {
                    Value::String(name) => {
                        let id = schema.id(&name).expect("not found error"); // TODO
                        fields.push(id);
                    }
                    _ => todo!("expected string, found {}", v),
                }
            }
            return Ok(fields);
        }
        _ => todo!("error, bad syntax, expected array")
    }
}
