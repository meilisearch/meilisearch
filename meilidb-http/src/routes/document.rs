use std::collections::{BTreeSet, HashSet};

use http::StatusCode;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tide::querystring::ContextExt as QSContextExt;
use tide::response::IntoResponse;
use tide::{Context, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::models::token::ACL::*;
use crate::Data;

pub async fn get_document(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(DocumentsRead)?;

    let index = ctx.index()?;

    let identifier = ctx.identifier()?;
    let document_id = meilidb_core::serde::compute_document_id(identifier.clone());

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let response = index
        .document::<IndexMap<String, Value>>(&reader, None, document_id)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::document_not_found(&identifier))?;

    if response.is_empty() {
        return Err(ResponseError::document_not_found(identifier));
    }

    Ok(tide::response::json(response))
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

pub async fn delete_document(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(DocumentsWrite)?;

    if !ctx.state().accept_updates() {
        return Err(ResponseError::Maintenance);
    }

    let index = ctx.index()?;
    let identifier = ctx.identifier()?;
    let document_id = meilidb_core::serde::compute_document_id(identifier.clone());

    let env = &ctx.state().db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let mut documents_deletion = index.documents_deletion();
    documents_deletion.delete_document_by_id(document_id);
    let update_id = documents_deletion
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

pub async fn browse_documents(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(DocumentsRead)?;

    let index = ctx.index()?;
    let query: BrowseQuery = ctx.url_query().unwrap_or(BrowseQuery::default());

    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(20);

    let env = &ctx.state().db.env;
    let reader = env.read_txn().map_err(ResponseError::internal)?;

    let documents_ids: Result<BTreeSet<_>, _> =
        match index.documents_fields_counts.documents_ids(&reader) {
            Ok(documents_ids) => documents_ids.skip(offset).take(limit).collect(),
            Err(e) => return Err(ResponseError::internal(e)),
        };

    let documents_ids = match documents_ids {
        Ok(documents_ids) => documents_ids,
        Err(e) => return Err(ResponseError::internal(e)),
    };

    let mut response_body = Vec::<IndexMap<String, Value>>::new();

    if let Some(attributes) = query.attributes_to_retrieve {
        let attributes = attributes.split(',').collect::<HashSet<&str>>();
        for document_id in documents_ids {
            if let Ok(Some(document)) = index.document(&reader, Some(&attributes), document_id) {
                response_body.push(document);
            }
        }
    } else {
        for document_id in documents_ids {
            if let Ok(Some(document)) = index.document(&reader, None, document_id) {
                response_body.push(document);
            }
        }
    }

    if response_body.is_empty() {
        Ok(tide::response::json(response_body)
            .with_status(StatusCode::NO_CONTENT)
            .into_response())
    } else {
        Ok(tide::response::json(response_body)
            .with_status(StatusCode::OK)
            .into_response())
    }
}

fn infered_schema(document: &IndexMap<String, Value>) -> Option<meilidb_schema::Schema> {
    use meilidb_schema::{SchemaBuilder, DISPLAYED, INDEXED};

    let mut identifier = None;
    for key in document.keys() {
        if identifier.is_none() && key.to_lowercase().contains("id") {
            identifier = Some(key);
        }
    }

    match identifier {
        Some(identifier) => {
            let mut builder = SchemaBuilder::with_identifier(identifier);
            for key in document.keys() {
                builder.new_attribute(key, DISPLAYED | INDEXED);
            }
            Some(builder.build())
        }
        None => None,
    }
}

pub async fn add_or_update_multiple_documents(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(DocumentsWrite)?;

    if !ctx.state().accept_updates() {
        return Err(ResponseError::Maintenance);
    }
    let data: Vec<IndexMap<String, Value>> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let index = ctx.index()?;

    let env = &ctx.state().db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let current_schema = index
        .main
        .schema(&writer)
        .map_err(ResponseError::internal)?;
    if current_schema.is_none() {
        match data.first().and_then(infered_schema) {
            Some(schema) => {
                index
                    .schema_update(&mut writer, schema)
                    .map_err(ResponseError::internal)?;
            }
            None => return Err(ResponseError::bad_request("Could not infer a schema")),
        }
    }

    let mut document_addition = index.documents_addition();

    for document in data {
        document_addition.update_document(document);
    }

    let update_id = document_addition
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn delete_multiple_documents(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(DocumentsWrite)?;
    if !ctx.state().accept_updates() {
        return Err(ResponseError::Maintenance);
    }
    let data: Vec<Value> = ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let index = ctx.index()?;

    let env = &ctx.state().db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;

    let mut documents_deletion = index.documents_deletion();

    for identifier in data {
        if let Some(identifier) = meilidb_core::serde::value_to_string(&identifier) {
            documents_deletion
                .delete_document_by_id(meilidb_core::serde::compute_document_id(identifier));
        }
    }

    let update_id = documents_deletion
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn clear_all_documents(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(DocumentsWrite)?;
    if !ctx.state().accept_updates() {
        return Err(ResponseError::Maintenance);
    }
    let index = ctx.index()?;

    let env = &ctx.state().db.env;
    let mut writer = env.write_txn().map_err(ResponseError::internal)?;
    let update_id = index
        .clear_all(&mut writer)
        .map_err(ResponseError::internal)?;
    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}
