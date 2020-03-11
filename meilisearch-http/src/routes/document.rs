use std::collections::{BTreeSet, HashSet};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tide::{Request, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::RequestExt;
use crate::helpers::tide::ACL::*;
use crate::Data;

pub async fn get_document(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Public)?;

    let index = ctx.index()?;

    let original_document_id = ctx.document_id()?;
    let document_id = meilisearch_core::serde::compute_document_id(original_document_id.clone());

    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let response = index
        .document::<IndexMap<String, Value>>(&reader, None, document_id)?
        .ok_or(ResponseError::document_not_found(&original_document_id))?;

    if response.is_empty() {
        return Err(ResponseError::document_not_found(&original_document_id));
    }

    Ok(tide::Response::new(200).body_json(&response)?)
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

pub async fn delete_document(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let index = ctx.index()?;
    let document_id = ctx.document_id()?;
    let document_id = meilisearch_core::serde::compute_document_id(document_id);
    let db = &ctx.state().db;
    let mut update_writer = db.update_write_txn()?;
    let mut documents_deletion = index.documents_deletion();
    documents_deletion.delete_document_by_id(document_id);
    let update_id = documents_deletion.finalize(&mut update_writer)?;

    update_writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

pub async fn get_all_documents(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let index = ctx.index()?;
    let query: BrowseQuery = ctx.query().unwrap_or_default();

    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(20);

    let db = &ctx.state().db;
    let reader = db.main_read_txn()?;

    let documents_ids: Result<BTreeSet<_>, _> = index
        .documents_fields_counts
        .documents_ids(&reader)?
        .skip(offset)
        .take(limit)
        .collect();

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

    Ok(tide::Response::new(200).body_json(&response_body)?)
}

fn find_primary_key(document: &IndexMap<String, Value>) -> Option<String> {
    for key in document.keys() {
        if key.to_lowercase().contains("id") {
            return Some(key.to_string());
        }
    }
    None
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateDocumentsQuery {
    primary_key: Option<String>,
}

async fn update_multiple_documents(mut ctx: Request<Data>, is_partial: bool) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let index = ctx.index()?;

    let data: Vec<IndexMap<String, Value>> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let query: UpdateDocumentsQuery = ctx.query().unwrap_or_default();

    let db = &ctx.state().db;

    let reader = db.main_read_txn()?;
    let mut schema = index
        .main
        .schema(&reader)?
        .ok_or(ResponseError::internal("schema not found"))?;

    if schema.primary_key().is_none() {
        let id = match query.primary_key {
            Some(id) => id,
            None => match data.first().and_then(|docs| find_primary_key(docs)) {
                Some(id) => id,
                None => return Err(ResponseError::bad_request("Could not infer a primary key")),
            },
        };

        let mut writer = db.main_write_txn()?;
        schema.set_primary_key(&id).map_err(ResponseError::bad_request)?;
        index.main.put_schema(&mut writer, &schema)?;
        writer.commit()?;
    }

    let mut document_addition = if is_partial {
        index.documents_partial_addition()
    } else {
        index.documents_addition()
    };

    for document in data {
        document_addition.update_document(document);
    }

    let mut update_writer = db.update_write_txn()?;
    let update_id = document_addition.finalize(&mut update_writer)?;
    update_writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn add_or_replace_multiple_documents(ctx: Request<Data>) -> SResult<Response> {
    update_multiple_documents(ctx, false).await
}

pub async fn add_or_update_multiple_documents(ctx: Request<Data>) -> SResult<Response> {
    update_multiple_documents(ctx, true).await
}

pub async fn delete_multiple_documents(mut ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let data: Vec<Value> = ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let mut documents_deletion = index.documents_deletion();

    for document_id in data {
        if let Some(document_id) = meilisearch_core::serde::value_to_string(&document_id) {
            documents_deletion
                .delete_document_by_id(meilisearch_core::serde::compute_document_id(document_id));
        }
    }

    let update_id = documents_deletion.finalize(&mut writer)?;

    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}

pub async fn clear_all_documents(ctx: Request<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn()?;

    let update_id = index.clear_all(&mut writer)?;
    writer.commit()?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::Response::new(202).body_json(&response_body)?)
}
