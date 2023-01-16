use std::str;

use actix_web::{web, HttpRequest, HttpResponse};
use deserr::DeserializeFromValue;
use meilisearch_auth::error::AuthControllerError;
use meilisearch_auth::AuthController;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::keys::{Action, CreateApiKey, Key, PatchApiKey};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use super::PAGINATION_DEFAULT_LIMIT;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::json::ValidatedJson;
use crate::extractors::query_parameters::QueryParameter;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::Pagination;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::post().to(SeqHandler(create_api_key)))
            .route(web::get().to(SeqHandler(list_api_keys))),
    )
    .service(
        web::resource("/{key}")
            .route(web::get().to(SeqHandler(get_api_key)))
            .route(web::patch().to(SeqHandler(patch_api_key)))
            .route(web::delete().to(SeqHandler(delete_api_key))),
    );
}

pub async fn create_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_CREATE }>, AuthController>,
    body: ValidatedJson<CreateApiKey, DeserrJsonError>,
    _req: HttpRequest,
) -> Result<HttpResponse, ResponseError> {
    let v = body.into_inner();
    let res = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let key = auth_controller.create_key(v)?;
        Ok(KeyView::from_key(key, &auth_controller))
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::Created().json(res))
}

#[derive(DeserializeFromValue, Debug, Clone, Copy)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
pub struct ListApiKeys {
    #[deserr(default, error = DeserrQueryParamError<InvalidApiKeyOffset>)]
    pub offset: Param<usize>,
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidApiKeyLimit>)]
    pub limit: Param<usize>,
}
impl ListApiKeys {
    fn as_pagination(self) -> Pagination {
        Pagination { offset: self.offset.0, limit: self.limit.0 }
    }
}

pub async fn list_api_keys(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_GET }>, AuthController>,
    list_api_keys: QueryParameter<ListApiKeys, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    let paginate = list_api_keys.into_inner().as_pagination();
    let page_view = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let keys = auth_controller.list_keys()?;
        let page_view = paginate
            .auto_paginate_sized(keys.into_iter().map(|k| KeyView::from_key(k, &auth_controller)));

        Ok(page_view)
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::Ok().json(page_view))
}

pub async fn get_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_GET }>, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let key = path.into_inner().key;

    let res = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let uid =
            Uuid::parse_str(&key).or_else(|_| auth_controller.get_uid_from_encoded_key(&key))?;
        let key = auth_controller.get_key(uid)?;

        Ok(KeyView::from_key(key, &auth_controller))
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::Ok().json(res))
}

pub async fn patch_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_UPDATE }>, AuthController>,
    body: ValidatedJson<PatchApiKey, DeserrJsonError>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let key = path.into_inner().key;
    let patch_api_key = body.into_inner();
    let res = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let uid =
            Uuid::parse_str(&key).or_else(|_| auth_controller.get_uid_from_encoded_key(&key))?;
        let key = auth_controller.update_key(uid, patch_api_key)?;

        Ok(KeyView::from_key(key, &auth_controller))
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::Ok().json(res))
}

pub async fn delete_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_DELETE }>, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let key = path.into_inner().key;
    tokio::task::spawn_blocking(move || {
        let uid =
            Uuid::parse_str(&key).or_else(|_| auth_controller.get_uid_from_encoded_key(&key))?;
        auth_controller.delete_key(uid)
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Deserialize)]
pub struct AuthParam {
    key: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KeyView {
    name: Option<String>,
    description: Option<String>,
    key: String,
    uid: Uuid,
    actions: Vec<Action>,
    indexes: Vec<String>,
    #[serde(serialize_with = "time::serde::rfc3339::option::serialize")]
    expires_at: Option<OffsetDateTime>,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    created_at: OffsetDateTime,
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    updated_at: OffsetDateTime,
}

impl KeyView {
    fn from_key(key: Key, auth: &AuthController) -> Self {
        let generated_key = auth.generate_key(key.uid).unwrap_or_default();

        KeyView {
            name: key.name,
            description: key.description,
            key: generated_key,
            uid: key.uid,
            actions: key.actions,
            indexes: key.indexes.into_iter().map(|x| x.to_string()).collect(),
            expires_at: key.expires_at,
            created_at: key.created_at,
            updated_at: key.updated_at,
        }
    }
}
