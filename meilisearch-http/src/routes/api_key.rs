use std::str;

use actix_web::{web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

use meilisearch_auth::{error::AuthControllerError, Action, AuthController, Key};
use meilisearch_types::error::{Code, ResponseError};

use crate::extractors::{
    authentication::{policies::*, GuardedData},
    sequential_extractor::SeqHandler,
};
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
    body: web::Json<Value>,
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

pub async fn list_api_keys(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_GET }>, AuthController>,
    paginate: web::Query<Pagination>,
) -> Result<HttpResponse, ResponseError> {
    let page_view = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let keys = auth_controller.list_keys()?;
        let page_view = paginate.auto_paginate_sized(
            keys.into_iter()
                .map(|k| KeyView::from_key(k, &auth_controller)),
        );

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
    body: web::Json<Value>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let key = path.into_inner().key;
    let body = body.into_inner();
    let res = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let uid =
            Uuid::parse_str(&key).or_else(|_| auth_controller.get_uid_from_encoded_key(&key))?;
        let key = auth_controller.update_key(uid, body)?;

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
            indexes: key.indexes.into_iter().map(String::from).collect(),
            expires_at: key.expires_at,
            created_at: key.created_at,
            updated_at: key.updated_at,
        }
    }
}
