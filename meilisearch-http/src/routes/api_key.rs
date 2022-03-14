use std::str;

use actix_web::{web, HttpRequest, HttpResponse};

use meilisearch_auth::{error::AuthControllerError, Action, AuthController, Key};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::extractors::{
    authentication::{policies::*, GuardedData},
    sequential_extractor::SeqHandler,
};
use meilisearch_error::{Code, ResponseError};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::post().to(SeqHandler(create_api_key)))
            .route(web::get().to(SeqHandler(list_api_keys))),
    )
    .service(
        web::resource("/{api_key}")
            .route(web::get().to(SeqHandler(get_api_key)))
            .route(web::patch().to(SeqHandler(patch_api_key)))
            .route(web::delete().to(SeqHandler(delete_api_key))),
    );
}

pub async fn create_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
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
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    _req: HttpRequest,
) -> Result<HttpResponse, ResponseError> {
    let res = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let keys = auth_controller.list_keys()?;
        let res: Vec<_> = keys
            .into_iter()
            .map(|k| KeyView::from_key(k, &auth_controller))
            .collect();
        Ok(res)
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::Ok().json(KeyListView::from(res)))
}

pub async fn get_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let api_key = path.into_inner().api_key;
    let res = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let key = auth_controller.get_key(&api_key)?;
        Ok(KeyView::from_key(key, &auth_controller))
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::Ok().json(res))
}

pub async fn patch_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    body: web::Json<Value>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let api_key = path.into_inner().api_key;
    let body = body.into_inner();
    let res = tokio::task::spawn_blocking(move || -> Result<_, AuthControllerError> {
        let key = auth_controller.update_key(&api_key, body)?;
        Ok(KeyView::from_key(key, &auth_controller))
    })
    .await
    .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::Ok().json(res))
}

pub async fn delete_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let api_key = path.into_inner().api_key;
    tokio::task::spawn_blocking(move || auth_controller.delete_key(&api_key))
        .await
        .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))??;

    Ok(HttpResponse::NoContent().finish())
}

#[derive(Deserialize)]
pub struct AuthParam {
    api_key: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KeyView {
    description: Option<String>,
    key: String,
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
        let key_id = str::from_utf8(&key.id).unwrap();
        let generated_key = auth.generate_key(key_id).unwrap_or_default();

        KeyView {
            description: key.description,
            key: generated_key,
            actions: key.actions,
            indexes: key.indexes,
            expires_at: key.expires_at,
            created_at: key.created_at,
            updated_at: key.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
struct KeyListView {
    results: Vec<KeyView>,
}

impl From<Vec<KeyView>> for KeyListView {
    fn from(results: Vec<KeyView>) -> Self {
        Self { results }
    }
}
