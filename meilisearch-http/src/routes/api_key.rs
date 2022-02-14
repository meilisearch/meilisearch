use std::str;

use actix_web::{web, HttpRequest, HttpResponse};

use meilisearch_auth::{Action, AuthController, Key};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::extractors::authentication::{policies::*, GuardedData};
use meilisearch_error::ResponseError;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::post().to(create_api_key))
            .route(web::get().to(list_api_keys)),
    )
    .service(
        web::resource("/{api_key}")
            .route(web::get().to(get_api_key))
            .route(web::patch().to(patch_api_key))
            .route(web::delete().to(delete_api_key)),
    );
}

pub async fn create_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    body: web::Json<Value>,
    _req: HttpRequest,
) -> Result<HttpResponse, ResponseError> {
    let key = auth_controller.create_key(body.into_inner()).await?;
    let res = KeyView::from_key(key, &auth_controller);

    Ok(HttpResponse::Created().json(res))
}

pub async fn list_api_keys(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    _req: HttpRequest,
) -> Result<HttpResponse, ResponseError> {
    let keys = auth_controller.list_keys().await?;
    let res: Vec<_> = keys
        .into_iter()
        .map(|k| KeyView::from_key(k, &auth_controller))
        .collect();

    Ok(HttpResponse::Ok().json(KeyListView::from(res)))
}

pub async fn get_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let key = auth_controller.get_key(&path.api_key).await?;
    let res = KeyView::from_key(key, &auth_controller);

    Ok(HttpResponse::Ok().json(res))
}

pub async fn patch_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    body: web::Json<Value>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let key = auth_controller
        .update_key(&path.api_key, body.into_inner())
        .await?;
    let res = KeyView::from_key(key, &auth_controller);

    Ok(HttpResponse::Ok().json(res))
}

pub async fn delete_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    auth_controller.delete_key(&path.api_key).await?;

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
