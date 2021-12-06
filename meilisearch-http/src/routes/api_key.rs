use std::str;

use actix_web::{web, HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use meilisearch_auth::{generate_key, Action, AuthController, Key};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    let res = KeyView::from_key(key, auth_controller.get_master_key());

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Created().json(res))
}

pub async fn list_api_keys(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    _req: HttpRequest,
) -> Result<HttpResponse, ResponseError> {
    let keys = auth_controller.list_keys().await?;
    let res: Vec<_> = keys
        .into_iter()
        .map(|k| KeyView::from_key(k, auth_controller.get_master_key()))
        .collect();

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Ok().json(res))
}

pub async fn get_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    // keep 8 first characters that are the ID of the API key.
    let key = auth_controller.get_key(&path.api_key).await?;
    let res = KeyView::from_key(key, auth_controller.get_master_key());

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Ok().json(res))
}

pub async fn patch_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    body: web::Json<Value>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    let key = auth_controller
        // keep 8 first characters that are the ID of the API key.
        .update_key(&path.api_key, body.into_inner())
        .await?;
    let res = KeyView::from_key(key, auth_controller.get_master_key());

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Ok().json(res))
}

pub async fn delete_api_key(
    auth_controller: GuardedData<MasterPolicy, AuthController>,
    path: web::Path<AuthParam>,
) -> Result<HttpResponse, ResponseError> {
    // keep 8 first characters that are the ID of the API key.
    auth_controller.delete_key(&path.api_key).await?;

    Ok(HttpResponse::NoContent().json(()))
}

#[derive(Deserialize)]
pub struct AuthParam {
    api_key: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KeyView {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    key: String,
    actions: Vec<Action>,
    indexes: Vec<String>,
    expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl KeyView {
    fn from_key(key: Key, master_key: Option<&String>) -> Self {
        let key_id = str::from_utf8(&key.id).unwrap();
        let generated_key = match master_key {
            Some(master_key) => generate_key(master_key.as_bytes(), key_id),
            None => generate_key(&[], key_id),
        };

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
