use std::str;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::Deserr;
use meilisearch_auth::error::AuthControllerError;
use meilisearch_auth::AuthController;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::keys::{CreateApiKey, Key, PatchApiKey};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use utoipa::{IntoParams, OpenApi, ToSchema};
use uuid::Uuid;

use super::{PaginationView, PAGINATION_DEFAULT_LIMIT, PAGINATION_DEFAULT_LIMIT_FN};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::Pagination;

#[derive(OpenApi)]
#[openapi(
    paths(create_api_key, list_api_keys, get_api_key, patch_api_key, delete_api_key),
    tags((
        name = "Keys",
        description = "Manage API `keys` for a Meilisearch instance. Each key has a given set of permissions.
You must have the master key or the default admin key to access the keys route. More information about the keys and their rights.
Accessing any route under `/keys` without having set a master key will result in an error.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/keys"),
    )),
)]
pub struct ApiKeyApi;

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

/// Create an API Key
///
/// Create an API Key.
#[utoipa::path(
    post,
    path = "",
    tag = "Keys",
    security(("Bearer" = ["keys.create", "keys.*", "*"])),
    request_body = CreateApiKey,
    responses(
        (status = 202, description = "Key has been created", body = KeyView, content_type = "application/json", example = json!(
            {
                "uid": "01b4bc42-eb33-4041-b481-254d00cce834",
                "key": "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
                "name": "Indexing Products API key",
                "description": null,
                "actions": [
                    "documents.add"
                ],
                "indexes": [
                    "products"
                ],
                "expiresAt": "2021-11-13T00:00:00Z",
                "createdAt": "2021-11-12T10:00:00Z",
                "updatedAt": "2021-11-12T10:00:00Z"
            }
        )),
        (status = 401, description = "The route has been hit on an unprotected instance", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
                "code": "missing_master_key",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_master_key"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn create_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_CREATE }>, Data<AuthController>>,
    body: AwebJson<CreateApiKey, DeserrJsonError>,
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

#[derive(Deserr, Debug, Clone, Copy, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct ListApiKeys {
    #[deserr(default, error = DeserrQueryParamError<InvalidApiKeyOffset>)]
    #[param(value_type = usize, default = 0)]
    pub offset: Param<usize>,
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidApiKeyLimit>)]
    #[param(value_type = usize, default = PAGINATION_DEFAULT_LIMIT_FN)]
    pub limit: Param<usize>,
}

impl ListApiKeys {
    fn as_pagination(self) -> Pagination {
        Pagination { offset: self.offset.0, limit: self.limit.0 }
    }
}

/// Get API Keys
///
/// List all API Keys
#[utoipa::path(
    get,
    path = "",
    tag = "Keys",
    security(("Bearer" = ["keys.get", "keys.*", "*"])),
    params(ListApiKeys),
    responses(
        (status = 202, description = "List of keys", body = PaginationView<KeyView>, content_type = "application/json", example = json!(
            {
                "results": [
                    {
                        "uid": "01b4bc42-eb33-4041-b481-254d00cce834",
                        "key": "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
                        "name": "An API Key",
                        "description": null,
                        "actions": [
                            "documents.add"
                        ],
                        "indexes": [
                            "movies"
                        ],
                        "expiresAt": "2022-11-12T10:00:00Z",
                        "createdAt": "2021-11-12T10:00:00Z",
                        "updatedAt": "2021-11-12T10:00:00Z"
                    }
                ],
                "limit": 20,
                "offset": 0,
                "total": 1
            }
        )),
        (status = 401, description = "The route has been hit on an unprotected instance", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
                "code": "missing_master_key",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_master_key"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn list_api_keys(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_GET }>, Data<AuthController>>,
    list_api_keys: AwebQueryParameter<ListApiKeys, DeserrQueryParamError>,
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

/// Get an API Key
///
/// Get an API key from its `uid` or its `key` field.
#[utoipa::path(
    get,
    path = "/{uidOrKey}",
    tag = "Keys",
    security(("Bearer" = ["keys.get", "keys.*", "*"])),
    params(("uidOrKey" = String, Path, format = Password, example = "7b198a7f-52a0-4188-8762-9ad93cd608b2", description = "The `uid` or `key` field of an existing API key", nullable = false)),
    responses(
        (status = 200, description = "The key is returned", body = KeyView, content_type = "application/json", example = json!(
            {
                "uid": "01b4bc42-eb33-4041-b481-254d00cce834",
                "key": "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
                "name": "An API Key",
                "description": null,
                "actions": [
                    "documents.add"
                ],
                "indexes": [
                    "movies"
                ],
                "expiresAt": "2022-11-12T10:00:00Z",
                "createdAt": "2021-11-12T10:00:00Z",
                "updatedAt": "2021-11-12T10:00:00Z"
            }
        )),
        (status = 401, description = "The route has been hit on an unprotected instance", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
                "code": "missing_master_key",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_master_key"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn get_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_GET }>, Data<AuthController>>,
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

/// Update a Key
///
/// Update the name and description of an API key.
/// Updates to keys are partial. This means you should provide only the fields you intend to update, as any fields not present in the payload will remain unchanged.
#[utoipa::path(
    patch,
    path = "/{uidOrKey}",
    tag = "Keys",
    security(("Bearer" = ["keys.update", "keys.*", "*"])),
    params(("uidOrKey" = String, Path, format = Password, example = "7b198a7f-52a0-4188-8762-9ad93cd608b2", description = "The `uid` or `key` field of an existing API key", nullable = false)),
    request_body = PatchApiKey,
    responses(
        (status = 200, description = "The key have been updated", body = KeyView, content_type = "application/json", example = json!(
            {
                "uid": "01b4bc42-eb33-4041-b481-254d00cce834",
                "key": "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
                "name": "An API Key",
                "description": null,
                "actions": [
                    "documents.add"
                ],
                "indexes": [
                    "movies"
                ],
                "expiresAt": "2022-11-12T10:00:00Z",
                "createdAt": "2021-11-12T10:00:00Z",
                "updatedAt": "2021-11-12T10:00:00Z"
            }
        )),
        (status = 401, description = "The route has been hit on an unprotected instance", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
                "code": "missing_master_key",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_master_key"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn patch_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_UPDATE }>, Data<AuthController>>,
    body: AwebJson<PatchApiKey, DeserrJsonError>,
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

/// Delete a key
///
/// Delete the specified API key.
#[utoipa::path(
    delete,
    path = "/{uidOrKey}",
    tag = "Keys",
    security(("Bearer" = ["keys.delete", "keys.*", "*"])),
    params(("uidOrKey" = String, Path, format = Password, example = "7b198a7f-52a0-4188-8762-9ad93cd608b2", description = "The `uid` or `key` field of an existing API key", nullable = false)),
    responses(
        (status = NO_CONTENT, description = "The key have been removed"),
        (status = 401, description = "The route has been hit on an unprotected instance", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
                "code": "missing_master_key",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_master_key"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn delete_api_key(
    auth_controller: GuardedData<ActionPolicy<{ actions::KEYS_DELETE }>, Data<AuthController>>,
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

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct KeyView {
    /// The name of the API Key if any
    name: Option<String>,
    /// The description of the API Key if any
    description: Option<String>,
    /// The actual API Key you can send to Meilisearch
    key: String,
    /// The `Uuid` specified while creating the key or autogenerated by Meilisearch.
    uid: Uuid,
    /// The actions accessible with this key.
    actions: Vec<Action>,
    /// The indexes accessible with this key.
    indexes: Vec<String>,
    /// The expiration date of the key. Once this timestamp is exceeded the key is not deleted but cannot be used anymore.
    #[serde(serialize_with = "time::serde::rfc3339::option::serialize")]
    expires_at: Option<OffsetDateTime>,
    /// The date of creation of this API Key.
    #[schema(read_only)]
    #[serde(serialize_with = "time::serde::rfc3339::serialize")]
    created_at: OffsetDateTime,
    /// The date of the last update made on this key.
    #[schema(read_only)]
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
