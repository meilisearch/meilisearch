use core::convert::Infallible;
use std::collections::BTreeMap;
use std::str::FromStr;

use actix_http::header::{
    HeaderName, HeaderValue, InvalidHeaderName as ActixInvalidHeaderName,
    InvalidHeaderValue as ActixInvalidHeaderValue,
};
use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::{DeserializeError, Deserr, ValuePointerRef};
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::{immutable_field_error, DeserrJsonError};
use meilisearch_types::error::deserr_codes::{
    BadRequest, InvalidWebhookHeaders, InvalidWebhookUrl,
};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::webhooks::Webhook;
use serde::Serialize;
use tracing::debug;
use url::Url;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;
use WebhooksError::*;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

#[derive(OpenApi)]
#[openapi(
    paths(get_webhooks, get_webhook, post_webhook, patch_webhook, delete_webhook),
    tags((
        name = "Webhooks",
        description = "The `/webhooks` route allows you to register endpoints to be called once tasks are processed.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/webhooks"),
    )),
)]
pub struct WebhooksApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(get_webhooks))
            .route(web::post().to(SeqHandler(post_webhook))),
    )
    .service(
        web::resource("/{uuid}")
            .route(web::get().to(get_webhook))
            .route(web::patch().to(SeqHandler(patch_webhook)))
            .route(web::delete().to(SeqHandler(delete_webhook))),
    );
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields = deny_immutable_fields_webhook)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub(super) struct WebhookSettings {
    #[schema(value_type = Option<String>, example = "https://your.site/on-tasks-completed")]
    #[deserr(default, error = DeserrJsonError<InvalidWebhookUrl>)]
    #[serde(default)]
    url: Setting<String>,
    #[schema(value_type = Option<BTreeMap<String, String>>, example = json!({"Authorization":"Bearer a-secret-token"}))]
    #[deserr(default, error = DeserrJsonError<InvalidWebhookHeaders>)]
    #[serde(default)]
    headers: Setting<BTreeMap<String, Setting<String>>>,
}

fn deny_immutable_fields_webhook(
    field: &str,
    accepted: &[&str],
    location: ValuePointerRef,
) -> DeserrJsonError {
    match field {
        "uuid" => immutable_field_error(field, accepted, Code::ImmutableWebhookUuid),
        "isEditable" => immutable_field_error(field, accepted, Code::ImmutableWebhookIsEditable),
        _ => deserr::take_cf_content(DeserrJsonError::<BadRequest>::error::<Infallible>(
            None,
            deserr::ErrorKind::UnknownKey { key: field, accepted },
            location,
        )),
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub(super) struct WebhookWithMetadataRedactedAuthorization {
    uuid: Uuid,
    is_editable: bool,
    #[schema(value_type = WebhookSettings)]
    #[serde(flatten)]
    webhook: Webhook,
}

impl WebhookWithMetadataRedactedAuthorization {
    pub fn from(uuid: Uuid, mut webhook: Webhook) -> Self {
        webhook.redact_authorization_header();
        Self { uuid, is_editable: uuid != Uuid::nil(), webhook }
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct WebhookResults {
    results: Vec<WebhookWithMetadataRedactedAuthorization>,
}

/// List webhooks
///
/// Get the list of all registered webhooks.
#[utoipa::path(
    get,
    path = "",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.get", "webhooks.*", "*.get", "*"])),
    responses(
        (status = OK, description = "Webhooks are returned", body = WebhookResults, content_type = "application/json", example = json!({
            "results": [
                {
                    "uuid": "550e8400-e29b-41d4-a716-446655440000",
                    "url": "https://your.site/on-tasks-completed",
                    "headers": {
                        "Authorization": "Bearer a-secret-token"
                    },
                    "isEditable": true
                },
                {
                    "uuid": "550e8400-e29b-41d4-a716-446655440001",
                    "url": "https://another.site/on-tasks-completed",
                    "isEditable": true
                }
            ]
        })),
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
async fn get_webhooks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_GET }>, Data<IndexScheduler>>,
) -> Result<HttpResponse, ResponseError> {
    let webhooks = index_scheduler.webhooks_view();
    let results = webhooks
        .webhooks
        .into_iter()
        .map(|(uuid, webhook)| WebhookWithMetadataRedactedAuthorization::from(uuid, webhook))
        .collect::<Vec<_>>();
    let results = WebhookResults { results };

    debug!(returns = ?results, "Get webhooks");
    Ok(HttpResponse::Ok().json(results))
}

#[derive(Serialize, Default)]
pub struct PatchWebhooksAnalytics;

impl Aggregate for PatchWebhooksAnalytics {
    fn event_name(&self) -> &'static str {
        "Webhooks Updated"
    }

    fn aggregate(self: Box<Self>, _new: Box<Self>) -> Box<Self> {
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

#[derive(Serialize, Default)]
pub struct PostWebhooksAnalytics;

impl Aggregate for PostWebhooksAnalytics {
    fn event_name(&self) -> &'static str {
        "Webhooks Created"
    }

    fn aggregate(self: Box<Self>, _new: Box<Self>) -> Box<Self> {
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

#[derive(Debug, thiserror::Error)]
enum WebhooksError {
    #[error("The URL for the webhook `{0}` is missing.")]
    MissingUrl(Uuid),
    #[error("Defining too many webhooks would crush the server. Please limit the number of webhooks to 20. You may use a third-party proxy server to dispatch events to more than 20 endpoints.")]
    TooManyWebhooks,
    #[error("Too many headers for the webhook `{0}`. Please limit the number of headers to 200. Hint: To remove an already defined header set its value to `null`")]
    TooManyHeaders(Uuid),
    #[error("Webhook `{0}` is immutable. The webhook defined from the command line cannot be modified using the API.")]
    ImmutableWebhook(Uuid),
    #[error("Webhook `{0}` not found.")]
    WebhookNotFound(Uuid),
    #[error("Invalid header name `{0}`: {1}")]
    InvalidHeaderName(String, ActixInvalidHeaderName),
    #[error("Invalid header value `{0}`: {1}")]
    InvalidHeaderValue(String, ActixInvalidHeaderValue),
    #[error("Invalid URL `{0}`: {1}")]
    InvalidUrl(String, url::ParseError),
    #[error("Invalid UUID: {0}")]
    InvalidUuid(uuid::Error),
}

impl ErrorCode for WebhooksError {
    fn error_code(&self) -> meilisearch_types::error::Code {
        match self {
            MissingUrl(_) => meilisearch_types::error::Code::InvalidWebhookUrl,
            TooManyWebhooks => meilisearch_types::error::Code::InvalidWebhooks,
            TooManyHeaders(_) => meilisearch_types::error::Code::InvalidWebhookHeaders,
            ImmutableWebhook(_) => meilisearch_types::error::Code::ImmutableWebhook,
            WebhookNotFound(_) => meilisearch_types::error::Code::WebhookNotFound,
            InvalidHeaderName(_, _) => meilisearch_types::error::Code::InvalidWebhookHeaders,
            InvalidHeaderValue(_, _) => meilisearch_types::error::Code::InvalidWebhookHeaders,
            InvalidUrl(_, _) => meilisearch_types::error::Code::InvalidWebhookUrl,
            InvalidUuid(_) => meilisearch_types::error::Code::InvalidWebhookUuid,
        }
    }
}

fn patch_webhook_inner(
    uuid: &Uuid,
    old_webhook: Webhook,
    new_webhook: WebhookSettings,
) -> Result<Webhook, WebhooksError> {
    let Webhook { url: old_url, mut headers } = old_webhook;

    let url = match new_webhook.url {
        Setting::Set(url) => url,
        Setting::NotSet => old_url,
        Setting::Reset => return Err(MissingUrl(uuid.to_owned())),
    };

    match new_webhook.headers {
        Setting::Set(new_headers) => {
            for (name, value) in new_headers {
                match value {
                    Setting::Set(value) => {
                        headers.insert(name, value);
                    }
                    Setting::NotSet => continue,
                    Setting::Reset => {
                        headers.remove(&name);
                        continue;
                    }
                }
            }
        }
        Setting::Reset => headers.clear(),
        Setting::NotSet => (),
    };

    if headers.len() > 200 {
        return Err(TooManyHeaders(uuid.to_owned()));
    }

    Ok(Webhook { url, headers })
}

fn check_changed(uuid: Uuid, webhook: &Webhook) -> Result<(), WebhooksError> {
    if uuid.is_nil() {
        return Err(ImmutableWebhook(uuid));
    }

    if webhook.url.is_empty() {
        return Err(MissingUrl(uuid));
    }

    if webhook.headers.len() > 200 {
        return Err(TooManyHeaders(uuid));
    }

    for (header, value) in &webhook.headers {
        HeaderName::from_bytes(header.as_bytes())
            .map_err(|e| InvalidHeaderName(header.to_owned(), e))?;
        HeaderValue::from_str(value).map_err(|e| InvalidHeaderValue(header.to_owned(), e))?;
    }

    if let Err(e) = Url::parse(&webhook.url) {
        return Err(InvalidUrl(webhook.url.to_owned(), e));
    }

    Ok(())
}

/// Get a webhook
///
/// Get a single webhook by its UUID.
#[utoipa::path(
    get,
    path = "/{uuid}",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.get", "webhooks.*", "*.get", "*"])),
    responses(
        (status = 200, description = "Webhook found", body = WebhookWithMetadataRedactedAuthorization, content_type = "application/json", example = json!({
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "url": "https://your.site/on-tasks-completed",
            "headers": {
                "Authorization": "Bearer a-secret"
            },
            "isEditable": true
        })),
        (status = 404, description = "Webhook not found", body = ResponseError, content_type = "application/json"),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json"),
    ),
    params(
        ("uuid" = Uuid, Path, description = "The universally unique identifier of the webhook")
    )
)]
async fn get_webhook(
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_GET }>, Data<IndexScheduler>>,
    uuid: Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let uuid = Uuid::from_str(&uuid.into_inner()).map_err(InvalidUuid)?;
    let mut webhooks = index_scheduler.webhooks_view();

    let webhook = webhooks.webhooks.remove(&uuid).ok_or(WebhookNotFound(uuid))?;
    let webhook = WebhookWithMetadataRedactedAuthorization::from(uuid, webhook);

    debug!(returns = ?webhook, "Get webhook");
    Ok(HttpResponse::Ok().json(webhook))
}

/// Create a webhook
///
/// Create a new webhook to receive task notifications.
#[utoipa::path(
    post,
    path = "",
    tag = "Webhooks",
    request_body = WebhookSettings,
    security(("Bearer" = ["webhooks.create", "webhooks.*", "*"])),
    responses(
        (status = 201, description = "Webhook created successfully", body = WebhookWithMetadataRedactedAuthorization, content_type = "application/json", example = json!({
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "url": "https://your.site/on-tasks-completed",
            "headers": {
                "Authorization": "Bearer a-secret-token"
            },
            "isEditable": true
        })),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json"),
        (status = 400, description = "Bad request", body = ResponseError, content_type = "application/json"),
    )
)]
async fn post_webhook(
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_CREATE }>, Data<IndexScheduler>>,
    webhook_settings: AwebJson<WebhookSettings, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let webhook_settings = webhook_settings.into_inner();
    debug!(parameters = ?webhook_settings, "Post webhook");

    let uuid = Uuid::new_v4();
    if webhook_settings.headers.as_ref().set().is_some_and(|h| h.len() > 200) {
        return Err(TooManyHeaders(uuid).into());
    }

    let mut webhooks = index_scheduler.retrieve_runtime_webhooks();
    if webhooks.len() >= 20 {
        return Err(TooManyWebhooks.into());
    }

    let webhook = Webhook {
        url: webhook_settings.url.set().ok_or(MissingUrl(uuid))?,
        headers: webhook_settings
            .headers
            .set()
            .map(|h| h.into_iter().map(|(k, v)| (k, v.set().unwrap_or_default())).collect())
            .unwrap_or_default(),
    };

    check_changed(uuid, &webhook)?;
    webhooks.insert(uuid, webhook.clone());
    index_scheduler.update_runtime_webhooks(webhooks)?;

    analytics.publish(PostWebhooksAnalytics, &req);

    let response = WebhookWithMetadataRedactedAuthorization::from(uuid, webhook);
    debug!(returns = ?response, "Post webhook");
    Ok(HttpResponse::Created().json(response))
}

/// Update a webhook
///
/// Update an existing webhook's URL or headers.
#[utoipa::path(
    patch,
    path = "/{uuid}",
    tag = "Webhooks",
    request_body = WebhookSettings,
    security(("Bearer" = ["webhooks.update", "webhooks.*", "*"])),
    responses(
        (status = 200, description = "Webhook updated successfully", body = WebhookWithMetadataRedactedAuthorization, content_type = "application/json", example = json!({
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "url": "https://your.site/on-tasks-completed",
            "headers": {
                "Authorization": "Bearer a-secret-token"
            },
            "isEditable": true
        })),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json"),
        (status = 400, description = "Bad request", body = ResponseError, content_type = "application/json"),
    ),
    params(
        ("uuid" = Uuid, Path, description = "The universally unique identifier of the webhook")
    )
)]
async fn patch_webhook(
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_UPDATE }>, Data<IndexScheduler>>,
    uuid: Path<String>,
    webhook_settings: AwebJson<WebhookSettings, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let uuid = Uuid::from_str(&uuid.into_inner()).map_err(InvalidUuid)?;
    let webhook_settings = webhook_settings.into_inner();
    debug!(parameters = ?(uuid, &webhook_settings), "Patch webhook");

    if uuid.is_nil() {
        return Err(ImmutableWebhook(uuid).into());
    }

    let mut webhooks = index_scheduler.retrieve_runtime_webhooks();
    let old_webhook = webhooks.remove(&uuid).ok_or(WebhookNotFound(uuid))?;
    let webhook = patch_webhook_inner(&uuid, old_webhook, webhook_settings)?;

    check_changed(uuid, &webhook)?;
    webhooks.insert(uuid, webhook.clone());
    index_scheduler.update_runtime_webhooks(webhooks)?;

    analytics.publish(PatchWebhooksAnalytics, &req);

    let response = WebhookWithMetadataRedactedAuthorization::from(uuid, webhook);
    debug!(returns = ?response, "Patch webhook");
    Ok(HttpResponse::Ok().json(response))
}

/// Delete a webhook
///
/// Delete an existing webhook by its UUID.
#[utoipa::path(
    delete,
    path = "/{uuid}",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.delete", "webhooks.*", "*"])),
    responses(
        (status = 204, description = "Webhook deleted successfully"),
        (status = 404, description = "Webhook not found", body = ResponseError, content_type = "application/json"),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json"),
    ),
    params(
        ("uuid" = Uuid, Path, description = "The universally unique identifier of the webhook")
    )
)]
async fn delete_webhook(
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_DELETE }>, Data<IndexScheduler>>,
    uuid: Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let uuid = Uuid::from_str(&uuid.into_inner()).map_err(InvalidUuid)?;
    debug!(parameters = ?uuid, "Delete webhook");

    if uuid.is_nil() {
        return Err(ImmutableWebhook(uuid).into());
    }

    let mut webhooks = index_scheduler.retrieve_runtime_webhooks();
    webhooks.remove(&uuid).ok_or(WebhookNotFound(uuid))?;
    index_scheduler.update_runtime_webhooks(webhooks)?;

    debug!(returns = "No Content", "Delete webhook");
    Ok(HttpResponse::NoContent().finish())
}
