use std::collections::BTreeMap;

use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{InvalidWebhooksHeaders, InvalidWebhooksUrl};
use meilisearch_types::error::{ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::webhooks::Webhook;
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use WebhooksError::*;

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
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub(super) struct WebhookSettings {
    #[schema(value_type = Option<String>)]
    #[deserr(default, error = DeserrJsonError<InvalidWebhooksUrl>)]
    #[serde(default)]
    url: Setting<String>,
    #[schema(value_type = Option<BTreeMap<String, String>>, example = json!({"Authorization":"Bearer a-secret-token"}))]
    #[deserr(default, error = DeserrJsonError<InvalidWebhooksHeaders>)]
    #[serde(default)]
    headers: Setting<BTreeMap<String, Setting<String>>>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub(super) struct WebhookWithMetadata {
    uuid: Uuid,
    is_editable: bool,
    #[schema(value_type = WebhookSettings)]
    #[serde(flatten)]
    webhook: Webhook,
}

impl WebhookWithMetadata {
    pub fn from(uuid: Uuid, webhook: Webhook) -> Self {
        Self { uuid, is_editable: uuid != Uuid::nil(), webhook }
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(super) struct WebhookResults {
    results: Vec<WebhookWithMetadata>,
}

#[utoipa::path(
    get,
    path = "",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.get", "*.get", "*"])),
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
    let webhooks = index_scheduler.webhooks();
    let results = webhooks
        .webhooks
        .into_iter()
        .map(|(uuid, webhook)| WebhookWithMetadata {
            uuid,
            is_editable: uuid != Uuid::nil(),
            webhook,
        })
        .collect::<Vec<_>>();
    let results = WebhookResults { results };
    debug!(returns = ?results, "Get webhooks");
    Ok(HttpResponse::Ok().json(results))
}

#[derive(Serialize, Default)]
pub struct PatchWebhooksAnalytics {
    patch_webhook_count: usize,
    post_webhook_count: usize,
    delete_webhook_count: usize,
}

impl PatchWebhooksAnalytics {
    pub fn patch_webhook() -> Self {
        PatchWebhooksAnalytics { patch_webhook_count: 1, ..Default::default() }
    }

    pub fn post_webhook() -> Self {
        PatchWebhooksAnalytics { post_webhook_count: 1, ..Default::default() }
    }

    pub fn delete_webhook() -> Self {
        PatchWebhooksAnalytics { delete_webhook_count: 1, ..Default::default() }
    }
}

impl Aggregate for PatchWebhooksAnalytics {
    fn event_name(&self) -> &'static str {
        "Webhooks Updated"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(PatchWebhooksAnalytics {
            patch_webhook_count: self.patch_webhook_count + new.patch_webhook_count,
            post_webhook_count: self.post_webhook_count + new.post_webhook_count,
            delete_webhook_count: self.delete_webhook_count + new.delete_webhook_count,
        })
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
    #[error("Too many headers for the webhook `{0}`. Please limit the number of headers to 200.")]
    TooManyHeaders(Uuid),
    #[error("Cannot edit webhook `{0}`. Webhooks prefixed with an underscore are reserved and may not be modified using the API.")]
    ReservedWebhook(Uuid),
    #[error("Webhook `{0}` not found.")]
    WebhookNotFound(Uuid),
}

impl ErrorCode for WebhooksError {
    fn error_code(&self) -> meilisearch_types::error::Code {
        match self {
            MissingUrl(_) => meilisearch_types::error::Code::InvalidWebhooksUrl,
            TooManyWebhooks => meilisearch_types::error::Code::InvalidWebhooks,
            TooManyHeaders(_) => meilisearch_types::error::Code::InvalidWebhooksHeaders,
            ReservedWebhook(_) => meilisearch_types::error::Code::ReservedWebhook,
            WebhookNotFound(_) => meilisearch_types::error::Code::WebhookNotFound,
        }
    }
}

fn patch_webhook_inner(
    uuid: &Uuid,
    old_webhook: Option<Webhook>,
    new_webhook: WebhookSettings,
) -> Result<Webhook, WebhooksError> {
    let (old_url, mut headers) =
        old_webhook.map(|w| (Some(w.url), w.headers)).unwrap_or((None, BTreeMap::new()));

    let url = match new_webhook.url {
        Setting::Set(url) => url,
        Setting::NotSet => old_url.ok_or_else(|| MissingUrl(uuid.to_owned()))?,
        Setting::Reset => return Err(MissingUrl(uuid.to_owned())),
    };

    let headers = match new_webhook.headers {
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
            headers
        }
        Setting::NotSet => headers,
        Setting::Reset => BTreeMap::new(),
    };

    if headers.len() > 200 {
        return Err(TooManyHeaders(uuid.to_owned()));
    }

    Ok(Webhook { url, headers })
}

#[utoipa::path(
    get,
    path = "/{uuid}",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.get", "*.get", "*"])),
    responses(
        (status = 200, description = "Webhook found", body = WebhookSettings, content_type = "application/json", example = json!({
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
    uuid: Path<Uuid>,
) -> Result<HttpResponse, ResponseError> {
    let uuid = uuid.into_inner();
    let mut webhooks = index_scheduler.webhooks();

    let webhook = webhooks.webhooks.remove(&uuid).ok_or(WebhookNotFound(uuid))?;

    Ok(HttpResponse::Ok().json(WebhookWithMetadata {
        uuid,
        is_editable: uuid != Uuid::nil(),
        webhook,
    }))
}

#[utoipa::path(
    post,
    path = "",
    tag = "Webhooks",
    request_body = WebhookSettings,
    security(("Bearer" = ["webhooks.update", "*"])),
    responses(
        (status = 201, description = "Webhook created successfully", body = WebhookWithMetadata, content_type = "application/json", example = json!({
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
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_UPDATE }>, Data<IndexScheduler>>,
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

    let mut webhooks = index_scheduler.webhooks();
    if dbg!(webhooks.webhooks.len() >= 20) {
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
    webhooks.webhooks.insert(uuid, webhook.clone());
    index_scheduler.put_webhooks(webhooks)?;

    analytics.publish(PatchWebhooksAnalytics::post_webhook(), &req);

    let response = WebhookWithMetadata::from(uuid, webhook);
    debug!(returns = ?response, "Post webhook");
    Ok(HttpResponse::Created().json(response))
}

#[utoipa::path(
    patch,
    path = "/{uuid}",
    tag = "Webhooks",
    request_body = WebhookSettings,
    security(("Bearer" = ["webhooks.update", "*"])),
    responses(
        (status = 200, description = "Webhook updated successfully", body = WebhookWithMetadata, content_type = "application/json", example = json!({
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
    uuid: Path<Uuid>,
    webhook_settings: AwebJson<WebhookSettings, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let uuid = uuid.into_inner();
    let webhook_settings = webhook_settings.into_inner();
    debug!(parameters = ?(uuid, &webhook_settings), "Patch webhook");

    if uuid.is_nil() {
        return Err(ReservedWebhook(uuid).into());
    }

    let mut webhooks = index_scheduler.webhooks();
    let old_webhook = webhooks.webhooks.remove(&uuid);
    let webhook = patch_webhook_inner(&uuid, old_webhook, webhook_settings)?;

    if webhook.headers.len() > 200 {
        return Err(TooManyHeaders(uuid).into());
    }

    webhooks.webhooks.insert(uuid, webhook.clone());
    index_scheduler.put_webhooks(webhooks)?;

    analytics.publish(PatchWebhooksAnalytics::patch_webhook(), &req);

    let response = WebhookWithMetadata::from(uuid, webhook);
    debug!(returns = ?response, "Patch webhook");
    Ok(HttpResponse::Ok().json(response))
}

#[utoipa::path(
    delete,
    path = "/{uuid}",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.update", "*"])),
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
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_UPDATE }>, Data<IndexScheduler>>,
    uuid: Path<Uuid>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let uuid = uuid.into_inner();
    debug!(parameters = ?uuid, "Delete webhook");

    if uuid.is_nil() {
        return Err(ReservedWebhook(uuid).into());
    }

    let mut webhooks = index_scheduler.webhooks();
    webhooks.webhooks.remove(&uuid).ok_or(WebhookNotFound(uuid))?;
    index_scheduler.put_webhooks(webhooks)?;

    analytics.publish(PatchWebhooksAnalytics::delete_webhook(), &req);

    debug!(returns = "No Content", "Delete webhook");
    Ok(HttpResponse::NoContent().finish())
}
