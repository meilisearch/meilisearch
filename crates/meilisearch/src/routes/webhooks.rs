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
use meilisearch_types::webhooks::{Webhook, Webhooks};
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

#[derive(OpenApi)]
#[openapi(
    paths(get_webhooks, patch_webhooks, get_webhook, post_webhook),
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
            .route(web::patch().to(SeqHandler(patch_webhooks)))
            .route(web::post().to(SeqHandler(post_webhook))),
    )
    .service(web::resource("/{uuid}").route(web::get().to(get_webhook)));
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
struct WebhookSettings {
    #[schema(value_type = Option<String>)]
    #[deserr(default, error = DeserrJsonError<InvalidWebhooksUrl>)]
    #[serde(default)]
    url: Setting<String>,
    #[schema(value_type = Option<BTreeMap<String, String>>, example = json!({"Authorization":"Bearer a-secret-token"}))]
    #[deserr(default, error = DeserrJsonError<InvalidWebhooksHeaders>)]
    #[serde(default)]
    headers: Setting<BTreeMap<String, Setting<String>>>,
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
struct WebhooksSettings {
    #[schema(value_type = Option<BTreeMap<String, WebhookSettings>>)]
    #[serde(default)]
    webhooks: Setting<BTreeMap<Uuid, Setting<WebhookSettings>>>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
struct WebhookWithMetadata {
    uuid: Uuid,
    is_editable: bool,
    #[schema(value_type = WebhookSettings)]
    #[serde(flatten)]
    webhook: Webhook,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebhookResults {
    results: Vec<WebhookWithMetadata>,
}

#[utoipa::path(
    get,
    path = "",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.get", "*.get", "*"])),
    responses(
        (status = OK, description = "Webhooks are returned", body = WebhooksSettings, content_type = "application/json", example = json!({
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
    patch_webhooks_count: usize,
    post_webhook_count: usize,
}

impl PatchWebhooksAnalytics {
    pub fn patch_webhooks() -> Self {
        PatchWebhooksAnalytics { patch_webhooks_count: 1, ..Default::default() }
    }

    pub fn post_webhook() -> Self {
        PatchWebhooksAnalytics { post_webhook_count: 1, ..Default::default() }
    }
}

impl Aggregate for PatchWebhooksAnalytics {
    fn event_name(&self) -> &'static str {
        "Webhooks Updated"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(PatchWebhooksAnalytics {
            patch_webhooks_count: self.patch_webhooks_count + new.patch_webhooks_count,
            post_webhook_count: self.post_webhook_count + new.post_webhook_count,
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
            WebhooksError::MissingUrl(_) => meilisearch_types::error::Code::InvalidWebhooksUrl,
            WebhooksError::TooManyWebhooks => meilisearch_types::error::Code::InvalidWebhooks,
            WebhooksError::TooManyHeaders(_) => {
                meilisearch_types::error::Code::InvalidWebhooksHeaders
            }
            WebhooksError::ReservedWebhook(_) => meilisearch_types::error::Code::ReservedWebhook,
            WebhooksError::WebhookNotFound(_) => meilisearch_types::error::Code::WebhookNotFound,
        }
    }
}

#[utoipa::path(
    patch,
    path = "",
    tag = "Webhooks",
    request_body = WebhooksSettings,
    security(("Bearer" = ["webhooks.update", "*"])),
    responses(
        (status = 200, description = "Returns the updated webhooks", body = WebhooksSettings, content_type = "application/json", example = json!({
            "webhooks": {
                "550e8400-e29b-41d4-a716-446655440000": {
                    "url": "http://example.com/webhook",
                },
                "550e8400-e29b-41d4-a716-446655440001": {
                    "url": "https://your.site/on-tasks-completed",
                    "headers": {
                        "Authorization": "Bearer a-secret-token"
                    }
                }
            }
        })),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        })),
    )
)]
async fn patch_webhooks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::WEBHOOKS_UPDATE }>, Data<IndexScheduler>>,
    new_webhooks: AwebJson<WebhooksSettings, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let webhooks = patch_webhooks_inner(&index_scheduler, new_webhooks.0)?;

    analytics.publish(PatchWebhooksAnalytics::patch_webhooks(), &req);

    Ok(HttpResponse::Ok().json(webhooks))
}

fn patch_webhooks_inner(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::WEBHOOKS_UPDATE }>, Data<IndexScheduler>>,
    new_webhooks: WebhooksSettings,
) -> Result<Webhooks, ResponseError> {
    fn merge_webhook(
        uuid: &Uuid,
        old_webhook: Option<Webhook>,
        new_webhook: WebhookSettings,
    ) -> Result<Webhook, WebhooksError> {
        let (old_url, mut headers) =
            old_webhook.map(|w| (Some(w.url), w.headers)).unwrap_or((None, BTreeMap::new()));

        let url = match new_webhook.url {
            Setting::Set(url) => url,
            Setting::NotSet => old_url.ok_or_else(|| WebhooksError::MissingUrl(uuid.to_owned()))?,
            Setting::Reset => return Err(WebhooksError::MissingUrl(uuid.to_owned())),
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
            return Err(WebhooksError::TooManyHeaders(uuid.to_owned()));
        }

        Ok(Webhook { url, headers })
    }

    debug!(parameters = ?new_webhooks, "Patch webhooks");

    let Webhooks { mut webhooks } = index_scheduler.webhooks();

    match new_webhooks.webhooks {
        Setting::Set(new_webhooks) => {
            for (uuid, new_webhook) in new_webhooks {
                if uuid.is_nil() {
                    return Err(WebhooksError::ReservedWebhook(uuid).into());
                }

                match new_webhook {
                    Setting::Set(new_webhook) => {
                        let old_webhook = webhooks.remove(&uuid);
                        let webhook = merge_webhook(&uuid, old_webhook, new_webhook)?;
                        webhooks.insert(uuid, webhook);
                    }
                    Setting::Reset => {
                        webhooks.remove(&uuid);
                    }
                    Setting::NotSet => (),
                }
            }
        }
        Setting::Reset => webhooks.clear(),
        Setting::NotSet => (),
    };

    if webhooks.len() > 20 {
        return Err(WebhooksError::TooManyWebhooks.into());
    }

    let webhooks = Webhooks { webhooks };
    index_scheduler.put_webhooks(webhooks.clone())?;

    debug!(returns = ?webhooks, "Patch webhooks");

    Ok(webhooks)
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

    let webhook = webhooks.webhooks.remove(&uuid).ok_or(WebhooksError::WebhookNotFound(uuid))?;

    debug!(returns = ?webhook, "Get webhook {}", uuid);
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
    let uuid = Uuid::new_v4();

    let webhooks = patch_webhooks_inner(
        &index_scheduler,
        WebhooksSettings {
            webhooks: Setting::Set(BTreeMap::from([(uuid, Setting::Set(webhook_settings.0))])),
        },
    )?;
    let webhook = webhooks.webhooks.get(&uuid).ok_or(WebhooksError::WebhookNotFound(uuid))?.clone();

    analytics.publish(PatchWebhooksAnalytics::post_webhook(), &req);

    debug!(returns = ?webhook, "Created webhook {}", uuid);
    Ok(HttpResponse::Created().json(WebhookWithMetadata { uuid, is_editable: true, webhook }))
}
