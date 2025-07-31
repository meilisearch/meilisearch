use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidWebhooks, InvalidWebhooksHeaders, InvalidWebhooksUrl,
};
use meilisearch_types::error::{ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::webhooks::{Webhook, Webhooks};
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

#[derive(OpenApi)]
#[openapi(
    paths(get_webhooks, patch_webhooks),
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
            .route(web::patch().to(SeqHandler(patch_webhooks))),
    );
}

#[utoipa::path(
    get,
    path = "",
    tag = "Webhooks",
    security(("Bearer" = ["webhooks.get", "*.get", "*"])),
    responses(
        (status = OK, description = "Webhooks are returned", body = WebhooksSettings, content_type = "application/json", example = json!({
            "webhooks": {
                "name": {
                    "url": "http://example.com/webhook",
                },
                "anotherName": {
                    "url": "https://your.site/on-tasks-completed",
                    "headers": {
                        "Authorization": "Bearer a-secret-token"
                    }
                }
            }
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
    debug!(returns = ?webhooks, "Get webhooks");
    Ok(HttpResponse::Ok().json(webhooks))
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidWebhooks>, rename_all = camelCase, deny_unknown_fields)]
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
    #[deserr(default, error = DeserrJsonError<InvalidWebhooks>)]
    #[serde(default)]
    webhooks: Setting<BTreeMap<String, Setting<WebhookSettings>>>,
}

#[derive(Serialize)]
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

#[derive(Debug, thiserror::Error)]
enum WebhooksError {
    #[error("The URL for the webhook `{0}` is missing.")]
    MissingUrl(String),
    #[error("Defining too many webhooks would crush the server. Please limit the number of webhooks to 20. You may use a third-party proxy server to dispatch events to more than 20 endpoints.")]
    TooManyWebhooks,
    #[error("Too many headers for the webhook `{0}`. Please limit the number of headers to 200.")]
    TooManyHeaders(String),
    #[error("Cannot edit webhook `{0}`. Webhooks prefixed with an underscore are reserved and may not be modified using the API.")]
    ReservedWebhook(String),
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
                "name": {
                    "url": "http://example.com/webhook",
                },
                "anotherName": {
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

    analytics.publish(PatchWebhooksAnalytics, &req);

    Ok(HttpResponse::Ok().json(webhooks))
}

fn patch_webhooks_inner(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::WEBHOOKS_UPDATE }>, Data<IndexScheduler>>,
    new_webhooks: WebhooksSettings,
) -> Result<Webhooks, ResponseError> {
    fn merge_webhook(
        name: &str,
        old_webhook: Option<Webhook>,
        new_webhook: WebhookSettings,
    ) -> Result<Webhook, WebhooksError> {
        let (old_url, mut headers) =
            old_webhook.map(|w| (Some(w.url), w.headers)).unwrap_or((None, BTreeMap::new()));

        let url = match new_webhook.url {
            Setting::Set(url) => url,
            Setting::NotSet => old_url.ok_or_else(|| WebhooksError::MissingUrl(name.to_owned()))?,
            Setting::Reset => return Err(WebhooksError::MissingUrl(name.to_owned())),
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
            return Err(WebhooksError::TooManyHeaders(name.to_owned()));
        }

        Ok(Webhook { url, headers })
    }

    debug!(parameters = ?new_webhooks, "Patch webhooks");

    let Webhooks { mut webhooks } = index_scheduler.webhooks();

    match new_webhooks.webhooks {
        Setting::Set(new_webhooks) => {
            for (name, new_webhook) in new_webhooks {
                if name.starts_with('_') {
                    return Err(WebhooksError::ReservedWebhook(name).into());
                }

                match new_webhook {
                    Setting::Set(new_webhook) => {
                        let old_webhook = webhooks.remove(&name);
                        let webhook = merge_webhook(&name, old_webhook, new_webhook)?;
                        webhooks.insert(name.clone(), webhook);
                    }
                    Setting::Reset => {
                        webhooks.remove(&name);
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
