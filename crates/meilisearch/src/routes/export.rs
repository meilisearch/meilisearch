use std::collections::BTreeMap;
use std::convert::Infallible;
use std::str::FromStr as _;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use byte_unit::Byte;
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::keys::actions;
use meilisearch_types::tasks::{ExportIndexSettings as DbExportIndexSettings, KindWithContent};
use serde::Serialize;
use serde_json::Value;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::export_analytics::ExportAnalytics;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[derive(OpenApi)]
#[openapi(
    paths(export),
    tags((
        name = "Export",
        description = "The `/export` route allows you to trigger an export process to a remote Meilisearch instance.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/export"),
    )),
)]
pub struct ExportApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(export)));
}

/// Export to a remote Meilisearch
///
/// Triggers an export process to a remote Meilisearch instance. This allows you to send
/// documents and settings from the current instance to another Meilisearch server.
#[utoipa::path(
    post,
    path = "",
    tag = "Export",
    security(("Bearer" = ["export", "*"])),
    responses(
        (status = 202, description = "Export successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 1,
                "status": "enqueued",
                "type": "export",
                "enqueuedAt": "2021-08-11T09:25:53.000000Z"
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
async fn export(
    index_scheduler: GuardedData<ActionPolicy<{ actions::EXPORT }>, Data<IndexScheduler>>,
    export: AwebJson<Export, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let export = export.into_inner();
    debug!(returns = ?export, "Trigger export");

    let analytics_aggregate = ExportAnalytics::from_export(&export);

    let Export { url, api_key, payload_size, indexes } = export;

    let indexes = match indexes {
        Some(indexes) => indexes
            .into_iter()
            .map(|(pattern, ExportIndexSettings { filter, override_settings })| {
                (pattern, DbExportIndexSettings { filter, override_settings })
            })
            .collect(),
        None => BTreeMap::from([(
            IndexUidPattern::new_unchecked("*"),
            DbExportIndexSettings::default(),
        )]),
    };

    let task = KindWithContent::Export {
        url,
        api_key,
        payload_size: payload_size.map(|ByteWithDeserr(bytes)| bytes),
        indexes,
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    analytics.publish(analytics_aggregate, &req);

    Ok(HttpResponse::Ok().json(task))
}

#[derive(Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct Export {
    #[schema(value_type = Option<String>, example = json!("https://ms-1234.heaven.meilisearch.com"))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidExportUrl>)]
    pub url: String,
    #[schema(value_type = Option<String>, example = json!("1234abcd"))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidExportApiKey>)]
    pub api_key: Option<String>,
    #[schema(value_type = Option<String>, example = json!("24MiB"))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidExportPayloadSize>)]
    pub payload_size: Option<ByteWithDeserr>,
    #[schema(value_type = Option<BTreeMap<String, ExportIndexSettings>>, example = json!({ "*": { "filter": null } }))]
    #[deserr(default)]
    #[serde(default)]
    pub indexes: Option<BTreeMap<IndexUidPattern, ExportIndexSettings>>,
}

/// A wrapper around the `Byte` type that implements `Deserr`.
#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct ByteWithDeserr(pub Byte);

impl<E> deserr::Deserr<E> for ByteWithDeserr
where
    E: deserr::DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        use deserr::{ErrorKind, Value, ValueKind};
        match value {
            Value::Integer(integer) => Ok(ByteWithDeserr(Byte::from_u64(integer))),
            Value::String(string) => Byte::from_str(&string).map(ByteWithDeserr).map_err(|e| {
                deserr::take_cf_content(E::error::<Infallible>(
                    None,
                    ErrorKind::Unexpected { msg: e.to_string() },
                    location,
                ))
            }),
            actual => Err(deserr::take_cf_content(E::error(
                None,
                ErrorKind::IncorrectValueKind {
                    actual,
                    accepted: &[ValueKind::Integer, ValueKind::String],
                },
                location,
            ))),
        }
    }
}

#[derive(Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ExportIndexSettings {
    #[schema(value_type = Option<String>, example = json!("genres = action"))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidExportIndexFilter>)]
    pub filter: Option<Value>,
    #[schema(value_type = Option<bool>, example = json!(true))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidExportIndexOverrideSettings>)]
    pub override_settings: bool,
}
