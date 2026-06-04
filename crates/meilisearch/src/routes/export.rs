use std::collections::BTreeMap;
use std::convert::Infallible;
use std::str::FromStr as _;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use byte_unit::Byte;
use deserr::actix_web::AwebJson;
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

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::export_analytics::ExportAnalytics;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[routes::routes(
    routes(
        "" => post(export),
    ),
    tag = "Export",
)]
pub struct ExportApi;

/// Export to a remote Meilisearch
///
/// Trigger an export that sends documents and settings from this instance to a remote Meilisearch server. Configure the remote URL and optional API key in the request body.
#[routes::path(
    request_body = Export,
    security(("Bearer" = ["export", "*"])),
    responses(
        (status = 202, description = "Export successfully enqueued.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 1,
                "status": "enqueued",
                "type": "export",
                "enqueuedAt": "2021-08-11T09:25:53.000000Z"
            })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
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

    // FIXME: This should be 202 Accepted, but changing would be breaking so we need to wait 2.0
    Ok(HttpResponse::Ok().json(task))
}

/// Request body for exporting data to a remote Meilisearch instance
#[routes::request]
#[derive(Debug)]
pub struct Export {
    /// URL of the destination Meilisearch instance
    #[request(default, error = DeserrJsonError<InvalidExportUrl>, example = json!("https://ms-1234.heaven.meilisearch.com"))]
    pub url: String,
    /// API key for authenticating with the destination instance
    #[request(default, error = DeserrJsonError<InvalidExportApiKey>, example = json!("1234abcd"))]
    pub api_key: Option<String>,
    /// Maximum payload size per request
    #[request(default, error = DeserrJsonError<InvalidExportPayloadSize>, example = json!("24MiB"), schema_type = Option<String>)]
    pub payload_size: Option<ByteWithDeserr>,
    /// Index patterns to export with their settings
    #[request(default, example = json!({ "*": { "filter": null } }), schema_type = Option<BTreeMap<String, ExportIndexSettings>>)]
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

/// Export settings for a specific index
#[routes::request]
#[derive(Debug)]
pub struct ExportIndexSettings {
    /// Filter expression to select which documents to export
    #[request(default, error = DeserrJsonError<InvalidExportIndexFilter>, schema_type = Option<String>, example = json!("genres = action"))]
    pub filter: Option<Value>,
    /// Whether to override settings on the destination index
    #[request(default, error = DeserrJsonError<InvalidExportIndexOverrideSettings>, schema_type = Option<bool>, example = json!(true))]
    pub override_settings: bool,
}
