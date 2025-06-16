use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
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
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
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

#[utoipa::path(
    get,
    path = "",
    tag = "Export",
    security(("Bearer" = ["export", "*"])),
    responses(
        (status = OK, description = "Known nodes are returned", body = Export, content_type = "application/json", example = json!(
        {
            "indexes": ["movie", "steam-*"],
            "skip_embeddings": true,
            "apiKey": "meilisearch-api-key"
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
    _analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    // TODO make it experimental?
    // index_scheduler.features().check_network("Using the /network route")?;

    let export = export.into_inner();
    debug!(returns = ?export, "Trigger export");

    let Export { url, api_key, indexes } = export;

    let indexes = if indexes.is_empty() {
        BTreeMap::from([(IndexUidPattern::new_unchecked("*"), DbExportIndexSettings::default())])
    } else {
        indexes
            .into_iter()
            .map(|(pattern, ExportIndexSettings { filter })| {
                (pattern, DbExportIndexSettings { filter })
            })
            .collect()
    };

    let task = KindWithContent::Export { url, api_key, indexes };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

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
    #[schema(value_type = Option<BTreeSet<String>>, example = json!(["movies", "steam-*"]))]
    #[deserr(default)]
    #[serde(default)]
    pub indexes: BTreeMap<IndexUidPattern, ExportIndexSettings>,
}

#[derive(Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ExportIndexSettings {
    #[schema(value_type = Option<String>, example = json!("genres = action"))]
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidExportIndexFilter>)]
    pub filter: Option<String>,
}
