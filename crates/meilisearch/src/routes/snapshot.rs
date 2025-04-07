use actix_web::web::Data;
use actix_web::{web, FromRequest, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::KindWithContent;
use tracing::debug;
use utoipa::OpenApi;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::Opt;

#[derive(OpenApi)]
#[openapi(
    paths(create_snapshot),
    tags((
        name = "Snapshots",
        description = "The snapshots route allows the creation of database snapshots. Snapshots are .snapshot files that can be used to launch Meilisearch.
Creating a snapshot is also referred to as exporting it, whereas launching Meilisearch with a snapshot is referred to as importing it.
During a snapshot export, all indexes of the current instance are exported—together with their documents and settings—and saved as a single .snapshot file.
During a snapshot import, all indexes contained in the indicated .snapshot file are imported along with their associated documents and settings.
Snapshot imports are performed at launch using an option.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/snapshots"),
    )),
)]
pub struct SnapshotApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(create_snapshot))));
}

crate::empty_analytics!(SnapshotAnalytics, "Snapshot Created");

/// Create a snapshot
///
/// Triggers a snapshot creation process. Once the process is complete, a snapshot is created in the snapshot directory. If the snapshot directory does not exist yet, it will be created.
#[utoipa::path(
    post,
    path = "",
    tag = "Snapshots",
    security(("Bearer" = ["snapshots.create", "snapshots.*", "*"])),
    request_body = SnapshotOptions,
    responses(
        (status = 202, description = "Snapshot is being created", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 0,
                "indexUid": null,
                "status": "enqueued",
                "type": "snapshotCreation",
                "enqueuedAt": "2021-01-01T09:39:00.000000Z"
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
pub async fn create_snapshot(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SNAPSHOTS_CREATE }>, Data<IndexScheduler>>,
    snapshot_options: Option<actix_web::web::Bytes>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(SnapshotAnalytics::default(), &req);

    let task = match snapshot_options {
        Some(snapshot_options) if !snapshot_options.is_empty() => {
            let mut payload = actix_web::dev::Payload::from(snapshot_options);
            let snapshot_options: AwebJson<SnapshotOptions, DeserrJsonError> =
                match AwebJson::from_request(&req, &mut payload).await {
                    Ok(snapshot_options) => snapshot_options,
                    Err(error) => {
                        return Err(ResponseError::from_msg(format!("{error}\n  - note: POST /snapshots without a body to use default parameters"), meilisearch_types::error::Code::InvalidSnapshotOptions));
                    }
                };
            let SnapshotOptions { compaction, compression } = snapshot_options.into_inner();

            KindWithContent::SnapshotCreationWithParams { compaction, compression }
        }
        _ => KindWithContent::SnapshotCreationWithParams { compaction: false, compression: true },
    };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();

    debug!(returns = ?task, "Create snapshot");
    Ok(HttpResponse::Accepted().json(task))
}

#[derive(Clone, Copy, deserr::Deserr, utoipa::ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
struct SnapshotOptions {
    #[deserr(default)]
    compaction: bool,
    #[deserr(default)]
    compression: bool,
}
