use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::KindWithContent;
use serde_json::json;
use tracing::debug_span;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::SummarizedTaskView;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(create_snapshot))));
}

pub async fn create_snapshot(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SNAPSHOTS_CREATE }>, Data<IndexScheduler>>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish("Snapshot Created".to_string(), json!({}), Some(&req));

    let task = KindWithContent::SnapshotCreation;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    debug_span!("Create snapshot", returns = ?task);
    Ok(HttpResponse::Accepted().json(task))
}
