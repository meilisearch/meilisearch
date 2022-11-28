use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use log::debug;
use meilisearch_auth::AuthController;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::KindWithContent;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::SummarizedTaskView;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(create_dump))));
}

pub async fn create_dump(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, Data<IndexScheduler>>,
    auth_controller: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, AuthController>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish("Dump Created".to_string(), json!({}), Some(&req));

    let task = KindWithContent::DumpCreation {
        keys: auth_controller.list_keys()?,
        instance_uid: analytics.instance_uid().cloned(),
    };
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??.into();

    debug!("returns: {:?}", task);
    Ok(HttpResponse::Accepted().json(task))
}
