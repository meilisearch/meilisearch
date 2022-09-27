use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use index_scheduler::KindWithContent;
use log::debug;
use meilisearch_types::error::ResponseError;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(create_dump))));
}

pub async fn create_dump(
    index_scheduler: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, Data<IndexScheduler>>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish("Dump Created".to_string(), json!({}), Some(&req));

    let task = KindWithContent::DumpExport {
        output: "todo".to_string().into(),
    };
    let res = tokio::task::spawn_blocking(move || index_scheduler.register(task)).await??;

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Accepted().json(res))
}
