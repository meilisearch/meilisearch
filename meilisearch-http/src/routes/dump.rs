use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::KindWithContent;
use log::debug;
use meilisearch_lib::MeiliSearch;
use meilisearch_types::error::ResponseError;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(create_dump))));
}

pub async fn create_dump(
    meilisearch: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, MeiliSearch>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish("Dump Created".to_string(), json!({}), Some(&req));

    let task = KindWithContent::DumpExport {
        output: "toto".to_string().into(),
    };
    let res = meilisearch.register_task(task).await?;

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Accepted().json(res))
}
