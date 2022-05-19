use actix_web::{web, HttpRequest, HttpResponse};
use log::debug;
use meilisearch_error::ResponseError;
use meilisearch_lib::MeiliSearch;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::task::SummarizedTaskView;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(create_dump))))
        .service(
            web::resource("/{dump_uid}/status").route(web::get().to(SeqHandler(get_dump_status))),
        );
}

pub async fn create_dump(
    meilisearch: GuardedData<ActionPolicy<{ actions::DUMPS_CREATE }>, MeiliSearch>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish("Dump Created".to_string(), json!({}), Some(&req));

    let res: SummarizedTaskView = meilisearch.register_dump_task().await?.into();

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Accepted().json(res))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DumpStatusResponse {
    status: String,
}

#[derive(Deserialize)]
struct DumpParam {
    dump_uid: String,
}

async fn get_dump_status(
    meilisearch: GuardedData<ActionPolicy<{ actions::DUMPS_GET }>, MeiliSearch>,
    path: web::Path<DumpParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!();

    // debug!("returns: {:?}", res);
    // Ok(HttpResponse::Ok().json(res))
}
