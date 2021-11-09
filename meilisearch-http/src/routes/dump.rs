use actix_web::{web, HttpRequest, HttpResponse};
use meilisearch_lib::MeiliSearch;
use serde::{Deserialize, Serialize};

use crate::analytics::Analytics;
use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(create_dump)))
        .service(web::resource("/{dump_uid}/status").route(web::get().to(get_dump_status)));
}

pub async fn create_dump(
    _meilisearch: GuardedData<Private, MeiliSearch>,
    _req: HttpRequest,
    _analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    // analytics.publish("Dump Created".to_string(), json!({}), Some(&req));

    // let res = meilisearch.create_dump().await?;

    // debug!("returns: {:?}", res);
    // Ok(HttpResponse::Accepted().json(res))
    todo!()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DumpStatusResponse {
    status: String,
}

#[derive(Deserialize)]
struct DumpParam {
    _dump_uid: String,
}

async fn get_dump_status(
    _meilisearch: GuardedData<Private, MeiliSearch>,
    _path: web::Path<DumpParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
    // let res = meilisearch.dump_info(path.dump_uid.clone()).await?;

    // debug!("returns: {:?}", res);
    // Ok(HttpResponse::Ok().json(res))
}
