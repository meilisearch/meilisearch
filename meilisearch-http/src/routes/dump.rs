use actix_web::{web, HttpResponse};
use log::debug;
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("/dumps").route(web::post().to(create_dump)))
        .service(web::resource("/dumps/{dump_uid}/status").route(web::get().to(get_dump_status)));
}

async fn create_dump(data: GuardedData<Private, Data>) -> Result<HttpResponse, ResponseError> {
    let res = data.create_dump().await?;

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
    data: GuardedData<Private, Data>,
    path: web::Path<DumpParam>,
) -> Result<HttpResponse, ResponseError> {
    let res = data.dump_status(path.dump_uid.clone()).await?;

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Ok().json(res))
}
