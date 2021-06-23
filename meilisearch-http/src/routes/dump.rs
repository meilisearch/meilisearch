use actix_web::HttpResponse;
use actix_web::{get, post, web};
use log::debug;
use serde::{Deserialize, Serialize};

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(create_dump).service(get_dump_status);
}

#[post("/dumps", wrap = "Authentication::Private")]
async fn create_dump(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
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

#[get("/dumps/{dump_uid}/status", wrap = "Authentication::Private")]
async fn get_dump_status(
    data: web::Data<Data>,
    path: web::Path<DumpParam>,
) -> Result<HttpResponse, ResponseError> {
    let res = data.dump_status(path.dump_uid.clone()).await?;

    debug!("returns: {:?}", res);
    Ok(HttpResponse::Ok().json(res))
}
