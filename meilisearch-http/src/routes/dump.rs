use actix_web::{post, get, web};
use actix_web::HttpResponse;
use serde::{Serialize, Deserialize};

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(trigger_dump)
        .service(get_dump_status);
}

#[post("/dumps", wrap = "Authentication::Private")]
async fn trigger_dump(
    data: web::Data<Data>,
) -> Result<HttpResponse, ResponseError> {
    eprintln!("dump started");
    let res = data.dump().await?;

    Ok(HttpResponse::Ok().body(res))
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

#[get("/dumps/{dump_uid}/status", wrap = "Authentication::Private")]
async fn get_dump_status(
    _data: web::Data<Data>,
    _path: web::Path<DumpParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}
