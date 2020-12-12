use std::fs::File;
use std::path::Path;

use actix_web::{get, post};
use actix_web::{HttpResponse, web};
use serde::{Deserialize, Serialize};

use crate::dump::{DumpInfo, DumpStatus, compressed_dumps_dir, init_dump_process};
use crate::Data;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(trigger_dump)
        .service(get_dump_status);
}

#[post("/dumps", wrap = "Authentication::Private")]
async fn trigger_dump(
    data: web::Data<Data>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
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
    todo!()
}
