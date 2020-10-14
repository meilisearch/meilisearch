use std::fs::File;
use std::path::Path;

use actix_web::{get, post};
use actix_web::{HttpResponse, web};
use serde::{Deserialize, Serialize};

use crate::dump::{DumpInfo, DumpStatus, compressed_dumps_folder, init_dump_process};
use crate::Data;
use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(trigger_dump)
        .service(get_dump_status)
        .service(stream_dump);
}

#[post("/dumps", wrap = "Authentication::Private")]
async fn trigger_dump(
    data: web::Data<Data>,
) -> Result<HttpResponse, ResponseError> {
    let dumps_folder = Path::new(&data.dumps_folder);
    match init_dump_process(&data, &dumps_folder) {
        Ok(resume) => Ok(HttpResponse::Accepted().json(resume)),
        Err(e) => Err(e.into())
    }
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
    let dumps_folder = Path::new(&data.dumps_folder);
    let dump_uid = &path.dump_uid;

    if let Some(resume) = DumpInfo::get_current() {
        if &resume.uid == dump_uid {
            return Ok(HttpResponse::Ok().json(resume));
        }
    }

    if File::open(compressed_dumps_folder(Path::new(dumps_folder), dump_uid)).is_ok() {
        let resume = DumpInfo::new(
            dump_uid.into(),
            DumpStatus::Done
        );

        Ok(HttpResponse::Ok().json(resume))
    } else {
        Err(Error::not_found("dump does not exist").into())
    }
}

use bytes::BytesMut;
use futures::{Future, Stream};
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio::runtime::Runtime;

#[get("/dumps/{dump_uid}", wrap = "Authentication::Private")]
async fn stream_dump(
    data: web::Data<Data>,
    path: web::Path<DumpParam>,
) -> Result<HttpResponse, ResponseError> {
    let dumps_folder = Path::new(&data.dumps_folder);
    let dump_uid = &path.dump_uid;

    let mut file = File::open(compressed_dumps_folder(Path::new(dumps_folder), dump_uid));

    if file.is_ok() {

        FramedRead::new(file, BytesCodec::new())
            .map(BytesMut::freeze) // Map stream of `BytesMut` to stream of `Bytes`
            .concat2()
            .and_then(|bytes| {
                Ok(HttpResponse::Ok().streaming(bytes))
            })
        
    } else {
        Err(Error::not_found("dump does not exist").into())
    }

}