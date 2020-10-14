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

use bytes::Bytes;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::io::BufReader;
use std::io::Read;
use futures_util::ready;
use futures_core::Future;

struct StreamBody {
    reader: BufReader<File>,
    buffer: Vec<u8>,
    delay: actix_rt::time::Delay,
}

impl StreamBody {
    fn new(file: File, chunk_size: usize) -> Self {
        let mut reader = BufReader::new(file);
        let mut buffer = vec![0u8; chunk_size];
        StreamBody {
            reader,
            buffer,
            delay: actix_rt::time::delay_for(std::time::Duration::from_millis(10)),
        }
    }
}

impl futures_core::stream::Stream for StreamBody {
    type Item = Result<Bytes, ResponseError>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        ready!(Pin::new(&mut self.delay).poll(cx));
        self.delay = actix_rt::time::delay_for(std::time::Duration::from_millis(10));
        match self.reader.read(&mut self.buffer) {
            Ok(count) => {
                if count > 0 {
                    Poll::Ready(Some(Ok(Bytes::from(&self.buffer[..count]))))
                } else {
                    Poll::Ready(None)
                }
            },
            Err(e) => Poll::Ready(None)
        }
    }
}

#[get("/dumps/{dump_uid}", wrap = "Authentication::Private")]
async fn stream_dump(
    data: web::Data<Data>,
    path: web::Path<DumpParam>,
) -> Result<HttpResponse, ResponseError> {
    let dumps_folder = Path::new(&data.dumps_folder);
    let dump_uid = &path.dump_uid;

    match File::open(compressed_dumps_folder(Path::new(dumps_folder), dump_uid)) {
        Ok(file) => Ok(HttpResponse::Ok().streaming(StreamBody::new(file, 1024))),
        Err(e) => Err(Error::not_found("dump does not exist").into())
    }

}