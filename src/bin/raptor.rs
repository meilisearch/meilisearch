extern crate env_logger;
extern crate fst;
extern crate fst_levenshtein;
extern crate futures;
#[macro_use]  extern crate lazy_static;
extern crate raptor;
extern crate tokio_minihttp;
extern crate tokio_proto;
extern crate tokio_service;
extern crate url;

use std::io;
use std::path::Path;
use std::fs::File;
use std::io::{Read, BufReader};

use fst_levenshtein::Levenshtein;
use fst::{IntoStreamer, Streamer};
use futures::future;
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::Service;

use raptor::MultiMap;

lazy_static! {
    static ref MAP: MultiMap = {
        let map = read_to_vec("map.fst").unwrap();
        let values = read_to_vec("values.vecs").unwrap();

        MultiMap::from_bytes(map, &values).unwrap()
    };
}

struct MainService {
    map: &'static MultiMap,
}

impl Service for MainService {
    type Request = Request;
    type Response = Response;
    type Error = io::Error;
    type Future = future::Ok<Response, io::Error>;

    fn call(&self, request: Request) -> Self::Future {

        let url = format!("http://raptor.net{}", request.path());
        let url = url::Url::parse(&url).unwrap();

        let mut resp = Response::new();
        resp.header("Content-Type", "text/html");
        resp.header("charset", "utf-8");

        if let Some((_, key)) = url.query_pairs().find(|&(ref k, _)| k == "q") {
            let key = key.to_lowercase();

            let lev = if key.len() <= 8 {
                Levenshtein::new(&key, 1).unwrap()
            } else {
                Levenshtein::new(&key, 2).unwrap()
            };

            let mut body = String::new();
            body.push_str("<html><body>");

            let mut stream = self.map.search(lev).into_stream();
            while let Some((key, values)) = stream.next() {
                let values = &values[..values.len().min(10)];
                body.push_str(&format!("{:?} {:?}</br>", key, values));
            }

            body.push_str("</body></html>");
            resp.body(&body);
        }

        future::ok(resp)
    }
}

fn read_to_vec<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let mut file = BufReader::new(file);

    let mut vec = Vec::new();
    file.read_to_end(&mut vec)?;

    Ok(vec)
}

fn main() {
    drop(env_logger::init());
    let addr = "0.0.0.0:8080".parse().unwrap();

    TcpServer::new(Http, addr).serve(|| Ok(MainService { map: &MAP }))
}
