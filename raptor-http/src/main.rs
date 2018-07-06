extern crate env_logger;
extern crate rocksdb;
extern crate fst;
extern crate futures;
extern crate raptor;
extern crate tokio_minihttp;
extern crate tokio_proto;
extern crate tokio_service;
extern crate url;

use std::{io, fs};
use std::sync::Arc;

use fst::Streamer;
use futures::future;
use rocksdb::{DB, DBOptions};
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::Service;

use raptor::{DocIndexMap, RankedStream, LevBuilder};

struct MainService {
    map: Arc<DocIndexMap>,
    lev_builder: Arc<LevBuilder>,
    db: Arc<DB>,
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

        if let Some((_, query)) = url.query_pairs().find(|&(ref k, _)| k == "q") {
            let query = query.to_lowercase();

            let mut automatons = Vec::new();

            for query in query.split_whitespace() {
                let lev = self.lev_builder.get_automaton(query);
                automatons.push(lev);
            }

            let mut limit = 20;
            let mut stream = RankedStream::new(&self.map, self.map.values(), automatons.clone(), 20);

            let mut body = String::new();
            body.push_str("<html><body>");

            while let Some(document_id) = stream.next() {
                if limit == 0 { break }

                body.push_str(&format!("<p>{:?}</p>", document_id));

                limit -= 1;
            }

            body.push_str("</body></html>");

            resp.body_vec(body.into_bytes());
        }

        future::ok(resp)
    }
}

fn main() {
    drop(env_logger::init());

    let addr = "0.0.0.0:8080".parse().unwrap();

    let lev_builder = Arc::new(LevBuilder::new());
    let map = {
        let fst = fs::read("map.fst").unwrap();
        let values = fs::read("values.vecs").unwrap();
        let map = DocIndexMap::from_bytes(fst, &values).unwrap();
        Arc::new(map)
    };

    let db = {
        let opts = DBOptions::new();
        let error_if_log_file_exist = false;
        let db = DB::open_for_read_only(opts, "rocksdb/storage", error_if_log_file_exist).unwrap();
        Arc::new(db)
    };

    TcpServer::new(Http, addr).serve(move || Ok(MainService {
        map: map.clone(),
        lev_builder: lev_builder.clone(),
        db: db.clone(),
    }))
}
