extern crate env_logger;
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
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::Service;

use raptor::{Map, OpWithStateBuilder, LevBuilder};

struct MainService {
    map: Arc<Map<u64>>,
    lev_builder: Arc<LevBuilder>,
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
                let lev = self.lev_builder.build_automaton(query);
                automatons.push(lev);
            }

            let mut op = OpWithStateBuilder::new(self.map.values());

            for automaton in automatons.iter().cloned() {
                let stream = self.map.as_map().search(automaton).with_state();
                op.push(stream);
            }

            let mut stream = op.union();

            let mut body = String::new();
            body.push_str("<html><body>");

            while let Some((key, ivalues)) = stream.next() {
                match std::str::from_utf8(key) {
                    Ok(key) => {
                        for ivalue in ivalues {
                            let i = ivalue.index;
                            let state = ivalue.state;
                            let distance = automatons[i].distance(state);
                            body.push_str(&format!("<p>{:?} (dist: {:?}) {:?}</p>", key, distance, ivalue.values));
                        }
                    },
                    Err(e) => eprintln!("{:?}", e),
                }
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
        let map = Map::from_bytes(fst, &values).unwrap();
        Arc::new(map)
    };

    TcpServer::new(Http, addr).serve(move || Ok(MainService {
        map: map.clone(),
        lev_builder: lev_builder.clone(),
    }))
}
