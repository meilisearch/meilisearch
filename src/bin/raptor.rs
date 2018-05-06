extern crate env_logger;
extern crate fst;
extern crate futures;
extern crate levenshtein_automata;
extern crate raptor;
extern crate tokio_minihttp;
extern crate tokio_proto;
extern crate tokio_service;
extern crate url;

use std::io;
use std::path::Path;
use std::fs::File;
use std::io::{Read, BufReader};

use fst::Streamer;
use futures::future;
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::Service;

use raptor::{FstMap, OpWithStateBuilder};

static mut MAP: Option<FstMap<u64>> = None;
static mut LEV_BUILDER_0: Option<LevBuilder> = None;
static mut LEV_BUILDER_1: Option<LevBuilder> = None;
static mut LEV_BUILDER_2: Option<LevBuilder> = None;

struct MainService<'a> {
    map: &'a FstMap<u64>,
    lev_builder_0: &'a LevBuilder,
    lev_builder_1: &'a LevBuilder,
    lev_builder_2: &'a LevBuilder,
}

impl<'a> Service for MainService<'a> {
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
                let lev = if query.len() <= 4 {
                    self.lev_builder_0.build_dfa(&query)
                } else if query.len() <= 8 {
                    self.lev_builder_1.build_dfa(&query)
                } else {
                    self.lev_builder_2.build_dfa(&query)
                };
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

fn read_to_vec<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let mut file = BufReader::new(file);

    let mut vec = Vec::new();
    file.read_to_end(&mut vec)?;

    Ok(vec)
}

fn main() {
    drop(env_logger::init());

    // initialize all static variables
    unsafe {
        MAP = {
            let map = read_to_vec("map.fst").unwrap();
            let values = read_to_vec("values.vecs").unwrap();

            Some(FstMap::from_bytes(map, &values).unwrap())
        };
        LEV_BUILDER_0 = Some(LevBuilder::new(0, false));
        LEV_BUILDER_1 = Some(LevBuilder::new(1, false));
        LEV_BUILDER_2 = Some(LevBuilder::new(2, false));
    }

    let addr = "0.0.0.0:8080".parse().unwrap();

    unsafe {
        TcpServer::new(Http, addr).serve(|| Ok(MainService {
            map: MAP.as_ref().unwrap(),
            lev_builder_0: LEV_BUILDER_0.as_ref().unwrap(),
            lev_builder_1: LEV_BUILDER_1.as_ref().unwrap(),
            lev_builder_2: LEV_BUILDER_2.as_ref().unwrap(),
        }))
    }
}
