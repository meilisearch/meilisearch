#[macro_use] extern crate lazy_static;
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

use fst::{IntoStreamer, Streamer};
use levenshtein_automata::LevenshteinAutomatonBuilder;
use futures::future;
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::Service;

use raptor::FstMap;

lazy_static! {
    static ref MAP: FstMap<u64> = {
        let map = read_to_vec("map.fst").unwrap();
        let values = read_to_vec("values.vecs").unwrap();

        FstMap::from_bytes(map, &values).unwrap()
    };

    static ref LEV_AUT_BLDR_0: LevenshteinAutomatonBuilder = LevenshteinAutomatonBuilder::new(0, false);
    static ref LEV_AUT_BLDR_1: LevenshteinAutomatonBuilder = LevenshteinAutomatonBuilder::new(1, false);
    static ref LEV_AUT_BLDR_2: LevenshteinAutomatonBuilder = LevenshteinAutomatonBuilder::new(2, false);
}

struct MainService {
    map: &'static FstMap<u64>,
    lev_aut_bldr_0: &'static LevenshteinAutomatonBuilder,
    lev_aut_bldr_1: &'static LevenshteinAutomatonBuilder,
    lev_aut_bldr_2: &'static LevenshteinAutomatonBuilder,
}

fn construct_body<'f, S>(mut stream: S) -> String
where
    S: 'f + for<'a> Streamer<'a, Item=(&'a str, &'a [u64])>
{
    let mut body = String::new();
    body.push_str("<html><body>");

    while let Some((key, values)) = stream.next() {
        let values = &values[..values.len().min(10)];
        body.push_str(&format!("{:?} {:?}</br>", key, values));
    }

    body.push_str("</body></html>");

    body
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

            let lev = if key.len() <= 4 {
                self.lev_aut_bldr_0.build_dfa(&key)
            } else if key.len() <= 8 {
                self.lev_aut_bldr_1.build_dfa(&key)
            } else {
                self.lev_aut_bldr_2.build_dfa(&key)
            };

            let stream = self.map.search(lev).into_stream();
            let body = construct_body(stream);

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

    // initialize all "lazy" variables
    lazy_static::initialize(&MAP);
    lazy_static::initialize(&LEV_AUT_BLDR_0);
    lazy_static::initialize(&LEV_AUT_BLDR_1);
    lazy_static::initialize(&LEV_AUT_BLDR_2);

    let addr = "0.0.0.0:8080".parse().unwrap();

    TcpServer::new(Http, addr).serve(|| Ok(MainService {
        map: &MAP,
        lev_aut_bldr_0: &LEV_AUT_BLDR_0,
        lev_aut_bldr_1: &LEV_AUT_BLDR_1,
        lev_aut_bldr_2: &LEV_AUT_BLDR_2,
    }))
}
