extern crate env_logger;
extern crate futures;
extern crate raptor;
extern crate tokio_minihttp;
extern crate tokio_proto;
extern crate tokio_service;
extern crate url;

use std::io;

use futures::future;
use tokio_minihttp::{Request, Response, Http};
use tokio_proto::TcpServer;
use tokio_service::Service;

use raptor::{MultiMapBuilder, MultiMap};

struct MainService {
    map: MultiMap,
}

impl Service for MainService {
    type Request = Request;
    type Response = Response;
    type Error = io::Error;
    type Future = future::Ok<Response, io::Error>;

    fn call(&self, request: Request) -> Self::Future {

        let url = format!("http://raptor.net{}", request.path());
        let url = url::Url::parse(&url).unwrap();

        if let Some((_, key)) = url.query_pairs().find(|&(ref k, _)| k == "query") {
            let values = self.map.get(&*key);
            println!("{:?}", values);
        }

        let mut resp = Response::new();
        resp.body("Hello, world!");
        future::ok(resp)
    }
}

fn main() {
    drop(env_logger::init());
    let addr = "0.0.0.0:8080".parse().unwrap();

    TcpServer::new(Http, addr).serve(|| {

        // TODO move the MultiMap construction out of this
        //      closure, make it global.
        //      It will permit the server to be multithreaded.

        let mut builder = MultiMapBuilder::new();
        builder.insert("foo", 12);
        builder.insert("foo", 13);
        builder.insert("bar", 10);

        let map = builder.build_memory().unwrap();

        println!("Called Fn here !");

        Ok(MainService { map })
    })
}
