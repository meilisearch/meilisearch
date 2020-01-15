// use async_std::prelude::*;
// use async_std::task;
// use std::time::Duration;

// use serde::{Deserialize, Serialize};
use http_service::Body;

mod common;

#[test]
fn create_index() {
    let mut server = common::setup_server().unwrap();

    let req = http::Request::get("/health").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);
}
