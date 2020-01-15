use http_service::Body;
use serde_json::json;
use std::convert::Into;

mod common;

#[test]
fn test_healthyness() {
    let mut server = common::setup_server().unwrap();

    // Check that the server is healthy

    let req = http::Request::get("/health").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    // Set the serve Unhealthy

    let body = json!({
        "health": false,
    }).to_string().into_bytes();

    let req = http::Request::put("/health").body(Body::from(body)).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    // Check that the server is unhealthy

    let req = http::Request::get("/health").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 503);

    // Set the server healthy

    let body = json!({
        "health": true,
    }).to_string().into_bytes();

    let req = http::Request::put("/health").body(Body::from(body)).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    // Check if the server is healthy

    let req = http::Request::get("/health").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);
}
