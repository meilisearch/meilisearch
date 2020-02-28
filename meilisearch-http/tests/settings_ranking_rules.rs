use std::time::Duration;

use assert_json_diff::assert_json_eq;
use async_std::io::prelude::*;
use async_std::task::{block_on, sleep};
use http_service::Body;
use serde_json::json;
use serde_json::Value;

mod common;

// Process:
// - Write a full settings update
// - Delete all settings
// Check:
// - Settings are deleted, all fields are null
// - POST success repond Status Code 202
// - Get success repond Status Code 200
// - Delete success repond Status Code 202
#[test]
fn write_all_and_delete() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create the index

    let body = json!({
        "uid": "movies",
        "identifier": "uid",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    // 2 - Send the settings

    let json = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "dsc(release_date)",
        "dsc(rank)",
    ]);

    let body = json.to_string().into_bytes();

    let req = http::Request::post("/indexes/movies/settings/ranking-rules")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 202);

    block_on(sleep(Duration::from_secs(2)));

    // 3 - Get all settings and compare to the previous one

    let req = http::Request::get("/indexes/movies/settings/ranking-rules")
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_json_eq!(json, res_value, ordered: false);

    // 4 - Delete all settings

    let req = http::Request::delete("/indexes/movies/settings/ranking-rules")
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 202);

    block_on(sleep(Duration::from_secs(2)));

    // 5 - Get all settings and check if they are empty

    let req = http::Request::get("/indexes/movies/settings/ranking-rules")
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    let json = json!([
      "typo",
      "words",
      "proximity",
      "attribute",
      "wordsPosition",
      "exactness"
    ]);

    assert_json_eq!(json, res_value, ordered: false);
}

// Process:
// - Write a full setting update
// - Rewrite an other settings confirmation
// Check:
// - Settings are overwrited
// - Forgotten attributes are deleted
// - Null attributes are deleted
// - Empty attribute are deleted
#[test]
fn write_all_and_update() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create the index

    let body = json!({
        "uid": "movies",
        "identifier": "uid",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    // 2 - Send the settings

    let json = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "dsc(release_date)",
        "dsc(rank)",
    ]);

    let body = json.to_string().into_bytes();

    let req = http::Request::post("/indexes/movies/settings/ranking-rules")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 202);

    block_on(sleep(Duration::from_secs(1)));

    // 3 - Get all settings and compare to the previous one

    let req = http::Request::get("/indexes/movies/settings/ranking-rules")
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    assert_json_eq!(json, res_value, ordered: false);

    // 4 - Update all settings

    let json_update = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "dsc(release_date)",
    ]);

    let body_update = json_update.to_string().into_bytes();

    let req = http::Request::post("/indexes/movies/settings/ranking-rules")
        .body(Body::from(body_update))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 202);

    block_on(sleep(Duration::from_secs(1)));

    // 5 - Get all settings and check if the content is the same of (4)

    let req = http::Request::get("/indexes/movies/settings/ranking-rules")
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    let res_expected = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "dsc(release_date)",
    ]);

    assert_json_eq!(res_expected, res_value, ordered: false);
}
