use std::time::Duration;
use std::convert::Into;

use async_std::task::{block_on, sleep};
use async_std::io::prelude::*;
use http_service::Body;
use serde_json::json;
use serde_json::Value;
use assert_json_diff::assert_json_eq;

mod common;

#[test]
fn write_all_and_retreive() {
    let mut server = common::setup_server().unwrap();

    // 1 - Create the index

    let body = json!({
        "uid": "movies",
    }).to_string().into_bytes();

    let req = http::Request::post("/indexes").body(Body::from(body)).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 201);

    // 2 - Send the settings

    let json = json!({
        "ranking_rules": [
            "_typo",
            "_words",
            "_proximity",
            "_attribute",
            "_words_position",
            "_exact",
            "dsc(release_date)",
            "dsc(rank)",
        ],
        "ranking_distinct": "movie_id",
        "attribute_identifier": "uid",
        "attributes_searchable": [
            "uid",
            "movie_id",
            "title",
            "description",
            "poster",
            "release_date",
            "rank",
        ],
        "attributes_displayed": [
            "title",
            "description",
            "poster",
            "release_date",
            "rank",
        ],
        "attributes_ranked": [
            "release_date",
            "rank",
        ],
        "stop_words": [
            "the",
            "a",
            "an",
        ],
        "synonyms": {
            "wolverine": ["xmen", "logan"],
            "logan": ["wolverine"],
        }
    });

    let body = json.to_string().into_bytes();

    let req = http::Request::post("/indexes/movies/settings").body(Body::from(body)).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 202);

    block_on(sleep(Duration::from_secs(1)));

    let req = http::Request::get("/indexes/movies/settings").body(Body::empty()).unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 200);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let res_value: Value = serde_json::from_slice(&buf).unwrap();

    println!("json1: {:?}", json);
    println!("json2: {:?}", res_value);

    assert_json_eq!(json, res_value, ordered: false);
}
