#![allow(dead_code)]

use serde_json::Value;
use std::error::Error;
use std::time::Duration;

use assert_json_diff::assert_json_eq;
use async_std::io::prelude::*;
use async_std::task::{block_on, sleep};
use http_service::Body;
use http_service_mock::{make_server, TestBackend};
use meilisearch_http::data::Data;
use meilisearch_http::option::Opt;
use meilisearch_http::routes;
use serde_json::json;
use tempdir::TempDir;
use tide::server::Service;

pub fn setup_server() -> Result<TestBackend<Service<Data>>, Box<dyn Error>> {
    let tmp_dir = TempDir::new("meilisearch")?;

    let opt = Opt {
        db_path: tmp_dir.path().to_str().unwrap().to_string(),
        http_addr: "127.0.0.1:7700".to_owned(),
        master_key: None,
        env: "development".to_owned(),
        no_analytics: true,
    };

    let data = Data::new(opt.clone());
    let mut app = tide::with_state(data);
    routes::load_routes(&mut app);
    let http_server = app.into_http_service();
    Ok(make_server(http_server)?)
}

pub fn enrich_server_with_movies_index(
    server: &mut TestBackend<Service<Data>>,
) -> Result<(), Box<dyn Error>> {
    let body = json!({
        "uid": "movies",
        "identifier": "id",
    })
    .to_string()
    .into_bytes();

    let req = http::Request::post("/indexes")
        .body(Body::from(body))
        .unwrap();
    let _res = server.simulate(req).unwrap();

    Ok(())
}

pub fn enrich_server_with_movies_settings(
    server: &mut TestBackend<Service<Data>>,
) -> Result<(), Box<dyn Error>> {
    let json = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "dsc(popularity)",
            "exactness",
            "dsc(vote_average)",
        ],
        "distinctAttribute": null,
        "searchableAttributes": [
            "title",
            "tagline",
            "overview",
            "cast",
            "director",
            "producer",
            "production_companies",
            "genres",
        ],
        "displayedAttributes": [
            "title",
            "director",
            "producer",
            "tagline",
            "genres",
            "id",
            "overview",
            "vote_count",
            "vote_average",
            "poster_path",
            "popularity",
        ],
        "stopWords": null,
        "synonyms": null,
        "acceptNewFields": false,
    });

    let body = json.to_string().into_bytes();

    let req = http::Request::post("/indexes/movies/settings")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let response: Value = serde_json::from_slice(&buf).unwrap();

    assert!(response["updateId"].as_u64().is_some());

    wait_update_id(server, response["updateId"].as_u64().unwrap());

    Ok(())
}

pub fn enrich_server_with_movies_documents(
    server: &mut TestBackend<Service<Data>>,
) -> Result<(), Box<dyn Error>> {
    let body = include_bytes!("assets/movies.json").to_vec();

    let req = http::Request::post("/indexes/movies/documents")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let response: Value = serde_json::from_slice(&buf).unwrap();

    assert!(response["updateId"].as_u64().is_some());

    wait_update_id(server, response["updateId"].as_u64().unwrap());

    Ok(())
}

pub fn search(server: &mut TestBackend<Service<Data>>, query: &str, expect: Value) {
    let req = http::Request::get(format!("/indexes/movies/search?{}", query))
        .body(Body::empty())
        .unwrap();
    let res = server.simulate(req).unwrap();

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let response: Value = serde_json::from_slice(&buf).unwrap();

    assert_json_eq!(expect, response["hits"].clone(), ordered: false)
}

pub fn update_config(server: &mut TestBackend<Service<Data>>, config: Value) {
    let body = config.to_string().into_bytes();

    let req = http::Request::post("/indexes/movies/settings")
        .body(Body::from(body))
        .unwrap();
    let res = server.simulate(req).unwrap();
    assert_eq!(res.status(), 202);

    let mut buf = Vec::new();
    block_on(res.into_body().read_to_end(&mut buf)).unwrap();
    let response: Value = serde_json::from_slice(&buf).unwrap();

    assert!(response["updateId"].as_u64().is_some());

    wait_update_id(server, response["updateId"].as_u64().unwrap());
}

pub fn wait_update_id(server: &mut TestBackend<Service<Data>>, update_id: u64) {
    loop {
        let req = http::Request::get(format!("/indexes/movies/updates/{}", update_id))
            .body(Body::empty())
            .unwrap();

        let res = server.simulate(req).unwrap();
        assert_eq!(res.status(), 200);

        let mut buf = Vec::new();
        block_on(res.into_body().read_to_end(&mut buf)).unwrap();
        let response: Value = serde_json::from_slice(&buf).unwrap();

        if response["status"] == "processed" {
            return;
        }
        block_on(sleep(Duration::from_secs(1)));
    }
}
