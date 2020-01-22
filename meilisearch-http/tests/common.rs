#![allow(dead_code)]
use std::error::Error;
use std::time::Duration;

use http_service::Body;
use http_service_mock::{make_server, TestBackend};
use meilisearch_http::data::Data;
use meilisearch_http::option::Opt;
use meilisearch_http::routes;
use serde_json::json;
use tempdir::TempDir;
use tide::server::Service;
use async_std::task::{block_on, sleep};

pub fn setup_server() -> Result<TestBackend<Service<Data>>, Box<dyn Error>> {
    let tmp_dir = TempDir::new("meilisearch")?;

    let opt = Opt {
        db_path: tmp_dir.path().to_str().unwrap().to_string(),
        http_addr: "127.0.0.1:7700".to_owned(),
        api_key: None,
        no_analytics: true,
    };

    let data = Data::new(opt.clone());
    let mut app = tide::with_state(data);
    routes::load_routes(&mut app);
    let http_server = app.into_http_service();
    Ok(make_server(http_server)?)
}

pub fn enrich_server_with_movies_index(server: &mut TestBackend<Service<Data>>) -> Result<(), Box<dyn Error>> {

    let body = json!({
        "uid": "movies",
    }).to_string().into_bytes();

    let req = http::Request::post("/indexes").body(Body::from(body)).unwrap();
    let res = server.simulate(req).unwrap();

    println!("enrich_server_with_movies_index: {:?}", res.status());

    Ok(())
}

pub fn enrich_server_with_movies_settings(server: &mut TestBackend<Service<Data>>) -> Result<(), Box<dyn Error>> {

    let json = json!({
        "rankingRules": [
            "_typo",
            "_words",
            "_proximity",
            "_attribute",
            "_words_position",
            "dsc(popularity)",
            "_exact",
            "dsc(vote_average)",
        ],
        "rankingDistinct": null,
        "attributeIdentifier": "id",
        "attributesSearchable": [
            "title",
            "tagline",
            "overview",
            "cast",
            "director",
            "producer",
            "production_companies",
            "genres",
        ],
        "attributesDisplayed": [
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
        "synonyms": null
    });

    let body = json.to_string().into_bytes();

    let req = http::Request::post("/indexes/movies/settings").body(Body::from(body)).unwrap();
    let res = server.simulate(req).unwrap();

    println!("enrich_server_with_movies_settings: {:?}", res.status());

    block_on(sleep(Duration::from_secs(5)));

    Ok(())
}

pub fn enrich_server_with_movies_documents(server: &mut TestBackend<Service<Data>>) -> Result<(), Box<dyn Error>> {

    let body = include_bytes!("assets/movies.json").to_vec();

    let req = http::Request::post("/indexes/movies/documents").body(Body::from(body)).unwrap();
    let res = server.simulate(req).unwrap();

    println!("enrich_server_with_movies_documents: {:?}", res.status());

    block_on(sleep(Duration::from_secs(5)));

    Ok(())
}

