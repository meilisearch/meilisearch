#![feature(async_await)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tide::querystring::ExtractQuery;
use tide::http::status::StatusCode;
use tide::{error::ResultExt, response, App, Context, EndpointResult};
use serde_json::Value;
use meilidb_data::{Database, Schema};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SearchQuery {
    q: String,
}

async fn create_index(mut cx: Context<Database>) -> EndpointResult<()> {
    let index: String = cx.param("index").client_err()?;
    let schema = cx.body_bytes().await.client_err()?;
    let schema = Schema::from_toml(schema.as_slice()).unwrap();

    let database = cx.app_data();
    database.create_index(&index, schema).unwrap();

    Ok(())
}

async fn update_documents(mut cx: Context<Database>) -> EndpointResult<()> {
    let index: String = cx.param("index").client_err()?;
    let document: HashMap<String, Value> = cx.body_json().await.client_err()?;

    let database = cx.app_data();
    let index = match database.open_index(&index).unwrap() {
        Some(index) => index,
        None => Err(StatusCode::NOT_FOUND)?,
    };

    let mut addition = index.documents_addition();
    addition.update_document(document).unwrap();
    addition.finalize().unwrap();

    Ok(())
}

async fn search_index(cx: Context<Database>) -> EndpointResult {
    let index: String = cx.param("index").client_err()?;
    let query: SearchQuery = cx.url_query()?;

    let database = cx.app_data();

    let index = match database.open_index(&index).unwrap() {
        Some(index) => index,
        None => Err(StatusCode::NOT_FOUND)?,
    };

    let documents_ids = index.query_builder().query(&query.q, 0..100).unwrap();
    let documents: Vec<Value> = documents_ids
        .into_iter()
        .filter_map(|x| index.document(None, x.id).unwrap())
        .collect();

    Ok(response::json(documents))
}

fn main() -> std::io::Result<()> {
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::start_default(&tmp_dir).unwrap();
    let mut app = App::new(database);

    app.at("/:index").post(create_index).put(update_documents);
    app.at("/:index/search").get(search_index);

    app.serve("127.0.0.1:8000")
}
