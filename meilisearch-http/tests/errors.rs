mod common;

use std::thread;
use std::time::Duration;

use actix_http::http::StatusCode;
use serde_json::{json, Map, Value};

macro_rules! assert_error {
    ($code:literal, $type:literal, $status:path, $req:expr) => {
        let (response, status_code) = $req;
        assert_eq!(status_code, $status);
        assert_eq!(response["errorCode"].as_str().unwrap(), $code);
        assert_eq!(response["errorType"].as_str().unwrap(), $type);
    };
}

macro_rules! assert_error_async {
    ($code:literal, $type:literal, $server:expr, $req:expr) => {
        let (response, _) = $req;
        let update_id = response["updateId"].as_u64().unwrap();
        for _ in 1..10 {
            let (response, status_code) = $server.get_update_status(update_id).await;
            assert_eq!(status_code, StatusCode::OK);
            if response["status"] == "processed" || response["status"] == "failed" {
                println!("response: {}", response);
                assert_eq!(response["status"], "failed");
                assert_eq!(response["errorCode"], $code);
                assert_eq!(response["errorType"], $type);
                return
            }
            thread::sleep(Duration::from_secs(1));
        }
    };
}

#[actix_rt::test]
async fn index_already_exists_error() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "uid": "test"
    });
    let (response, status_code) = server.create_index(body.clone()).await;
    println!("{}", response);
    assert_eq!(status_code, StatusCode::CREATED);

    let (response, status_code) = server.create_index(body.clone()).await;
    println!("{}", response);

    assert_error!(
        "index_already_exists",
        "invalid_request_error",
        StatusCode::BAD_REQUEST,
        (response, status_code));
}

#[actix_rt::test]
async fn index_not_found_error() {
    let mut server = common::Server::with_uid("test");
    assert_error!(
        "index_not_found",
        "invalid_request_error",
        StatusCode::NOT_FOUND,
        server.get_index().await);
}

#[actix_rt::test]
async fn primary_key_already_present_error() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "uid": "test",
        "primaryKey": "test"
    });
    server.create_index(body.clone()).await;
    let body = json!({
        "primaryKey": "t"
    });
    assert_error!(
        "primary_key_already_present",
        "invalid_request_error",
        StatusCode::BAD_REQUEST,
        server.update_index(body).await);
}

#[actix_rt::test]
async fn max_field_limit_exceeded_error() {
    let mut server = common::Server::test_server().await;
    let body = json!({
        "uid": "test",
    });
    server.create_index(body).await;
    let mut doc = Map::with_capacity(70_000);
    doc.insert("id".into(), Value::String("foo".into()));
    for i in 0..69_999 {
        doc.insert(format!("field{}", i), Value::String("foo".into()));
    }
    let docs = json!([doc]);
    assert_error_async!(
        "max_fields_limit_exceeded",
        "invalid_request_error",
        server,
        server.add_or_replace_multiple_documents_sync(docs).await);
}

#[actix_rt::test]
async fn missing_document_id() {
    let mut server = common::Server::test_server().await;
    let body = json!({
        "uid": "test",
        "primaryKey": "test"
    });
    server.create_index(body).await;
    let docs = json!([
        {
            "foo": "bar",
        }
    ]);
    assert_error_async!(
        "missing_document_id",
        "invalid_request_error",
        server,
        server.add_or_replace_multiple_documents_sync(docs).await);
}

#[actix_rt::test]
async fn facet_error() {
    let mut server = common::Server::test_server().await;
    let search = json!({
        "q": "foo",
        "facetFilters": ["test:hello"]
    });
    assert_error!(
        "invalid_facet",
        "invalid_request_error",
        StatusCode::BAD_REQUEST,
        server.search_post(search).await);
}

#[actix_rt::test]
async fn filters_error() {
    let mut server = common::Server::test_server().await;
    let search = json!({
        "q": "foo",
        "filters": "fo:12"
    });
    assert_error!(
        "invalid_filter",
        "invalid_request_error",
        StatusCode::BAD_REQUEST,
        server.search_post(search).await);
}

#[actix_rt::test]
async fn bad_request_error() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "foo": "bar",
    });
    assert_error!(
        "bad_request",
        "invalid_request_error",
        StatusCode::BAD_REQUEST,
        server.search_post(body).await);
}

#[actix_rt::test]
async fn document_not_found_error() {
    let mut server = common::Server::with_uid("test");
    server.create_index(json!({"uid": "test"})).await;
    assert_error!(
        "document_not_found",
        "invalid_request_error",
        StatusCode::NOT_FOUND,
        server.get_document(100).await);
}

#[actix_rt::test]
async fn payload_too_large_error() {
    let mut server = common::Server::with_uid("test");
    let bigvec = vec![0u64; 100_000_000]; // 800mb
    assert_error!(
        "payload_too_large",
        "invalid_request_error",
        StatusCode::PAYLOAD_TOO_LARGE,
        server.create_index(json!(bigvec)).await);
}

#[actix_rt::test]
async fn missing_primary_key_error() {
    let mut server = common::Server::with_uid("test");
    server.create_index(json!({"uid": "test"})).await;
    let document = json!([{
        "content": "test"
    }]);
    assert_error!(
        "missing_primary_key",
        "invalid_request_error",
        StatusCode::BAD_REQUEST,
        server.add_or_replace_multiple_documents_sync(document).await);
}
