mod common;

use serde_json::json;
use actix_http::http::StatusCode;

macro_rules! assert_error {
    ($code:literal, $type:literal, $status:path, $req:expr) => {
        let (response, status_code) = $req;
        assert_eq!(status_code, $status);
        assert_eq!(response["errorCode"].as_str().unwrap(), $code);
        assert_eq!(response["errorType"].as_str().unwrap(), $type);
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
    assert_error!(
        "index_already_exists",
        "invalid_request_error",
        StatusCode::BAD_REQUEST,
        server.create_index(body).await);
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
#[ignore]
async fn max_field_limit_exceeded_error() {
    todo!("error reported in update")
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
    let bigvec = vec![0u64; 10_000_000]; // 80mb
    assert_error!(
        "payload_too_large",
        "invalid_request_error",
        StatusCode::PAYLOAD_TOO_LARGE,
        server.create_index(json!(bigvec)).await);
}
