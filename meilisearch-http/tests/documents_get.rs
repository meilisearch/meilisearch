use serde_json::json;
use actix_web::http::StatusCode;

mod common;

#[actix_rt::test]
async fn get_documents_from_unexisting_index_is_error() {
    let mut server = common::Server::with_uid("test");
    let (response, status) = server.get_all_documents().await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(response["errorCode"], "index_not_found");
    assert_eq!(response["errorType"], "invalid_request_error");
    assert_eq!(response["errorLink"], "https://docs.meilisearch.com/errors#index_not_found");
}

#[actix_rt::test]
async fn get_empty_documents_list() {
    let mut server = common::Server::with_uid("test");
    server.create_index(json!({ "uid": "test" })).await;
    let (response, status) = server.get_all_documents().await;
    assert_eq!(status, StatusCode::OK);
    assert!(response.as_array().unwrap().is_empty());
}
