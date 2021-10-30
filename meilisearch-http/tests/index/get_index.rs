use crate::common::Server;
use serde_json::json;
use serde_json::Value;

#[actix_rt::test]
async fn create_and_get_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;

    assert_eq!(code, 201);

    let (response, code) = index.get().await;

    assert_eq!(code, 200);
    assert_eq!(response["uid"], "test");
    assert_eq!(response["name"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());
    assert_eq!(response["createdAt"], response["updatedAt"]);
    assert_eq!(response["primaryKey"], Value::Null);
    assert_eq!(response.as_object().unwrap().len(), 5);
}

#[actix_rt::test]
async fn error_get_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.get().await;

    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn no_index_return_empty_list() {
    let server = Server::new().await;
    let (response, code) = server.list_indexes().await;
    assert_eq!(code, 200);
    assert!(response.is_array());
    assert!(response.as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn list_multiple_indexes() {
    let server = Server::new().await;
    server.index("test").create(None).await;
    server.index("test1").create(Some("key")).await;

    let (response, code) = server.list_indexes().await;
    assert_eq!(code, 200);
    assert!(response.is_array());
    let arr = response.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr
        .iter()
        .any(|entry| entry["uid"] == "test" && entry["primaryKey"] == Value::Null));
    assert!(arr
        .iter()
        .any(|entry| entry["uid"] == "test1" && entry["primaryKey"] == "key"));
}
