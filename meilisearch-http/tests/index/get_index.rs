use crate::common::Server;
use serde_json::Value;

#[actix_rt::test]
async fn create_and_get_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;

    assert_eq!(code, 200);

    let (response, code) = index.get().await;

    assert_eq!(code, 200);
    assert_eq!(response["uid"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());
    assert_eq!(response["createdAt"], response["updatedAt"]);
    assert_eq!(response["primaryKey"], Value::Null);
    assert_eq!(response.as_object().unwrap().len(), 4);
}

// TODO: partial test since we are testing error, amd error is not yet fully implemented in
// transplant
#[actix_rt::test]
async fn get_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, code) = index.get().await;

    assert_eq!(code, 400);
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
    assert!(arr.iter().find(|entry| entry["uid"] == "test" && entry["primaryKey"] == Value::Null).is_some());
    assert!(arr.iter().find(|entry| entry["uid"] == "test1" && entry["primaryKey"] == "key").is_some());
}
