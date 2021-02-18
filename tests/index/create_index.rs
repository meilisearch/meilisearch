use crate::common::Server;
use serde_json::Value;

#[actix_rt::test]
async fn create_index_no_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.create(None).await;

    assert_eq!(code, 200);
    assert_eq!(response["uid"], "test");
    assert!(response.get("uuid").is_some());
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());
    assert_eq!(response["createdAt"], response["updatedAt"]);
    assert_eq!(response["primaryKey"], Value::Null);
    assert_eq!(response.as_object().unwrap().len(), 5);
}

#[actix_rt::test]
async fn create_index_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.create(Some("primary")).await;

    assert_eq!(code, 200);
    assert_eq!(response["uid"], "test");
    assert!(response.get("uuid").is_some());
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());
    assert_eq!(response["createdAt"], response["updatedAt"]);
    assert_eq!(response["primaryKey"], "primary");
    assert_eq!(response.as_object().unwrap().len(), 5);
}

// TODO: partial test since we are testing error, amd error is not yet fully implemented in
// transplant
#[actix_rt::test]
async fn create_existing_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(Some("primary")).await;

    assert_eq!(code, 200);

    let (_response, code) = index.create(Some("primary")).await;
    assert_eq!(code, 400);
}
