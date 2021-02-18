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

// test fails (issue #46)
#[actix_rt::test]
#[ignore]
async fn create_with_invalid_index_uid() {
    let server = Server::new().await;
    let index = server.index("test test");
    let (_, code) = index.create(None).await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn test_create_multiple_indexes() {
    let server = Server::new().await;
    let index1 = server.index("test1");
    let index2 = server.index("test2");
    let index3 = server.index("test3");
    let index4 = server.index("test4");

    index1.create(None).await;
    index2.create(None).await;
    index3.create(None).await;

    assert_eq!(index1.get().await.1, 200);
    assert_eq!(index2.get().await.1, 200);
    assert_eq!(index3.get().await.1, 200);
    assert_eq!(index4.get().await.1, 400);
}
