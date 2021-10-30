use crate::common::Server;
use serde_json::{json, Value};

#[actix_rt::test]
async fn create_index_no_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.create(None).await;

    assert_eq!(code, 201);
    assert_eq!(response["uid"], "test");
    assert_eq!(response["name"], "test");
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

    assert_eq!(code, 201);
    assert_eq!(response["uid"], "test");
    assert_eq!(response["name"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());
    //assert_eq!(response["createdAt"], response["updatedAt"]);
    assert_eq!(response["primaryKey"], "primary");
    assert_eq!(response.as_object().unwrap().len(), 5);
}

#[actix_rt::test]
async fn create_index_with_invalid_primary_key() {
    let document = json!([ { "id": 2, "title": "Pride and Prejudice" } ]);

    let server = Server::new().await;
    let index = server.index("movies");
    let (_response, code) = index.add_documents(document, Some("title")).await;
    assert_eq!(code, 202);

    index.wait_update_id(0).await;

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], Value::Null);
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
    assert_eq!(index4.get().await.1, 404);
}

#[actix_rt::test]
async fn error_create_existing_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(Some("primary")).await;

    assert_eq!(code, 201);

    let (response, code) = index.create(Some("primary")).await;

    let expected_response = json!({
        "message": "Index `test` already exists.",
        "code": "index_already_exists",
        "type": "invalid_request",
        "link":"https://docs.meilisearch.com/errors#index_already_exists"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 409);
}

#[actix_rt::test]
#[ignore] // TODO: Fix in an other PR: uid returned `test%20test%23%21` instead of `test test#!`
async fn error_create_with_invalid_index_uid() {
    let server = Server::new().await;
    let index = server.index("test test#!");
    let (response, code) = index.create(None).await;

    let expected_response = json!({
        "message": "`test test#!` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
        "code": "invalid_index_uid",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 400);
}
