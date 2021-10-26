use crate::common::Server;
use chrono::DateTime;
use serde_json::json;

#[actix_rt::test]
async fn update_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;

    assert_eq!(code, 201);

    let (response, code) = index.update(Some("primary")).await;

    assert_eq!(code, 200);
    assert_eq!(response["uid"], "test");
    assert_eq!(response["name"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());

    let created_at = DateTime::parse_from_rfc3339(response["createdAt"].as_str().unwrap()).unwrap();
    let updated_at = DateTime::parse_from_rfc3339(response["updatedAt"].as_str().unwrap()).unwrap();
    assert!(created_at < updated_at);

    assert_eq!(response["primaryKey"], "primary");
    assert_eq!(response.as_object().unwrap().len(), 5);
}

#[actix_rt::test]
async fn update_nothing() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.create(None).await;

    assert_eq!(code, 201);

    let (update, code) = index.update(None).await;

    assert_eq!(code, 200);
    assert_eq!(response, update);
}

#[actix_rt::test]
async fn error_update_existing_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.create(Some("id")).await;

    assert_eq!(code, 201);

    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    index.add_documents(documents, None).await;
    index.wait_update_id(0).await;

    let (response, code) = index.update(Some("primary")).await;

    let expected_response = json!({
        "message": "Index already has a primary key: `id`.",
        "code": "index_primary_key_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_primary_key_already_exists"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn error_update_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.index("test").update(None).await;

    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}
