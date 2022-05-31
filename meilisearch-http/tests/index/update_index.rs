use crate::common::Server;
use serde_json::json;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

#[actix_rt::test]
async fn update_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;

    assert_eq!(code, 202);

    index.update(Some("primary")).await;

    let response = index.wait_task(1).await;

    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get().await;

    assert_eq!(code, 200);

    assert_eq!(response["uid"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());

    let created_at =
        OffsetDateTime::parse(response["createdAt"].as_str().unwrap(), &Rfc3339).unwrap();
    let updated_at =
        OffsetDateTime::parse(response["updatedAt"].as_str().unwrap(), &Rfc3339).unwrap();
    assert!(created_at < updated_at);

    assert_eq!(response["primaryKey"], "primary");
    assert_eq!(response.as_object().unwrap().len(), 4);
}

#[actix_rt::test]
async fn update_nothing() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;

    assert_eq!(code, 202);

    index.wait_task(0).await;

    let (_, code) = index.update(None).await;

    assert_eq!(code, 202);

    let response = index.wait_task(1).await;

    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn error_update_existing_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.create(Some("id")).await;

    assert_eq!(code, 202);

    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    index.add_documents(documents, None).await;

    let (_, code) = index.update(Some("primary")).await;

    assert_eq!(code, 202);

    let response = index.wait_task(2).await;

    let expected_response = json!({
        "message": "Index already has a primary key: `id`.",
        "code": "index_primary_key_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_primary_key_already_exists"
    });

    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn error_update_unexisting_index() {
    let server = Server::new().await;
    let (_, code) = server.index("test").update(None).await;

    assert_eq!(code, 202);

    let response = server.index("test").wait_task(0).await;

    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response["error"], expected_response);
}
