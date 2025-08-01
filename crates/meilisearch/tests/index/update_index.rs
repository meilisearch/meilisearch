use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::common::encoder::Encoder;
use crate::common::{shared_does_not_exists_index, shared_index_with_documents, Server};
use crate::json;

#[actix_rt::test]
async fn update_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(None).await;

    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) = index.update(Some("primary")).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.get().await;

    assert_eq!(code, 200);

    assert_eq!(response["uid"], index.uid);
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
async fn create_and_update_with_different_encoding() {
    let server = Server::new_shared();
    let index = server.unique_index_with_encoder(Encoder::Gzip);
    let (create_task, code) = index.create(None).await;

    assert_eq!(code, 202);
    server.wait_task(create_task.uid()).await.succeeded();

    let index = index.with_encoder(Encoder::Brotli);
    let (task, _status_code) = index.update(Some("primary")).await;

    server.wait_task(task.uid()).await.succeeded();
}

#[actix_rt::test]
async fn update_nothing() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task1, code) = index.create(None).await;

    assert_eq!(code, 202);

    server.wait_task(task1.uid()).await.succeeded();

    let (task2, code) = index.update(None).await;

    assert_eq!(code, 202);

    server.wait_task(task2.uid()).await.succeeded();
}

#[actix_rt::test]
async fn error_update_existing_primary_key() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let (update_task, code) = index.update_index_fail(Some("primary"), server).await;

    assert_eq!(code, 202);
    let response = server.wait_task(update_task.uid()).await.failed();

    let expected_response = json!({
        "message": format!("Index `{}`: Index already has a primary key: `id`.", index.uid),
        "code": "index_primary_key_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_primary_key_already_exists"
    });

    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn error_update_unexisting_index() {
    let server = Server::new_shared();
    let index = shared_does_not_exists_index().await;
    let (task, code) = index.update_index_fail(Some("my-primary-key"), server).await;

    assert_eq!(code, 202);

    let response = server.wait_task(task.uid()).await.failed();

    let expected_response = json!({
        "message": format!("Index `{}` not found.", index.uid),
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response["error"], expected_response);
}
