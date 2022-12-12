use serde_json::json;

use crate::common::{GetAllDocumentsOptions, Server};

#[actix_rt::test]
async fn delete_one_document_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.delete_document(0).await;
    assert_eq!(code, 202);

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "failed");
}

#[actix_rt::test]
async fn delete_one_unexisting_document() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    let (response, code) = index.delete_document(0).await;
    assert_eq!(code, 202, "{}", response);
    let update = index.wait_task(0).await;
    assert_eq!(update["status"], "succeeded");
}

#[actix_rt::test]
async fn delete_one_document() {
    let server = Server::new().await;
    let index = server.index("test");
    index.add_documents(json!([{ "id": 0, "content": "foobar" }]), None).await;
    index.wait_task(0).await;
    let (_response, code) = server.index("test").delete_document(0).await;
    assert_eq!(code, 202);
    index.wait_task(1).await;

    let (_response, code) = index.get_document(0, None).await;
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn clear_all_documents_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.clear_all_documents().await;
    assert_eq!(code, 202);

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "failed");
}

#[actix_rt::test]
async fn clear_all_documents() {
    let server = Server::new().await;
    let index = server.index("test");
    index
        .add_documents(
            json!([{ "id": 1, "content": "foobar" }, { "id": 0, "content": "foobar" }]),
            None,
        )
        .await;
    index.wait_task(0).await;
    let (_response, code) = index.clear_all_documents().await;
    assert_eq!(code, 202);

    let _update = index.wait_task(1).await;
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert!(response["results"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn clear_all_documents_empty_index() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;

    let (_response, code) = index.clear_all_documents().await;
    assert_eq!(code, 202);

    let _update = index.wait_task(0).await;
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert!(response["results"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn error_delete_batch_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.delete_batch(vec![]).await;
    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });
    assert_eq!(code, 202);

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "failed");
    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn delete_batch() {
    let server = Server::new().await;
    let index = server.index("test");
    index.add_documents(json!([{ "id": 1, "content": "foobar" }, { "id": 0, "content": "foobar" }, { "id": 3, "content": "foobar" }]), Some("id")).await;
    index.wait_task(0).await;
    let (_response, code) = index.delete_batch(vec![1, 0]).await;
    assert_eq!(code, 202);

    let _update = index.wait_task(1).await;
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);
    assert_eq!(response["results"][0]["id"], json!(3));
}

#[actix_rt::test]
async fn delete_no_document_batch() {
    let server = Server::new().await;
    let index = server.index("test");
    index.add_documents(json!([{ "id": 1, "content": "foobar" }, { "id": 0, "content": "foobar" }, { "id": 3, "content": "foobar" }]), Some("id")).await;
    index.wait_task(0).await;
    let (_response, code) = index.delete_batch(vec![]).await;
    assert_eq!(code, 202, "{}", _response);

    let _update = index.wait_task(1).await;
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 3);
}
