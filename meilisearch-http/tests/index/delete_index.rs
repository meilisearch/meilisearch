use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn create_and_delete_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.create(None).await;

    assert_eq!(code, 202);

    index.wait_task(0).await;

    assert_eq!(index.get().await.1, 200);

    let (_response, code) = index.delete().await;

    assert_eq!(code, 202);

    index.wait_task(1).await;

    assert_eq!(index.get().await.1, 404);
}

#[actix_rt::test]
async fn error_delete_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.delete().await;

    assert_eq!(code, 202);

    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    let response = index.wait_task(0).await;
    assert_eq!(response["status"], "failed");
    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn loop_delete_add_documents() {
    let server = Server::new().await;
    let index = server.index("test");
    let documents = json!([{"id": 1, "field1": "hello"}]);
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let (response, code) = index.add_documents(documents.clone(), None).await;
        tasks.push(response["taskUid"].as_u64().unwrap());
        assert_eq!(code, 202, "{}", response);
        let (response, code) = index.delete().await;
        tasks.push(response["taskUid"].as_u64().unwrap());
        assert_eq!(code, 202, "{}", response);
    }

    for task in tasks {
        let response = index.wait_task(task).await;
        assert_eq!(response["status"], "succeeded", "{}", response);
    }
}
