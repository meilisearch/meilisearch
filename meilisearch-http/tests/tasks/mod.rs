use crate::common::Server;
use serde_json::json;

#[actix_rt::test]
async fn error_get_task_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.service.get("/indexes/test/tasks").await;

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
async fn error_get_unexisting_task_status() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    index.wait_task(0).await;
    let (response, code) = index.get_task(1).await;

    let expected_response = json!({
        "message": "Task `1` not found.",
        "code": "task_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#task_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn get_task_status() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    index
        .add_documents(
            serde_json::json!([{
                "id": 1,
                "content": "foobar",
            }]),
            None,
        )
        .await;
    index.wait_task(0).await;
    let (_response, code) = index.get_task(1).await;
    assert_eq!(code, 200);
    // TODO check resonse format, as per #48
}

#[actix_rt::test]
async fn error_list_tasks_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.index("test").list_tasks().await;

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
async fn list_tasks() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    index.wait_task(0).await;
    index
        .add_documents(
            serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(),
            None,
        )
        .await;
    let (response, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}
