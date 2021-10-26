use crate::common::Server;
use serde_json::json;

#[actix_rt::test]
async fn error_get_update_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.index("test").get_update(0).await;

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
async fn error_get_unexisting_update_status() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    let (response, code) = index.get_update(0).await;

    let expected_response = json!({
        "message": "Task `0` not found.",
        "code": "task_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#task_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn get_update_status() {
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
    let (_response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    // TODO check resonse format, as per #48
}

#[actix_rt::test]
async fn error_list_updates_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.index("test").list_updates().await;

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
async fn list_no_updates() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    let (response, code) = index.list_updates().await;
    assert_eq!(code, 200);
    assert!(response.as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn list_updates() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    index
        .add_documents(
            serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(),
            None,
        )
        .await;
    let (response, code) = index.list_updates().await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 1);
}
