mod common;

#[actix_rt::test]
async fn delete() {
    let mut server = common::Server::test_server().await;

    let (_response, status_code) = server.get_document(50).await;
    assert_eq!(status_code, 200);

    server.delete_document(50).await;

    let (_response, status_code) = server.get_document(50).await;
    assert_eq!(status_code, 404);
}

// Resolve the issue https://github.com/meilisearch/MeiliSearch/issues/493
#[actix_rt::test]
async fn delete_batch() {
    let mut server = common::Server::test_server().await;

    let (_response, status_code) = server.get_document(50).await;
    assert_eq!(status_code, 200);

    let body = serde_json::json!([50, 55, 60]);
    server.delete_multiple_documents(body).await;

    let (_response, status_code) = server.get_document(50).await;
    assert_eq!(status_code, 404);
}
