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

    let doc_ids = vec!(50, 55, 60);
    for doc_id in &doc_ids {
        let (_response, status_code) = server.get_document(doc_id).await;
        assert_eq!(status_code, 200);
    }

    let body = serde_json::json!(&doc_ids);
    server.delete_multiple_documents(body).await;

    for doc_id in &doc_ids {
        let (_response, status_code) = server.get_document(doc_id).await;
        assert_eq!(status_code, 404);
    }
}
