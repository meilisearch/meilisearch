mod common;

#[actix_rt::test]
async fn delete() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let (_response, status_code) = server.get_document(419704).await;
    assert_eq!(status_code, 200);

    server.delete_document(419704).await;

    let (_response, status_code) = server.get_document(419704).await;
    assert_eq!(status_code, 404);
}

// Resolve teh issue https://github.com/meilisearch/MeiliSearch/issues/493
#[actix_rt::test]
async fn delete_batch() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    let (_response, status_code) = server.get_document(419704).await;
    assert_eq!(status_code, 200);

    let body = serde_json::json!([419704, 512200, 181812]);
    server.delete_multiple_documents(body).await;

    let (_response, status_code) = server.get_document(419704).await;
    assert_eq!(status_code, 404);
}
