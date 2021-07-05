use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn create_and_delete_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.create(None).await;

    assert_eq!(code, 200);

    let (_response, code) = index.delete().await;

    assert_eq!(code, 204);

    assert_eq!(index.get().await.1, 404);
}

#[actix_rt::test]
async fn delete_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.delete().await;

    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn loop_delete_add_documents() {
    let server = Server::new().await;
    let index = server.index("test");
    let documents = json!([{"id": 1, "field1": "hello"}]);
    for _ in 0..50 {
        let (response, code) = index.add_documents(documents.clone(), None).await;
        assert_eq!(code, 202, "{}", response);
        let (response, code) = index.delete().await;
        assert_eq!(code, 204, "{}", response);
    }
}
