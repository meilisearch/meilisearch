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
