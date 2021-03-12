use crate::common::Server;

#[actix_rt::test]
async fn get_update_unexisting_index() {
    let server = Server::new().await;
    let (_response, code) = server.index("test").get_update(0).await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn get_unexisting_update_status() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    let (_response, code) = index.get_update(0).await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn get_update_status() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    index.add_documents(
        serde_json::json!([{
            "id": 1,
            "content": "foobar",
        }]),
        None
    ).await;
    let (_response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    // TODO check resonse format, as per #48
}

#[actix_rt::test]
async fn list_updates_unexisting_index() {
    let server = Server::new().await;
    let (_response, code) = server.index("test").list_updates().await;
    assert_eq!(code, 400);
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
    index.add_documents(
        serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(),
        None
    ).await;
    let (response, code) = index.list_updates().await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 1);
}
