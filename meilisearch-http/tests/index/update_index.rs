use crate::common::Server;
use chrono::DateTime;

#[actix_rt::test]
async fn update_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;

    assert_eq!(code, 200);

    let (response, code) = index.update(Some("primary")).await;

    assert_eq!(code, 200);
    assert_eq!(response["uid"], "test");
    assert_eq!(response["name"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());

    let created_at = DateTime::parse_from_rfc3339(response["createdAt"].as_str().unwrap()).unwrap();
    let updated_at = DateTime::parse_from_rfc3339(response["updatedAt"].as_str().unwrap()).unwrap();
    assert!(created_at < updated_at);

    assert_eq!(response["primaryKey"], "primary");
    assert_eq!(response.as_object().unwrap().len(), 5);
}

#[actix_rt::test]
async fn update_nothing() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.create(None).await;

    assert_eq!(code, 200);

    let (update, code) = index.update(None).await;

    assert_eq!(code, 200);
    assert_eq!(response, update);
}

// TODO: partial test since we are testing error, amd error is not yet fully implemented in
// transplant
#[actix_rt::test]
async fn update_existing_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.create(Some("primary")).await;

    assert_eq!(code, 200);

    let (_update, code) = index.update(Some("primary2")).await;

    assert_eq!(code, 400);
}

// TODO: partial test since we are testing error, amd error is not yet fully implemented in
// transplant
#[actix_rt::test]
async fn test_unexisting_index() {
    let server = Server::new().await;
    let (_response, code) = server.index("test").update(None).await;
    assert_eq!(code, 404);
}
