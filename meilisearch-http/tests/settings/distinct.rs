use crate::common::Server;
use serde_json::json;

#[actix_rt::test]
async fn set_and_reset_distinct_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, _code) = index
        .update_settings(json!({ "distinctAttribute": "test"}))
        .await;
    index.wait_update_id(0).await;

    let (response, _) = index.settings().await;

    assert_eq!(response["distinctAttribute"], "test");

    index
        .update_settings(json!({ "distinctAttribute": null }))
        .await;

    index.wait_update_id(1).await;

    let (response, _) = index.settings().await;

    assert_eq!(response["distinctAttribute"], json!(null));
}

#[actix_rt::test]
async fn set_and_reset_distinct_attribute_with_dedicated_route() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, _code) = index.update_distinct_attribute(json!("test")).await;
    index.wait_update_id(0).await;

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, "test");

    index.update_distinct_attribute(json!(null)).await;

    index.wait_update_id(1).await;

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, json!(null));
}
