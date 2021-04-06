use crate::common::Server;
use serde_json::{json, Value};

#[actix_rt::test]
async fn set_and_reset_distinct_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, _code) = index.update_settings(json!({ "distinctAttribute": "test"})).await;
    index.wait_update_id(0).await;

    let (response, _) = index.settings().await;

    assert_eq!(response["distinctAttribute"], "test");

    index.update_settings(json!({ "distinctAttribute": Value::Null })).await;

    index.wait_update_id(1).await;

    let (response, _) = index.settings().await;

    assert_eq!(response["distinctAttribute"], Value::Null);
}
