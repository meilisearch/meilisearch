use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn set_and_reset_distinct_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task1, _code) = index.update_settings(json!({ "distinctAttribute": "test"})).await;
    index.wait_task(task1.uid()).await.succeeded();

    let (response, _) = index.settings().await;

    assert_eq!(response["distinctAttribute"], "test");

    let (task2, _status_code) = index.update_settings(json!({ "distinctAttribute": null })).await;

    index.wait_task(task2.uid()).await.succeeded();

    let (response, _) = index.settings().await;

    assert_eq!(response["distinctAttribute"], json!(null));
}

#[actix_rt::test]
async fn set_and_reset_distinct_attribute_with_dedicated_route() {
    let server = Server::new().await;
    let index = server.index("test");

    let (update_task1, _code) = index.update_distinct_attribute(json!("test")).await;
    index.wait_task(update_task1.uid()).await.succeeded();

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, "test");

    let (update_task2, _status_code) = index.update_distinct_attribute(json!(null)).await;

    index.wait_task(update_task2.uid()).await.succeeded();

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, json!(null));
}
