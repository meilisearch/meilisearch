use serde_json::json;
use std::convert::Into;

mod common;

#[actix_rt::test]
async fn test_healthyness() {
    let mut server = common::Server::with_uid("movies");

    // Check that the server is healthy

    let (_response, status_code) = server.get_health().await;
    assert_eq!(status_code, 200);

    // Set the serve Unhealthy
    let body = json!({
        "health": false,
    });
    let (_response, status_code) = server.update_health(body).await;
    assert_eq!(status_code, 200);

    // Check that the server is unhealthy

    let (_response, status_code) = server.get_health().await;
    assert_eq!(status_code, 503);

    // Set the server healthy
    let body = json!({
        "health": true,
    });
    let (_response, status_code) = server.update_health(body).await;
    assert_eq!(status_code, 200);

    // Check if the server is healthy

    let (_response, status_code) = server.get_health().await;
    assert_eq!(status_code, 200);
}
