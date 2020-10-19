mod common;

#[actix_rt::test]
async fn test_healthyness() {
    let mut server = common::Server::with_uid("movies");

    // Check that the server is healthy

    let (_response, status_code) = server.get_health().await;
    assert_eq!(status_code, 204);
}
