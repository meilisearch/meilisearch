use crate::common::Server;

#[actix_rt::test]
async fn get_settings_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.version().await;
    assert_eq!(code, 200);
    let version = response.as_object().unwrap();
    assert!(version.get("commitSha").is_some());
    assert!(version.get("buildDate").is_some());
    assert!(version.get("pkgVersion").is_some());
}

#[actix_rt::test]
async fn test_healthyness() {
    let server = Server::new().await;

    let (response, status_code) = server.service.get("/health").await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "available");
}
