use serde_json::json;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::common::Server;

#[actix_rt::test]
async fn get_settings_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.version().await;
    assert_eq!(code, 200);
    let version = response.as_object().unwrap();
    assert!(version.get("commitSha").is_some());
    assert!(version.get("commitDate").is_some());
    assert!(version.get("pkgVersion").is_some());
}

#[actix_rt::test]
async fn test_healthyness() {
    let server = Server::new().await;

    let (response, status_code) = server.service.get("/health").await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "available");
}

#[actix_rt::test]
async fn stats() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(Some("id")).await;

    assert_eq!(code, 202);
    index.wait_task(0).await;

    let (response, code) = server.stats().await;

    assert_eq!(code, 200);
    assert!(response.get("databaseSize").is_some());
    assert!(response.get("lastUpdate").is_some());
    assert!(response["indexes"].get("test").is_some());
    assert_eq!(response["indexes"]["test"]["numberOfDocuments"], 0);
    assert!(response["indexes"]["test"]["isIndexing"] == false);

    let documents = json!([
        {
            "id": 1,
            "name": "Alexey",
        },
        {
            "id": 2,
            "age": 45,
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202, "{}", response);
    assert_eq!(response["taskUid"], 1);

    index.wait_task(1).await;

    let timestamp = OffsetDateTime::now_utc();
    let (response, code) = server.stats().await;

    assert_eq!(code, 200);
    assert!(response["databaseSize"].as_u64().unwrap() > 0);
    let last_update =
        OffsetDateTime::parse(response["lastUpdate"].as_str().unwrap(), &Rfc3339).unwrap();
    assert!(last_update - timestamp < time::Duration::SECOND);

    assert_eq!(response["indexes"]["test"]["numberOfDocuments"], 2);
    assert!(response["indexes"]["test"]["isIndexing"] == false);
    assert_eq!(response["indexes"]["test"]["fieldDistribution"]["id"], 2);
    assert_eq!(response["indexes"]["test"]["fieldDistribution"]["name"], 1);
    assert_eq!(response["indexes"]["test"]["fieldDistribution"]["age"], 1);
}
