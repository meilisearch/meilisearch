use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn stats() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(Some("id")).await;

    assert_eq!(code, 201);

    let (response, code) = index.stats().await;

    assert_eq!(code, 200);
    assert_eq!(response["numberOfDocuments"], 0);
    assert!(response["isIndexing"] == false);
    assert!(response["fieldDistribution"]
        .as_object()
        .unwrap()
        .is_empty());

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
    assert_eq!(code, 202);
    assert_eq!(response["updateId"], 0);

    index.wait_update_id(0).await;

    let (response, code) = index.stats().await;

    assert_eq!(code, 200);
    assert_eq!(response["numberOfDocuments"], 2);
    assert!(response["isIndexing"] == false);
    assert_eq!(response["fieldDistribution"]["id"], 2);
    assert_eq!(response["fieldDistribution"]["name"], 1);
    assert_eq!(response["fieldDistribution"]["age"], 1);
}
