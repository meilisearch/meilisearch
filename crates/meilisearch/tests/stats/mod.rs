use meili_snap::{json_string, snapshot};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::common::Server;
use crate::json;

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
    let (task, code) = index.create(Some("id")).await;

    assert_eq!(code, 202);
    index.wait_task(task.uid()).await.succeeded();

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

    index.wait_task(response.uid()).await.succeeded();

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

#[actix_rt::test]
async fn add_remove_embeddings() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            },
            "handcrafted": {
                "source": "userProvided",
                "dimensions": 3,
            },

          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    // 2 embedded documents for 5 embeddings in total
    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0], "handcrafted": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1], "handcrafted": [[1, 1, 1], [2, 2, 2]] }},
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 2,
      "isIndexing": false,
      "numberOfEmbeddings": 5,
      "numberOfEmbeddedDocuments": 2,
      "fieldDistribution": {
        "id": 2,
        "name": 2
      }
    }
    "###);

    // 2 embedded documents for 3 embeddings in total
    let documents = json!([
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1], "handcrafted": null }},
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 2,
      "isIndexing": false,
      "numberOfEmbeddings": 3,
      "numberOfEmbeddedDocuments": 2,
      "fieldDistribution": {
        "id": 2,
        "name": 2
      }
    }
    "###);

    // 2 embedded documents for 2 embeddings in total
    let documents = json!([
        {"id": 0, "name": "kefir", "_vectors": { "manual": null, "handcrafted": [0, 0, 0] }},
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 2,
      "isIndexing": false,
      "numberOfEmbeddings": 2,
      "numberOfEmbeddedDocuments": 2,
      "fieldDistribution": {
        "id": 2,
        "name": 2
      }
    }
    "###);

    // 1 embedded documents for 2 embeddings in total
    let documents = json!([
        {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0], "handcrafted": [0, 0, 0] }},
        {"id": 1, "name": "echo", "_vectors": { "manual": null, "handcrafted": null }},
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 2,
      "isIndexing": false,
      "numberOfEmbeddings": 2,
      "numberOfEmbeddedDocuments": 1,
      "fieldDistribution": {
        "id": 2,
        "name": 2
      }
    }
    "###);
}

#[actix_rt::test]
async fn add_remove_embedded_documents() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            },
            "handcrafted": {
                "source": "userProvided",
                "dimensions": 3,
            },

          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    // 2 embedded documents for 5 embeddings in total
    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0], "handcrafted": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1], "handcrafted": [[1, 1, 1], [2, 2, 2]] }},
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 2,
      "isIndexing": false,
      "numberOfEmbeddings": 5,
      "numberOfEmbeddedDocuments": 2,
      "fieldDistribution": {
        "id": 2,
        "name": 2
      }
    }
    "###);

    // delete one embedded document, remaining 1 embedded documents for 3 embeddings in total
    let (response, code) = index.delete_document(0).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 1,
      "isIndexing": false,
      "numberOfEmbeddings": 3,
      "numberOfEmbeddedDocuments": 1,
      "fieldDistribution": {
        "id": 1,
        "name": 1
      }
    }
    "###);
}

#[actix_rt::test]
async fn update_embedder_settings() {
    let server = Server::new().await;
    let index = server.index("doggo");

    // 2 embedded documents for 3 embeddings in total
    // but no embedders are added in the settings yet so we expect 0 embedded documents for 0 embeddings in total
    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0], "handcrafted": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1], "handcrafted": null }},
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 2,
      "isIndexing": false,
      "numberOfEmbeddings": 0,
      "numberOfEmbeddedDocuments": 0,
      "fieldDistribution": {
        "id": 2,
        "name": 2
      }
    }
    "###);

    // add embedders to the settings
    // 2 embedded documents for 3 embeddings in total
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            },
            "handcrafted": {
                "source": "userProvided",
                "dimensions": 3,
            },

          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (stats, _code) = index.stats().await;
    snapshot!(json_string!(stats), @r###"
    {
      "numberOfDocuments": 2,
      "isIndexing": false,
      "numberOfEmbeddings": 3,
      "numberOfEmbeddedDocuments": 2,
      "fieldDistribution": {
        "id": 2,
        "name": 2
      }
    }
    "###);
}
