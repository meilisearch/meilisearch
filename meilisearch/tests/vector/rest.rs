use crate::vector::GetAllDocumentsOptions;
use meili_snap::{json_string, snapshot};
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::common::{Server, Value};
use crate::json;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

async fn create_mock() -> (MockServer, Value) {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(|_req: &Request| {
            let cpt = COUNTER.fetch_add(1, Ordering::Relaxed);
            ResponseTemplate::new(200).set_body_json(json!({ "data": vec![cpt; 3] }))
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let embedder_settings = json!({
        "source": "rest",
        "url": url,
        "dimensions": 3,
        "query": {},
    });

    (mock_server, embedder_settings)
}

#[actix_rt::test]
async fn dummy_testing_the_mock() {
    let (mock, _setting) = create_mock().await;
    let body = reqwest::get(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @"[0,0,0]");
    let body = reqwest::get(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @"[1,1,1]");
    let body = reqwest::get(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @"[2,2,2]");
    let body = reqwest::get(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @"[3,3,3]");
    let body = reqwest::get(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @"[4,4,4]");
}

async fn get_server_vector() -> Server {
    let server = Server::new().await;
    let (value, code) = server.set_features(json!({"vectorStore": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r###"
    {
      "vectorStore": true,
      "metrics": false,
      "logsRoute": false
    }
    "###);
    server
}

#[actix_rt::test]
async fn bad_settings() {
    let (mock, _setting) = create_mock().await;

    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.rest`: Missing field `url` (note: this field is mandatory for source rest)",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": "kefir" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.rest.url`: could not parse `kefir`: relative URL without a base",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri() }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": 0,
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]"
          }
        }
      },
      "error": {
        "message": "internal: Error while generating embeddings: runtime error: could not determine model dimensions: test embedding failed with user error: was expected 'input' to be an object in query 'null'.",
        "code": "internal",
        "type": "internal",
        "link": "https://docs.meilisearch.com/errors#internal"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "query": {} }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": 1,
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "query": {}
          }
        }
      },
      "error": {
        "message": "internal: Error while generating embeddings: runtime error: could not determine model dimensions: test embedding failed with error: component `embedding` not found in path `embedding` in response: `{\n  \"data\": [\n    0,\n    0,\n    0\n  ]\n}`.",
        "code": "internal",
        "type": "internal",
        "link": "https://docs.meilisearch.com/errors#internal"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "query": {}, "pathToEmbeddings": ["data"] }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": 2,
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "query": {},
            "pathToEmbeddings": [
              "data"
            ]
          }
        }
      },
      "error": {
        "message": "internal: Error while generating embeddings: runtime error: could not determine model dimensions: test embedding failed with error: component `embedding` not found in path `embedding` in response: `{\n  \"data\": [\n    1,\n    1,\n    1\n  ]\n}`.",
        "code": "internal",
        "type": "internal",
        "link": "https://docs.meilisearch.com/errors#internal"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "query": {}, "embeddingObject": ["data"] }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": 3,
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "query": {},
            "embeddingObject": [
              "data"
            ]
          }
        }
      },
      "error": {
        "message": "internal: Error while generating embeddings: runtime error: could not determine model dimensions: test embedding failed with error: component `data` not found in path `data` in response: `{\n  \"data\": [\n    2,\n    2,\n    2\n  ]\n}`.",
        "code": "internal",
        "type": "internal",
        "link": "https://docs.meilisearch.com/errors#internal"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // Validate an embedder with a bad dimension of 2 instead of 3
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "query": {}, "pathToEmbeddings": [], "embeddingObject": ["data"], "dimensions": 2 }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);

    let (response, code) = index.add_documents(json!( { "id": 1, "name": "kefir" }), None).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": 5,
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "An unexpected crash occurred when processing the task.",
        "code": "internal",
        "type": "internal",
        "link": "https://docs.meilisearch.com/errors#internal"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn add_vector_and_user_provided() {
    let (_mock, setting) = create_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir"},
      {"id": 1, "name": "echo", "_vectors": { "rest": [1, 1, 1] }},
      {"id": 2, "name": "intel"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @"");

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [],
      "offset": 0,
      "limit": 20,
      "total": 0
    }
    "###);
}
