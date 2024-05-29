mod errors;

use meili_snap::{json_string, snapshot};
use once_cell::sync::Lazy;

use crate::common::{Server, Value};
use crate::json;

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "release_year": 2019,
            "id": "287947",
            // Three semantic properties:
            // 1. magic, anything that reminds you of magic
            // 2. authority, anything that inspires command
            // 3. horror, anything that inspires fear or dread
            "_vectors": { "manual": [0.8, 0.4, -0.5]},
        },
        {
            "title": "Captain Marvel",
            "release_year": 2019,
            "id": "299537",
            "_vectors": { "manual": [0.6, 0.8, -0.2] },
        },
        {
            "title": "Escape Room",
            "release_year": 2019,
            "id": "522681",
            "_vectors": { "manual": [0.1, 0.6, 0.8] },
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "release_year": 2019,
            "id": "166428",
            "_vectors": { "manual": [0.7, 0.7, -0.4] },
        },
        {
            "title": "All Quiet on the Western Front",
            "release_year": 1930,
            "id": "143",
            "_vectors": { "manual": [-0.5, 0.3, 0.85] },
        }
    ])
});

#[actix_rt::test]
async fn basic() {
    let server = Server::new().await;
    let index = server.index("test");
    let (value, code) = server.set_features(json!({"vectorStore": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r###"
    {
      "vectorStore": true,
      "metrics": false,
      "logsRoute": false
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    index
        .similar(json!({"id": 143}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "Escape Room",
                "release_year": 2019,
                "id": "522681",
                "_vectors": {
                  "manual": [
                    0.1,
                    0.6,
                    0.8
                  ]
                }
              },
              {
                "title": "Captain Marvel",
                "release_year": 2019,
                "id": "299537",
                "_vectors": {
                  "manual": [
                    0.6,
                    0.8,
                    -0.2
                  ]
                }
              },
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "release_year": 2019,
                "id": "166428",
                "_vectors": {
                  "manual": [
                    0.7,
                    0.7,
                    -0.4
                  ]
                }
              },
              {
                "title": "Shazam!",
                "release_year": 2019,
                "id": "287947",
                "_vectors": {
                  "manual": [
                    0.8,
                    0.4,
                    -0.5
                  ]
                }
              }
            ]
            "###);
        })
        .await;

    index
        .similar(json!({"id": "299537"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "release_year": 2019,
                "id": "166428",
                "_vectors": {
                  "manual": [
                    0.7,
                    0.7,
                    -0.4
                  ]
                }
              },
              {
                "title": "Shazam!",
                "release_year": 2019,
                "id": "287947",
                "_vectors": {
                  "manual": [
                    0.8,
                    0.4,
                    -0.5
                  ]
                }
              },
              {
                "title": "Escape Room",
                "release_year": 2019,
                "id": "522681",
                "_vectors": {
                  "manual": [
                    0.1,
                    0.6,
                    0.8
                  ]
                }
              },
              {
                "title": "All Quiet on the Western Front",
                "release_year": 1930,
                "id": "143",
                "_vectors": {
                  "manual": [
                    -0.5,
                    0.3,
                    0.85
                  ]
                }
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn filter() {
    let server = Server::new().await;
    let index = server.index("test");
    let (value, code) = server.set_features(json!({"vectorStore": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r###"
    {
      "vectorStore": true,
      "metrics": false,
      "logsRoute": false
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title", "release_year"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    index
        .similar(json!({"id": 522681, "filter": "release_year = 2019"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "Captain Marvel",
                "release_year": 2019,
                "id": "299537",
                "_vectors": {
                  "manual": [
                    0.6,
                    0.8,
                    -0.2
                  ]
                }
              },
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "release_year": 2019,
                "id": "166428",
                "_vectors": {
                  "manual": [
                    0.7,
                    0.7,
                    -0.4
                  ]
                }
              },
              {
                "title": "Shazam!",
                "release_year": 2019,
                "id": "287947",
                "_vectors": {
                  "manual": [
                    0.8,
                    0.4,
                    -0.5
                  ]
                }
              }
            ]
            "###);
        })
        .await;

    index
        .similar(json!({"id": 522681, "filter": "release_year < 2000"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "All Quiet on the Western Front",
                "release_year": 1930,
                "id": "143",
                "_vectors": {
                  "manual": [
                    -0.5,
                    0.3,
                    0.85
                  ]
                }
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn limit_and_offset() {
    let server = Server::new().await;
    let index = server.index("test");
    let (value, code) = server.set_features(json!({"vectorStore": true})).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r###"
    {
      "vectorStore": true,
      "metrics": false,
      "logsRoute": false
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    index
        .similar(json!({"id": 143, "limit": 1}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "Escape Room",
                "release_year": 2019,
                "id": "522681",
                "_vectors": {
                  "manual": [
                    0.1,
                    0.6,
                    0.8
                  ]
                }
              }
            ]
            "###);
        })
        .await;

    index
        .similar(json!({"id": 143, "limit": 1, "offset": 1}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "Captain Marvel",
                "release_year": 2019,
                "id": "299537",
                "_vectors": {
                  "manual": [
                    0.6,
                    0.8,
                    -0.2
                  ]
                }
              }
            ]
            "###);
        })
        .await;
}
