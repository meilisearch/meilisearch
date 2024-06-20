mod settings;

use meili_snap::{json_string, snapshot};

use crate::common::index::Index;
use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;

#[actix_rt::test]
async fn add_remove_user_provided() {
    let server = Server::new().await;
    let index = server.index("doggo");
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
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1] }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [10, 10, 10] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": null }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  10.0,
                  10.0,
                  10.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {}
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);

    let (value, code) = index.delete_document(0).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 1,
          "name": "echo",
          "_vectors": {}
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
}

async fn generate_default_user_provided_documents(server: &Server) -> Index {
    let index = server.index("doggo");
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
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1] }},
      {"id": 2, "name": "billou", "_vectors": { "manual": [[2, 2, 2], [2, 2, 3]] }},
      {"id": 3, "name": "intel", "_vectors": { "manual": { "regenerate": false, "embeddings": [3, 3, 3] }}},
      {"id": 4, "name": "max", "_vectors": { "manual": { "regenerate": false, "embeddings": [[4, 4, 4], [4, 4, 5]] }}},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    index
}

#[actix_rt::test]
async fn clear_documents() {
    let server = Server::new().await;
    let index = generate_default_user_provided_documents(&server).await;

    let (value, _code) = index.clear_all_documents().await;
    index.wait_task(value.uid()).await;

    // Make sure the documents DB has been cleared
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

    // Make sure the arroy DB has been cleared
    let (documents, _code) = index.search_post(json!({ "vector": [1, 1, 1] })).await;
    snapshot!(json_string!(documents), @r###"
    {
      "hits": [],
      "query": "",
      "processingTimeMs": 0,
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "semanticHitCount": 0
    }
    "###);
}

#[actix_rt::test]
async fn add_remove_one_vector_4588() {
    // https://github.com/meilisearch/meilisearch/issues/4588
    let server = Server::new().await;
    let index = server.index("doggo");
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
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, name: "settings-processed");

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0] }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, name: "document-added");

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": null }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, name: "document-deleted");

    let (documents, _code) = index.search_post(json!({"vector": [1, 1, 1] })).await;
    snapshot!(json_string!(documents), @r###"
    {
      "hits": [
        {
          "id": 0,
          "name": "kefir"
        }
      ],
      "query": "",
      "processingTimeMs": 1,
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1,
      "semanticHitCount": 1
    }
    "###);

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {}
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
}
