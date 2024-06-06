use meili_snap::{json_string, snapshot};

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
              "userProvided": true
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
              "userProvided": true
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
              "userProvided": true
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
