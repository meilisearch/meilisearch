use std::collections::BTreeMap;

use meili_snap::{json_string, snapshot};
use tokio::sync::OnceCell;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::common::index::Index;
use crate::common::Shared;
use crate::common::Value;
use crate::json;
use crate::vector::Server;
use crate::vector::{get_server_vector, GetAllDocumentsOptions};

async fn shared_index_for_fragments() -> Index<'static, Shared> {
    static INDEX: OnceCell<(Server<Shared>, String)> = OnceCell::const_new();
    let (server, uid) = INDEX
        .get_or_init(|| async {
            let (_mock, settings) = create_mock(
                json!({
                    "withBreed": {"value": "{{ doc.name }} is a {{ doc.breed }}"},
                    "basic": {"value": "{{ doc.name }} is a dog"},
                }),
                json!({
                    "justBreed": {"value": "It's a {{ media.breed }}"},
                    "justName": {"value": "{{ media.name }} is a dog"},
                    "query": {"value": "Some pre-prompt for query {{ q }}"},
                }),
            )
            .await;

            let server = Server::new().await;
            let index = server.unique_index();

            let (_response, code) = server.set_features(json!({"multimodal": true})).await;
            snapshot!(code, @"200 OK");

            // Configure the index to use our mock embedder
            let (response, code) = index
                .update_settings(json!({
                    "embedders": {
                        "rest": settings,
                    },
                }))
                .await;
            snapshot!(code, @"202 Accepted");

            let task = server.wait_task(response.uid()).await;
            snapshot!(task["status"], @r###""succeeded""###);

            // Send documents
            let documents = json!([
                {"id": 0, "name": "kefir"},
                {"id": 1, "name": "echo", "_vectors": { "rest": [1, 1, 1] }},
                {"id": 2, "name": "intel", "breed": "labrador"},
                {"id": 3, "name": "dustin", "breed": "bulldog"},
            ]);
            let (value, code) = index.add_documents(documents, None).await;
            snapshot!(code, @"202 Accepted");

            let task = index.wait_task(value.uid()).await;
            snapshot!(task["status"], @r###""succeeded""###);

            let uid = index.uid.clone();
            (server.into_shared(), uid)
        })
        .await;
    server._index(uid).to_shared()
}

async fn create_mock(indexing_fragments: Value, search_fragments: Value) -> (MockServer, Value) {
    let mock_server = MockServer::start().await;

    let text_to_embedding: BTreeMap<_, _> = vec![
        ("kefir", [0.5, -0.5, 0.0]),
        ("intel", [1.0, 1.0, 0.0]),
        ("dustin", [-0.5, 0.5, 0.0]),
        ("bulldog", [0.0, 0.0, 1.0]),
        ("labrador", [0.0, 0.0, -1.0]),
    ]
    .into_iter()
    .collect();

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            let text = String::from_utf8_lossy(&req.body).to_string();
            let mut data = [0.0, 0.0, 0.0];
            for (inner_text, inner_data) in &text_to_embedding {
                if text.contains(inner_text) {
                    for (i, &value) in inner_data.iter().enumerate() {
                        data[i] += value;
                    }
                }
            }
            ResponseTemplate::new(200).set_body_json(json!({ "data": data }))
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let embedder_settings = json!({
        "source": "rest",
        "url": url,
        "dimensions": 3,
        "request": "{{fragment}}",
        "response": {
          "data": "{{embedding}}"
        },
        "indexingFragments": indexing_fragments,
        "searchFragments": search_fragments,
        "documentTemplate": "document template: {{dog.name}}",
    });

    (mock_server, embedder_settings)
}

#[actix_rt::test]
async fn indexing_fragments() {
    let index = shared_index_for_fragments().await;

    // Make sure the documents have been indexed and their embeddings retrieved
    let (documents, code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r#"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  0.5,
                  -0.5,
                  0.0
                ]
              ],
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "rest": {
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
        },
        {
          "id": 2,
          "name": "intel",
          "breed": "labrador",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  0.0
                ],
                [
                  1.0,
                  1.0,
                  -1.0
                ]
              ],
              "regenerate": true
            }
          }
        },
        {
          "id": 3,
          "name": "dustin",
          "breed": "bulldog",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  -0.5,
                  0.5,
                  0.0
                ],
                [
                  -0.5,
                  0.5,
                  1.0
                ]
              ],
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "#);
}

#[actix_rt::test]
async fn search_with_vector() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index.search_post(
        json!({"vector": [1.0, 1.0, 1.0], "hybrid": {"semanticRatio": 1.0, "embedder": "rest"}, "limit": 1}
    )).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "hits": [
        {
          "id": 1,
          "name": "echo"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 1,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 1
    }
    "#);
}

#[actix_rt::test]
async fn search_with_media() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .search_post(json!({
                "media": { "breed": "labrador" },
                "hybrid": {"semanticRatio": 1.0, "embedder": "rest"},
                "limit": 1
            }
        ))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "hits": [
        {
          "id": 2,
          "name": "intel",
          "breed": "labrador"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 1,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 1
    }
    "#);
}

#[actix_rt::test]
async fn search_with_media_matching_multiple_fragments() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .search_post(json!({
                "media": { "name": "dustin", "breed": "labrador" },
                "hybrid": {"semanticRatio": 1.0, "embedder": "rest"},
                "limit": 1
            }
        ))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Error while generating embeddings: user error: Query matches multiple search fragments.\n  - Note: First matched fragment `justBreed`.\n  - Note: Second matched fragment `justName`.\n  - Note: {\"q\":null,\"media\":{\"name\":\"dustin\",\"breed\":\"labrador\"}}",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "#);
}

#[actix_rt::test]
async fn search_with_media_matching_no_fragment() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .search_post(json!({
                "media": { "ticker": "GME", "section": "portfolio" },
                "hybrid": {"semanticRatio": 1.0, "embedder": "rest"},
                "limit": 1
            }
        ))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Error while generating embeddings: user error: Query matches no search fragment.\n  - Note: {\"q\":null,\"media\":{\"ticker\":\"GME\",\"section\":\"portfolio\"}}",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "#);
}

#[actix_rt::test]
async fn search_with_query() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .search_post(json!({
                "q": "bulldog",
                "hybrid": {"semanticRatio": 1.0, "embedder": "rest"},
                "limit": 1
            }
        ))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "hits": [
        {
          "id": 3,
          "name": "dustin",
          "breed": "bulldog"
        }
      ],
      "query": "bulldog",
      "processingTimeMs": "[duration]",
      "limit": 1,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 1
    }
    "#);
}
