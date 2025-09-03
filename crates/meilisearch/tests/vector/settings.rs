use meili_snap::{json_string, snapshot};

use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;
use crate::vector::generate_default_user_provided_documents;

#[actix_rt::test]
async fn field_unavailable_for_source() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": { "manual": {"source": "userProvided", "dimensions": 128, "documentTemplate": "{{doc.documentTemplate}}"}},
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.manual`: Field `documentTemplate` unavailable for source `userProvided`.\n  - note: `documentTemplate` is available for sources: `openAi`, `huggingFace`, `ollama`, `rest`\n  - note: available fields for source `userProvided`: `source`, `dimensions`, `distribution`, `binaryQuantized`",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": { "default": {"source": "openAi", "revision": "42"}},
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.default`: Field `revision` unavailable for source `openAi`.\n  - note: `revision` is available for sources: `huggingFace`\n  - note: available fields for source `openAi`: `source`, `model`, `apiKey`, `dimensions`, `documentTemplate`, `documentTemplateMaxBytes`, `url`, `distribution`, `binaryQuantized`",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
}

#[actix_rt::test]
async fn update_embedder() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": { "manual": {}},
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 2,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");

    let ret = server.wait_task(response.uid()).await;
    snapshot!(ret, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "manual": {
            "source": "userProvided",
            "dimensions": 2
          }
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn reset_embedder_documents() {
    let server = Server::new().await;
    let index = generate_default_user_provided_documents(&server).await;

    let (response, code) = index.delete_settings().await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    // Make sure the documents are still present
    let (documents, _code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir"
        },
        {
          "id": 1,
          "name": "echo"
        },
        {
          "id": 2,
          "name": "billou"
        },
        {
          "id": 3,
          "name": "intel"
        },
        {
          "id": 4,
          "name": "max"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 5
    }
    "###);

    // Make sure we are still able to retrieve their vectors
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
        },
        {
          "id": 2,
          "name": "billou",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  2.0,
                  2.0,
                  2.0
                ],
                [
                  2.0,
                  2.0,
                  3.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 3,
          "name": "intel",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  3.0,
                  3.0,
                  3.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 4,
          "name": "max",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  4.0,
                  4.0,
                  4.0
                ],
                [
                  4.0,
                  4.0,
                  5.0
                ]
              ],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 5
    }
    "###);

    // Make sure the vector DB has been cleared
    let (documents, _code) =
        index.search_post(json!({ "vector": [1, 1, 1], "hybrid": {"embedder": "default"} })).await;
    snapshot!(json_string!(documents), @r###"
    {
      "message": "Cannot find embedder with name `default`.",
      "code": "invalid_search_embedder",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_embedder"
    }
    "###);
}

#[actix_rt::test]
async fn ollama_url_checks() {
    let server = super::get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
  .update_settings(json!({
    "embedders": { "ollama": {"source": "ollama", "model": "toto", "dimensions": 1, "url": "http://localhost:11434/api/embeddings"}},
  }))
  .await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;

    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "ollama": {
            "source": "ollama",
            "model": "toto",
            "dimensions": 1,
            "url": "[url]"
          }
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
    .update_settings(json!({
      "embedders": { "ollama": {"source": "ollama", "model": "toto", "dimensions": 1, "url": "http://localhost:11434/api/embed"}},
    }))
    .await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;

    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "ollama": {
            "source": "ollama",
            "model": "toto",
            "dimensions": 1,
            "url": "[url]"
          }
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
      .update_settings(json!({
        "embedders": { "ollama": {"source": "ollama", "model": "toto", "dimensions": 1, "url": "http://localhost:11434/api/embedd"}},
      }))
      .await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;

    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "ollama": {
            "source": "ollama",
            "model": "toto",
            "dimensions": 1,
            "url": "[url]"
          }
        }
      },
      "error": {
        "message": "Index `doggo`: Error while generating embeddings: user error: unsupported Ollama URL.\n  - For `ollama` sources, the URL must end with `/api/embed` or `/api/embeddings`\n  - Got `http://localhost:11434/api/embedd`",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": { "ollama": {"source": "ollama", "model": "toto", "dimensions": 1, "url": "http://localhost:11434/v1/embeddings"}},
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;

    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "ollama": {
            "source": "ollama",
            "model": "toto",
            "dimensions": 1,
            "url": "[url]"
          }
        }
      },
      "error": {
        "message": "Index `doggo`: Error while generating embeddings: user error: unsupported Ollama URL.\n  - For `ollama` sources, the URL must end with `/api/embed` or `/api/embeddings`\n  - Got `http://localhost:11434/v1/embeddings`",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn composite_checks() {
    let server = Server::new().await;
    let index = server.index("test");
    // feature not enabled, using source
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "using `\"composite\"` as source requires enabling the `composite embedders` experimental feature. See https://github.com/orgs/meilisearch/discussions/816",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // feature not enabled, using search embedder
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "userProvided",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              }
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "setting `searchEmbedder` requires enabling the `composite embedders` experimental feature. See https://github.com/orgs/meilisearch/discussions/816",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // feature not enabled, using indexing embedder
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "userProvided",
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              }
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "setting `indexingEmbedder` requires enabling the `composite embedders` experimental feature. See https://github.com/orgs/meilisearch/discussions/816",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // enable feature
    let (_, code) = server.set_features(json!({"compositeEmbedders": true})).await;
    snapshot!(code, @"200 OK");

    // inner distribution
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                "distribution": {
                  "mean": 0.5,
                  "sigma": 0.2,
                }
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test.searchEmbedder`: Field `distribution` unavailable for source `huggingFace` for the search embedder.\n  - note: available fields for source `huggingFace` for the search embedder: `source`, `model`, `revision`, `pooling`\n  - note: `distribution` is available when source `huggingFace` is not for the search embedder",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // manual source
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "userProvided",
                "dimensions": 42,
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test.searchEmbedder.source`: Source `userProvided` is not available in a nested embedder",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // composite source
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "composite",
                "searchEmbedder": {
                  "source": "huggingFace",
                  "model": "sentence-transformers/all-MiniLM-L6-v2",
                  "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                },
                "indexingEmbedder": {
                  "source": "huggingFace",
                  "model": "sentence-transformers/all-MiniLM-L6-v2",
                  "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                }
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test.searchEmbedder.source`: Source `composite` is not available in a nested embedder",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // no source in indexing
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
              "indexingEmbedder": {},
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test.indexingEmbedder`: Missing field `source`.\n  - note: this field is mandatory for nested embedders",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // no source in search
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {},
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test.searchEmbedder`: Missing field `source`.\n  - note: this field is mandatory for nested embedders",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // no indexing
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test`: Missing field `indexingEmbedder` (note: this field is mandatory for source `composite`)",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // no search
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test`: Missing field `searchEmbedder` (note: this field is mandatory for source `composite`)",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // inner quantized
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                "binaryQuantized": true,
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                "binaryQuantized": false,
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test.searchEmbedder`: Field `binaryQuantized` unavailable for source `huggingFace` for the search embedder.\n  - note: available fields for source `huggingFace` for the search embedder: `source`, `model`, `revision`, `pooling`\n  - note: `binaryQuantized` is available when source `huggingFace` is not for the search embedder",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // prompt in search
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                "documentTemplate": "toto",
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.test.searchEmbedder`: Field `documentTemplate` unavailable for source `huggingFace` for the search embedder.\n  - note: available fields for source `huggingFace` for the search embedder: `source`, `model`, `revision`, `pooling`\n  - note: `documentTemplate` is available when source `huggingFace` is not for the search embedder",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
    // dimensions don't match
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "ollama",
                "dimensions": 0x42,
                "model": "does-not-exist",
              },
              "indexingEmbedder": {
                "source": "ollama",
                "dimensions": 42,
                "model": "does-not-exist",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "test": {
            "source": "composite",
            "searchEmbedder": {
              "source": "ollama",
              "model": "does-not-exist",
              "dimensions": 66
            },
            "indexingEmbedder": {
              "source": "ollama",
              "model": "does-not-exist",
              "dimensions": 42
            }
          }
        }
      },
      "error": {
        "message": "Index `test`: Error while generating embeddings: user error: error while generating test embeddings.\n  - the dimensions of embeddings produced at search time and at indexing time don't match.\n  - Search time dimensions: 66\n  - Indexing time dimensions: 42\n  - Note: Dimensions of embeddings produced by both embedders are required to match.",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
    // pooling don't match
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                "pooling": "forceMean"
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                "pooling": "forceCls"
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "test": {
            "source": "composite",
            "searchEmbedder": {
              "source": "huggingFace",
              "model": "sentence-transformers/all-MiniLM-L6-v2",
              "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              "pooling": "forceMean"
            },
            "indexingEmbedder": {
              "source": "huggingFace",
              "model": "sentence-transformers/all-MiniLM-L6-v2",
              "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              "pooling": "forceCls"
            }
          }
        }
      },
      "error": {
        "message": "Index `test`: Error while generating embeddings: user error: error while generating test embeddings.\n  - the embeddings produced at search time and indexing time are not similar enough.\n  - angular distance 0.25\n  - Meilisearch requires a maximum distance of 0.01.\n  - Note: check that both embedders produce similar embeddings.\n  - Make sure the `model`, `revision` and `pooling` of both embedders match.",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // ok
    let (response, _code) = index
        .update_settings(json!({
          "embedders": {
            "test": null
          }
        }))
        .await;
    server.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
            "test": {
              "source": "composite",
              "searchEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
              "indexingEmbedder": {
                "source": "huggingFace",
                "model": "sentence-transformers/all-MiniLM-L6-v2",
                "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
              },
           }
          }
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "test": {
            "source": "composite",
            "searchEmbedder": {
              "source": "huggingFace",
              "model": "sentence-transformers/all-MiniLM-L6-v2",
              "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e"
            },
            "indexingEmbedder": {
              "source": "huggingFace",
              "model": "sentence-transformers/all-MiniLM-L6-v2",
              "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e"
            }
          }
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}
