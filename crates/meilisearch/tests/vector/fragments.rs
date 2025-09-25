use meili_snap::{json_string, snapshot};

use crate::common::{
    init_fragments_index, init_fragments_index_composite, shared_index_for_fragments,
};
use crate::json;
use crate::vector::{GetAllDocumentsOptions, Server};

#[actix_rt::test]
async fn experimental_feature_not_enabled() {
    let server = Server::new().await;
    let index = server.unique_index();

    let settings = json!({
        "embedders": {
            "rest": {
                "source": "rest",
                "url": "http://localhost:1337",
                "dimensions": 3,
                "request": "{{fragment}}",
                "response": {
                    "data": "{{embedding}}"
                },
                "indexingFragments": {
                    "basic": {"value": "{{ doc.name }} is a dog"},
                },
                "searchFragments": {
                    "query": {"value": "Some pre-prompt for query {{ q }}"},
                }
            },
        },
    });
    let (response, code) = index.update_settings(settings.clone()).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r#"
    {
      "message": "setting `indexingFragments` requires enabling the `multimodal` experimental feature. See https://github.com/orgs/meilisearch/discussions/846",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "#);
}

#[actix_rt::test]
async fn indexing_fragments() {
    let index = shared_index_for_fragments().await;

    // Make sure the documents have been indexed and their embeddings retrieved
    let (documents, code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(documents, @r#"
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
async fn replace_document() {
    let (server, uid, _settings) = init_fragments_index().await;
    let index = server.index(uid);

    let documents = json!([
        { "id": 0, "name": "kefir", "breed": "sorry-I-forgot" },
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");

    server.wait_task(value.uid()).await.succeeded();

    // Make sure kefir now has 2 vectors
    let (documents, code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(documents, @r#"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "breed": "sorry-I-forgot",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  0.5,
                  -0.5,
                  0.0
                ],
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
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
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
      "requestUid": "[uuid]",
      "semanticHitCount": 1
    }
    "###);
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
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
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
      "requestUid": "[uuid]",
      "semanticHitCount": 1
    }
    "###);
}

#[actix_rt::test]
async fn search_with_media_and_vector() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .search_post(json!({
                "vector": [1.0, 1.0, 1.0],
                "media": { "breed": "labrador" },
                "hybrid": {"semanticRatio": 1.0, "embedder": "rest"},
                "limit": 1
            }
        ))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Invalid request: both `media` and `vector` parameters are present.",
      "code": "invalid_search_media_and_vector",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_media_and_vector"
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
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
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
      "requestUid": "[uuid]",
      "semanticHitCount": 1
    }
    "###);
}

#[actix_rt::test]
async fn deleting_fragments_deletes_vectors() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    settings["embedders"]["rest"]["indexingFragments"]["basic"] = serde_json::Value::Null;

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    let value = server.wait_task(response.uid()).await.succeeded();
    snapshot!(value, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": null,
              "withBreed": {
                "value": "{{ doc.name }} is a {{ doc.breed }}"
              }
            },
            "searchFragments": {
              "justBreed": {
                "value": "It's a {{ media.breed }}"
              },
              "justName": {
                "value": "{{ media.name }} is a dog"
              },
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);

    let (value, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["embedders"], {
        ".rest.url" => "[url]",
    }), @r#"
    {
      "rest": {
        "source": "rest",
        "dimensions": 3,
        "url": "[url]",
        "indexingFragments": {
          "withBreed": {
            "value": "{{ doc.name }} is a {{ doc.breed }}"
          }
        },
        "searchFragments": {
          "justBreed": {
            "value": "It's a {{ media.breed }}"
          },
          "justName": {
            "value": "{{ media.name }} is a dog"
          },
          "query": {
            "value": "Some pre-prompt for query {{ q }}"
          }
        },
        "request": "{{fragment}}",
        "response": {
          "data": "{{embedding}}"
        },
        "headers": {}
      }
    }
    "#);

    let (documents, code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "rest": {
              "embeddings": [],
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
    "###);
}

#[actix_rt::test]
async fn modifying_fragments_modifies_vectors() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    settings["embedders"]["rest"]["indexingFragments"]["basic"]["value"] =
        serde_json::Value::String("{{ doc.name }} is a dog (maybe bulldog?)".to_string());

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    let value = server.wait_task(response.uid()).await.succeeded();
    snapshot!(value, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": {
                "value": "{{ doc.name }} is a dog (maybe bulldog?)"
              },
              "withBreed": {
                "value": "{{ doc.name }} is a {{ doc.breed }}"
              }
            },
            "searchFragments": {
              "justBreed": {
                "value": "It's a {{ media.breed }}"
              },
              "justName": {
                "value": "{{ media.name }} is a dog"
              },
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);

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
                  1.0
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
                  1.0
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
                  1.0
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
async fn swapping_fragments() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    let basic = settings["embedders"]["rest"]["indexingFragments"]["basic"].clone();
    let with_breed = settings["embedders"]["rest"]["indexingFragments"]["withBreed"].clone();
    settings["embedders"]["rest"]["indexingFragments"]["basic"] = with_breed;
    settings["embedders"]["rest"]["indexingFragments"]["withBreed"] = basic;

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    let value = server.wait_task(response.uid()).await.succeeded();
    snapshot!(value, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": {
                "value": "{{ doc.name }} is a {{ doc.breed }}"
              },
              "withBreed": {
                "value": "{{ doc.name }} is a dog"
              }
            },
            "searchFragments": {
              "justBreed": {
                "value": "It's a {{ media.breed }}"
              },
              "justName": {
                "value": "{{ media.name }} is a dog"
              },
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);

    let (documents, code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(documents, @r#"
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
                      -1.0
                    ],
                    [
                      1.0,
                      1.0,
                      0.0
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
                      1.0
                    ],
                    [
                      -0.5,
                      0.5,
                      0.0
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
async fn ommitted_fragment_isnt_removed() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    settings["embedders"]["rest"]["indexingFragments"]["basic"] = serde_json::Value::Null; // basic is removed
    settings["embedders"]["rest"]["indexingFragments"].as_object_mut().unwrap().remove("withBreed"); // withBreed isn't specified

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    let value = server.wait_task(response.uid()).await.succeeded();
    snapshot!(value, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": null
            },
            "searchFragments": {
              "justBreed": {
                "value": "It's a {{ media.breed }}"
              },
              "justName": {
                "value": "{{ media.name }} is a dog"
              },
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);

    // Make sure withBreed is still here because it wasn't specified
    let (value, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["embedders"], {
        ".rest.url" => "[url]",
    }), @r#"
    {
      "rest": {
        "source": "rest",
        "dimensions": 3,
        "url": "[url]",
        "indexingFragments": {
          "withBreed": {
            "value": "{{ doc.name }} is a {{ doc.breed }}"
          }
        },
        "searchFragments": {
          "justBreed": {
            "value": "It's a {{ media.breed }}"
          },
          "justName": {
            "value": "{{ media.name }} is a dog"
          },
          "query": {
            "value": "Some pre-prompt for query {{ q }}"
          }
        },
        "request": "{{fragment}}",
        "response": {
          "data": "{{embedding}}"
        },
        "headers": {}
      }
    }
    "#);
}

#[actix_rt::test]
async fn fragment_insertion() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    settings["embedders"]["rest"]["indexingFragments"].as_object_mut().unwrap().insert(
        String::from("useless"),
        serde_json::json!({
            "value": "This fragment is useless"
        }),
    );

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    let value = server.wait_task(response.uid()).await.succeeded();
    snapshot!(value, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": {
                "value": "{{ doc.name }} is a dog"
              },
              "useless": {
                "value": "This fragment is useless"
              },
              "withBreed": {
                "value": "{{ doc.name }} is a {{ doc.breed }}"
              }
            },
            "searchFragments": {
              "justBreed": {
                "value": "It's a {{ media.breed }}"
              },
              "justName": {
                "value": "{{ media.name }} is a dog"
              },
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);

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
                ],
                [
                  0.0,
                  0.0,
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
                ],
                [
                  0.0,
                  0.0,
                  0.0
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
                ],
                [
                  0.0,
                  0.0,
                  0.0
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
async fn multiple_embedders() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    let url = settings["embedders"]["rest"]["url"].as_str().unwrap();

    let settings2 = json!({
        "embedders": {
            "rest2": {
                "source": "rest",
                "url": url,
                "dimensions": 3,
                "request": "{{fragment}}",
                "response": {
                "data": "{{embedding}}"
                },
                "indexingFragments": {
                    "withBreed": {"value": "{{ doc.name }} is a {{ doc.breed }}"},
                    "basic": {"value": "{{ doc.name }} is a dog"},
                },
                "searchFragments": {
                    "query": {"value": "Some pre-prompt for query {{ q }}"},
                }
            },
            "rest3": {
                "source": "rest",
                "url": url,
                "dimensions": 3,
                "request": "{{fragment}}",
                "response": {
                "data": "{{embedding}}"
                },
                "indexingFragments": {
                    "basic": {"value": "{{ doc.name }} is a dog"},
                },
                "searchFragments": {
                    "query": {"value": "Some pre-prompt for query {{ q }}"},
                }
            },
        },
    });
    let (response, code) = index.update_settings(settings2).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest2": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": {
                "value": "{{ doc.name }} is a dog"
              },
              "withBreed": {
                "value": "{{ doc.name }} is a {{ doc.breed }}"
              }
            },
            "searchFragments": {
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
            }
          },
          "rest3": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": {
                "value": "{{ doc.name }} is a dog"
              }
            },
            "searchFragments": {
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);

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
            },
            "rest2": {
              "embeddings": [
                [
                  0.5,
                  -0.5,
                  0.0
                ]
              ],
              "regenerate": true
            },
            "rest3": {
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
            },
            "rest2": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
            },
            "rest3": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
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
            },
            "rest2": {
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
            },
            "rest3": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  0.0
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
            },
            "rest2": {
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
            },
            "rest3": {
              "embeddings": [
                [
                  -0.5,
                  0.5,
                  0.0
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

    // Remove Rest2

    settings["embedders"]["rest2"] = serde_json::Value::Null;

    let (response, code) = index.update_settings(settings.clone()).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

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
            },
            "rest3": {
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
            },
            "rest3": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
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
            },
            "rest3": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  0.0
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
            },
            "rest3": {
              "embeddings": [
                [
                  -0.5,
                  0.5,
                  0.0
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

    // Remove rest's basic fragment

    settings["embedders"]["rest"]["indexingFragments"]["basic"] = serde_json::Value::Null;
    //settings["embedders"].as_object_mut().unwrap().remove("rest2");

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

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
              "embeddings": [],
              "regenerate": true
            },
            "rest3": {
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
            },
            "rest3": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
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
                  -1.0
                ]
              ],
              "regenerate": true
            },
            "rest3": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  0.0
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
                  1.0
                ]
              ],
              "regenerate": true
            },
            "rest3": {
              "embeddings": [
                [
                  -0.5,
                  0.5,
                  0.0
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
async fn remove_non_existant_embedder() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    settings["embedders"]
        .as_object_mut()
        .unwrap()
        .insert(String::from("non-existant"), serde_json::Value::Null);

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "non-existant": null,
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": {
                "value": "{{ doc.name }} is a dog"
              },
              "withBreed": {
                "value": "{{ doc.name }} is a {{ doc.breed }}"
              }
            },
            "searchFragments": {
              "justBreed": {
                "value": "It's a {{ media.breed }}"
              },
              "justName": {
                "value": "{{ media.name }} is a dog"
              },
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);
}

#[actix_rt::test]
async fn double_remove_embedder() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    settings["embedders"]
        .as_object_mut()
        .unwrap()
        .insert(String::from("rest"), serde_json::Value::Null);

    let (response, code) = index.update_settings(settings.clone()).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": null
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    let (response, code) = index.update_settings(settings.clone()).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": null
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);
}

#[actix_rt::test]
async fn complex_fragment() {
    let (server, uid, mut settings) = init_fragments_index().await;
    let index = server.index(uid);

    settings["embedders"]["rest"]["indexingFragments"].as_object_mut().unwrap().insert(
        String::from("complex"),
        serde_json::json!({
            "value": {
                "breed": "{{ doc.breed }}",
                "breeds": [
                    "{{ doc.breed }}",
                    {
                        "breed": "{{ doc.breed }}",
                    }
                ]
            }
        }),
    );

    let (response, code) = index.update_settings(settings).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "indexingFragments": {
              "basic": {
                "value": "{{ doc.name }} is a dog"
              },
              "complex": {
                "value": {
                  "breed": "{{ doc.breed }}",
                  "breeds": [
                    "{{ doc.breed }}",
                    {
                      "breed": "{{ doc.breed }}"
                    }
                  ]
                }
              },
              "withBreed": {
                "value": "{{ doc.name }} is a {{ doc.breed }}"
              }
            },
            "searchFragments": {
              "justBreed": {
                "value": "It's a {{ media.breed }}"
              },
              "justName": {
                "value": "{{ media.name }} is a dog"
              },
              "query": {
                "value": "Some pre-prompt for query {{ q }}"
              }
            },
            "request": "{{fragment}}",
            "response": {
              "data": "{{embedding}}"
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
    "#);

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
                ],
                [
                  0.0,
                  0.0,
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
                ],
                [
                  0.0,
                  0.0,
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
async fn both_fragments_and_document_template() {
    let server = Server::new().await;
    let index = server.unique_index();

    let (_response, code) = server.set_features(json!({"multimodal": true})).await;
    snapshot!(code, @"200 OK");

    let settings = json!({
        "embedders": {
            "rest": {
                "source": "rest",
                "url": "http://localhost:1337",
                "dimensions": 3,
                "request": "{{fragment}}",
                "response": {
                    "data": "{{embedding}}"
                },
                "indexingFragments": {
                    "basic": {"value": "{{ doc.name }} is a dog"},
                },
                "searchFragments": {
                    "justBreed": {"value": "It's a {{ media.breed }}"},
                },
                "documentTemplate": "{{ doc.name }} is a dog",
            },
        },
    });

    let (response, code) = index.update_settings(settings.clone()).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r#"
    {
      "message": "Error while generating embeddings: user error: cannot pass both fragments and a document template.\n  - Note: 1 fragments declared in `indexingFragments` and 1 fragments declared in `search_fragments_len`.\n  - Hint: remove the declared fragments or remove the `documentTemplate`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "#);
}

#[ignore = "failing due to issue #5746"]
#[actix_rt::test]
async fn set_fragments_then_document_template() {
    let (server, uid, settings) = init_fragments_index().await;
    let index = server.index(uid);

    let url = settings["embedders"]["rest"]["url"].as_str().unwrap();

    let settings = json!({
        "embedders": {
            "rest": {
                "source": "rest",
                "url": url,
                "dimensions": 3,
                "request": "{{fragment}}",
                "response": {
                    "data": "{{embedding}}"
                },
                "documentTemplate": "{{ doc.name }} is a dog",
            },
        },
    });

    let (response, code) = index.update_settings(settings.clone()).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r"");

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(settings, @r#""#); // Should have removed fragments
}

#[actix_rt::test]
async fn composite() {
    let (server, uid, _settings) = init_fragments_index_composite().await;
    let index = server.index(uid);

    let (value, code) = index.search_post(
        json!({"vector": [1.0, 1.0, 1.0], "hybrid": {"semanticRatio": 1.0, "embedder": "rest"}, "limit": 1}
    )).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
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
      "requestUid": "[uuid]",
      "semanticHitCount": 1
    }
    "###);

    let (value, code) = index
        .search_post(
            json!({"q": "bulldog", "hybrid": {"semanticRatio": 1.0, "embedder": "rest"}, "limit": 1}
            ),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r#"
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
      "requestUid": "[uuid]",
      "semanticHitCount": 1
    }
    "#);
}
