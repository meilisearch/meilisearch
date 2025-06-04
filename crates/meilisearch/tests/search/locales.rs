use meili_snap::*;
use once_cell::sync::Lazy;

use crate::common::{Server, Value};
use crate::json;

static NESTED_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "id": 852,
            "document_en": {
                    "name": "Attack on Titan",
                    "description": "Attack on Titan is a Japanese manga series written and illustrated by Hajime Isayama",
                    "author": "Hajime Isayama",
                },
            "document_ja": {
                "name": "進撃の巨人",
                "description": "進撃の巨人は、日本の漫画シリーズであり、諫山 創によって作画されている。",
                "author": "諫山 創",
            },
            "document_zh": {
                "name": "进击的巨人",
                "description": "进击的巨人是日本的漫画系列，由諫山 創作画。",
                "author": "諫山創",
            },
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "id": 654,
            "document_en":
                {
                    "name": "One Piece",
                    "description": "One Piece is a Japanese manga series written and illustrated by Eiichiro Oda",
                    "author": "Eiichiro Oda",
                },
            "document_ja": {
                "name": "ワンピース",
                "description": "ワンピースは、日本の漫画シリーズであり、尾田 栄一郎によって作画されている。",
                "author": "尾田 栄一郎",
            },
            "document_zh": {
                "name": "ONE PIECE",
                "description": "海贼王》是尾田荣一郎创作的日本漫画系列。",
                "author": "尾田 栄一郎",
            },
            "_vectors": { "manual": [1, 2, 54] },
        }
    ])
});

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "id": 852,
            "name_en": "Attack on Titan",
            "description_en": "Attack on Titan is a Japanese manga series written and illustrated by Hajime Isayama",
            "author_en": "Hajime Isayama",
            "name_ja": "進撃の巨人",
            "description_ja": "進撃の巨人は、日本の漫画シリーズであり、諫山 創によって作画されている。",
            "author_ja": "諫山 創",
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "id": 853,
            "name_zh": "进击的巨人",
            "description_zh": "进击的巨人是日本的漫画系列，由諫山 創作画。",
            "author_zh": "諫山創",
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "id": 654,
            "name_en": "One Piece",
            "description_en": "One Piece is a Japanese manga series written and illustrated by Eiichiro Oda",
            "author_en": "Eiichiro Oda",
            "name_ja": "ワンピース",
            "description_ja": "ワンピースは、日本の漫画シリーズであり、尾田 栄一郎によって作画されている。",
            "author_ja": "尾田 栄一郎",
            "_vectors": { "manual": [1, 2, 54] },
        },
        {
            "id": 655,
            "name_zh": "ONE PIECE",
            "description_zh": "海贼王》是尾田荣一郎创作的日本漫画系列。",
            "author_zh": "尾田 栄一郎",
            "_vectors": { "manual": [1, 2, 54] },
        }
    ])
});

#[actix_rt::test]
async fn simple_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index
        .update_settings(
            json!({"searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"]}),
        )
        .await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    // english
    index
        .search(json!({"q": "Atta", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(response, @r###"
            {
              "hits": [
                {
                  "id": 852
                }
              ],
              "query": "Atta",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 1
            }
            "###);
            snapshot!(code, @"200 OK");
        })
        .await;

    // japanese
    index
        .search(json!({"q": "進撃", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(response, @r###"
            {
              "hits": [
                {
                  "id": 853
                }
              ],
              "query": "進撃",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 1
            }
            "###);
            snapshot!(code, @"200 OK");
        })
        .await;

    index
        .search(
            json!({"q": "進撃", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r#"
                {
                  "hits": [
                    {
                      "id": 852
                    }
                  ],
                  "query": "進撃",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "#);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // chinese
    index
        .search(json!({"q": "进击", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(response, @r#"
            {
              "hits": [
                {
                  "id": 853
                }
              ],
              "query": "进击",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 1
            }
            "#);
            snapshot!(code, @"200 OK");
        })
        .await;
}

#[actix_rt::test]
async fn force_locales() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(
            json!({
                "searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"],
                "localizedAttributes": [
                    // force japanese
                    {"attributePatterns": ["name_ja", "name_zh", "author_ja", "author_zh", "description_ja", "description_zh"], "locales": ["jpn"]}
                ]
            }),
        )
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    // chinese detection
    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 853
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 853
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}

#[actix_rt::test]
async fn force_locales_with_pattern() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(
            json!({
                "searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"],
                "localizedAttributes": [
                    // force japanese
                    {"attributePatterns": ["*_ja", "*_zh"], "locales": ["jpn"]}
                ]
            }),
        )
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    // chinese detection
    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 853
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 853
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}

#[actix_rt::test]
async fn force_locales_with_pattern_nested() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = NESTED_DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(json!({
            "searchableAttributes": ["document_en", "document_ja", "document_zh"],
            "localizedAttributes": [
                // force japanese
                {"attributePatterns": ["document_ja.*", "*_zh.*"], "locales": ["jpn"]}
            ]
        }))
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    // chinese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["cmn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 852
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}
#[actix_rt::test]
async fn force_different_locales_with_pattern() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(
            json!({
                "searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"],
                "localizedAttributes": [
                    // force japanese
                    {"attributePatterns": ["*_zh"], "locales": ["jpn"]},
                    // force chinese
                    {"attributePatterns": ["*_ja"], "locales": ["cmn"]}
                ]
            }),
        )
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    // force chinese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["cmn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 853
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}

#[actix_rt::test]
async fn auto_infer_locales_at_search_with_attributes_to_search_on() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(
            json!({
                "searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"],
                "localizedAttributes": [
                    // force japanese
                    {"attributePatterns": ["*_zh"], "locales": ["jpn"]},
                    // force chinese
                    {"attributePatterns": ["*_ja"], "locales": ["cmn"]},
                    // any language
                    {"attributePatterns": ["*_en"], "locales": []}
                ]
            }),
        )
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    // auto infer any language
    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // should infer chinese
    index
            .search(
                json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"], "attributesToSearchOn": ["name_zh", "description_zh"]}),
                |response, code| {
                    snapshot!(response, @r###"
                    {
                      "hits": [
                        {
                          "id": 853
                        }
                      ],
                      "query": "\"进击的巨人\"",
                      "processingTimeMs": "[duration]",
                      "limit": 20,
                      "offset": 0,
                      "estimatedTotalHits": 1
                    }
                    "###);
                    snapshot!(code, @"200 OK");
                },
            )
            .await;
}

#[actix_rt::test]
async fn auto_infer_locales_at_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(
            json!({
                "searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"],
                "localizedAttributes": [
                    // force japanese
                    {"attributePatterns": ["*"], "locales": ["jpn"]},
                ]
            }),
        )
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 853
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                    {
                      "hits": [
                        {
                          "id": 853
                        }
                      ],
                      "query": "\"进击的巨人\"",
                      "processingTimeMs": "[duration]",
                      "limit": 20,
                      "offset": 0,
                      "estimatedTotalHits": 1
                    }
                    "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 853
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}

#[actix_rt::test]
async fn force_different_locales_with_pattern_nested() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = NESTED_DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(json!({
            "searchableAttributes": ["document_en", "document_ja", "document_zh"],
            "localizedAttributes": [
              // force japanese
              {"attributePatterns": ["*_zh.*"], "locales": ["jpn"]},
              // force chinese
              {"attributePatterns": ["document_ja.*", "document_zh.*"], "locales": ["cmn"]}
            ]
        }))
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    // chinese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["cmn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                    {
                      "hits": [
                        {
                          "id": 852
                        }
                      ],
                      "query": "\"进击的巨人\"",
                      "processingTimeMs": "[duration]",
                      "limit": 20,
                      "offset": 0,
                      "estimatedTotalHits": 1
                    }
                    "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["ja"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "id": 852
                    }
                  ],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 1
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}

#[actix_rt::test]
async fn settings_change() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, _) = index
        .update_settings(json!({
            "searchableAttributes": ["document_en", "document_ja", "document_zh"],
            "localizedAttributes": [
                // force japanese
                {"attributePatterns": ["document_ja.*", "*_zh.*"], "locales": ["jpn"]}
            ]
        }))
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    index.wait_task(response.uid()).await.succeeded();

    // chinese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["cmn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // change settings
    let (response, _) = index
        .update_settings(json!({
            "searchableAttributes": ["document_en", "document_ja", "document_zh"],
            "localizedAttributes": [
              // force japanese
              {"attributePatterns": ["*_zh.*"], "locales": ["jpn"]},
              // force chinese
              {"attributePatterns": ["document_ja.*"], "locales": ["cmn"]}
            ]
        }))
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    index.wait_task(response.uid()).await.succeeded();

    // chinese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["cmn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "locales": ["jpn"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [],
                  "query": "\"进击的巨人\"",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 0
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}

#[actix_rt::test]
async fn invalid_locales() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index
        .update_settings(
            json!({"searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"]}),
        )
        .await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.search_post(json!({"q": "Atta", "locales": ["invalid"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown value `invalid` at `.locales[0]`: expected one of `af`, `ak`, `am`, `ar`, `az`, `be`, `bn`, `bg`, `ca`, `cs`, `da`, `de`, `el`, `en`, `eo`, `et`, `fi`, `fr`, `gu`, `he`, `hi`, `hr`, `hu`, `hy`, `id`, `it`, `jv`, `ja`, `kn`, `ka`, `km`, `ko`, `la`, `lv`, `lt`, `ml`, `mr`, `mk`, `my`, `ne`, `nl`, `nb`, `or`, `pa`, `fa`, `pl`, `pt`, `ro`, `ru`, `si`, `sk`, `sl`, `sn`, `es`, `sr`, `sv`, `ta`, `te`, `tl`, `th`, `tk`, `tr`, `uk`, `ur`, `uz`, `vi`, `yi`, `zh`, `zu`, `afr`, `aka`, `amh`, `ara`, `aze`, `bel`, `ben`, `bul`, `cat`, `ces`, `dan`, `deu`, `ell`, `eng`, `epo`, `est`, `fin`, `fra`, `guj`, `heb`, `hin`, `hrv`, `hun`, `hye`, `ind`, `ita`, `jav`, `jpn`, `kan`, `kat`, `khm`, `kor`, `lat`, `lav`, `lit`, `mal`, `mar`, `mkd`, `mya`, `nep`, `nld`, `nob`, `ori`, `pan`, `pes`, `pol`, `por`, `ron`, `rus`, `sin`, `slk`, `slv`, `sna`, `spa`, `srp`, `swe`, `tam`, `tel`, `tgl`, `tha`, `tuk`, `tur`, `ukr`, `urd`, `uzb`, `vie`, `yid`, `zho`, `zul`, `cmn`",
      "code": "invalid_search_locales",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_locales"
    }
    "###);

    let (response, code) = index
        .search_get(&yaup::to_string(&json!({"q": "Atta", "locales": ["invalid"]})).unwrap())
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `locales`: Unsupported locale `invalid`, expected one of af, ak, am, ar, az, be, bg, bn, ca, cs, da, de, el, en, eo, es, et, fa, fi, fr, gu, he, hi, hr, hu, hy, id, it, ja, jv, ka, km, kn, ko, la, lt, lv, mk, ml, mr, my, nb, ne, nl, or, pa, pl, pt, ro, ru, si, sk, sl, sn, sr, sv, ta, te, th, tk, tl, tr, uk, ur, uz, vi, yi, zh, zu, afr, aka, amh, ara, aze, bel, ben, bul, cat, ces, cmn, dan, deu, ell, eng, epo, est, fin, fra, guj, heb, hin, hrv, hun, hye, ind, ita, jav, jpn, kan, kat, khm, kor, lat, lav, lit, mal, mar, mkd, mya, nep, nld, nob, ori, pan, pes, pol, por, ron, rus, sin, slk, slv, sna, spa, srp, swe, tam, tel, tgl, tha, tuk, tur, ukr, urd, uzb, vie, yid, zho, zul",
      "code": "invalid_search_locales",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_locales"
    }
    "###);
}

#[actix_rt::test]
async fn invalid_localized_attributes_rules() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, _) = index
        .update_settings(json!({
            "localizedAttributes": [
                {"attributePatterns": ["*_ja", "*_zh"], "locales": ["japan"]}
            ]
        }))
        .await;
    snapshot!(response, @r###"
    {
      "message": "Unknown value `japan` at `.localizedAttributes[0].locales[0]`: expected one of `af`, `ak`, `am`, `ar`, `az`, `be`, `bn`, `bg`, `ca`, `cs`, `da`, `de`, `el`, `en`, `eo`, `et`, `fi`, `fr`, `gu`, `he`, `hi`, `hr`, `hu`, `hy`, `id`, `it`, `jv`, `ja`, `kn`, `ka`, `km`, `ko`, `la`, `lv`, `lt`, `ml`, `mr`, `mk`, `my`, `ne`, `nl`, `nb`, `or`, `pa`, `fa`, `pl`, `pt`, `ro`, `ru`, `si`, `sk`, `sl`, `sn`, `es`, `sr`, `sv`, `ta`, `te`, `tl`, `th`, `tk`, `tr`, `uk`, `ur`, `uz`, `vi`, `yi`, `zh`, `zu`, `afr`, `aka`, `amh`, `ara`, `aze`, `bel`, `ben`, `bul`, `cat`, `ces`, `dan`, `deu`, `ell`, `eng`, `epo`, `est`, `fin`, `fra`, `guj`, `heb`, `hin`, `hrv`, `hun`, `hye`, `ind`, `ita`, `jav`, `jpn`, `kan`, `kat`, `khm`, `kor`, `lat`, `lav`, `lit`, `mal`, `mar`, `mkd`, `mya`, `nep`, `nld`, `nob`, `ori`, `pan`, `pes`, `pol`, `por`, `ron`, `rus`, `sin`, `slk`, `slv`, `sna`, `spa`, `srp`, `swe`, `tam`, `tel`, `tgl`, `tha`, `tuk`, `tur`, `ukr`, `urd`, `uzb`, `vie`, `yid`, `zho`, `zul`, `cmn`",
      "code": "invalid_settings_localized_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_localized_attributes"
    }
    "###);

    let (response, _) = index
        .update_settings(json!({
            "localizedAttributes": [
                {"attributePatterns": ["*_ja", "*_zh"], "locales": "jpn"}
            ]
        }))
        .await;
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.localizedAttributes[0].locales`: expected an array, but found a string: `\"jpn\"`",
      "code": "invalid_settings_localized_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_localized_attributes"
    }
    "###);

    let (response, _) = index
        .update_settings(json!({
            "localizedAttributes": [
                {"attributePatterns": "*_ja", "locales": ["jpn"]}
            ]
        }))
        .await;
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.localizedAttributes[0].attributePatterns`: expected an array, but found a string: `\"*_ja\"`",
      "code": "invalid_settings_localized_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_localized_attributes"
    }
    "###);

    let (response, _) = index
        .update_settings(json!({
            "localizedAttributes": [
                {"locales": ["jpn"]}
            ]
        }))
        .await;
    snapshot!(response, @r###"
    {
      "message": "Missing field `attributePatterns` inside `.localizedAttributes[0]`",
      "code": "invalid_settings_localized_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_localized_attributes"
    }
    "###);
}

#[actix_rt::test]
async fn simple_facet_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(json!({
            "filterableAttributes": ["name_en", "name_ja", "name_zh"],
        }))
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, _) = index
        .facet_search(json!({"facetName": "name_zh", "facetQuery": "進撃", "locales": ["cmn"]}))
        .await;

    snapshot!(response, @r###"
    {
      "facetHits": [
        {
          "value": "进击的巨人",
          "count": 1
        }
      ],
      "facetQuery": "進撃",
      "processingTimeMs": "[duration]"
    }
    "###);

    let (response, _) = index
        .facet_search(json!({"facetName": "name_zh", "facetQuery": "進撃", "locales": ["jpn"]}))
        .await;

    snapshot!(response, @r###"
    {
      "facetHits": [
        {
          "value": "进击的巨人",
          "count": 1
        }
      ],
      "facetQuery": "進撃",
      "processingTimeMs": "[duration]"
    }
    "###);
}

#[actix_rt::test]
async fn facet_search_with_localized_attributes() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(json!({
            "filterableAttributes": ["name_ja", "name_zh"],
            "localizedAttributes": [
                // force japanese
                {"attributePatterns": ["*_ja", "*_zh"], "locales": ["jpn"]}
            ]
        }))
        .await;
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, _) = index
        .facet_search(json!({"facetName": "name_zh", "facetQuery": "进击", "locales": ["cmn"]}))
        .await;

    snapshot!(response, @r###"
    {
      "facetHits": [],
      "facetQuery": "进击",
      "processingTimeMs": "[duration]"
    }
    "###);

    let (response, _) = index
        .facet_search(json!({"facetName": "name_zh", "facetQuery": "进击", "locales": ["jpn"]}))
        .await;

    snapshot!(response, @r###"
    {
      "facetHits": [
        {
          "value": "进击的巨人",
          "count": 1
        }
      ],
      "facetQuery": "进击",
      "processingTimeMs": "[duration]"
    }
    "###);

    let (response, _) =
        index.facet_search(json!({"facetName": "name_zh", "facetQuery": "进击"})).await;

    snapshot!(response, @r###"
    {
      "facetHits": [
        {
          "value": "进击的巨人",
          "count": 1
        }
      ],
      "facetQuery": "进击",
      "processingTimeMs": "[duration]"
    }
    "###);
}

#[actix_rt::test]
async fn swedish_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
      {"id": "tra1-1", "product": "trä"},
      {"id": "tra2-1", "product": "traktor"},
      {"id": "tra1-2", "product": "träbjälke"},
      {"id": "tra2-2", "product": "trafiksignal"},
    ]);
    index.add_documents(documents, None).await;
    let (_response, _) = index
        .update_settings(json!({
            "searchableAttributes": ["product"],
            "localizedAttributes": [
                // force swedish
                {"attributePatterns": ["product"], "locales": ["swe"]}
            ]
        }))
        .await;
    index.wait_task(_response.uid()).await.succeeded();

    // infer swedish
    index
        .search(json!({"q": "trä", "attributesToRetrieve": ["product"]}), |response, code| {
            snapshot!(response, @r###"
            {
              "hits": [
                {
                  "product": "trä"
                },
                {
                  "product": "träbjälke"
                }
              ],
              "query": "trä",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 2
            }
            "###);
            snapshot!(code, @"200 OK");
        })
        .await;

    index
        .search(json!({"q": "tra", "attributesToRetrieve": ["product"]}), |response, code| {
            snapshot!(response, @r###"
            {
              "hits": [
                {
                  "product": "traktor"
                },
                {
                  "product": "trafiksignal"
                }
              ],
              "query": "tra",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 2
            }
            "###);
            snapshot!(code, @"200 OK");
        })
        .await;

    // force swedish
    index
        .search(
            json!({"q": "trä", "locales": ["swe"], "attributesToRetrieve": ["product"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "product": "trä"
                    },
                    {
                      "product": "träbjälke"
                    }
                  ],
                  "query": "trä",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 2
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
    index
        .search(
            json!({"q": "tra", "locales": ["swe"], "attributesToRetrieve": ["product"]}),
            |response, code| {
                snapshot!(response, @r###"
                {
                  "hits": [
                    {
                      "product": "traktor"
                    },
                    {
                      "product": "trafiksignal"
                    }
                  ],
                  "query": "tra",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 2
                }
                "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}

#[actix_rt::test]
async fn german_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
      {"id": 1, "product": "Interkulturalität"},
      {"id": 2, "product": "Wissensorganisation"},
    ]);
    index.add_documents(documents, None).await;
    let (_response, _) = index
        .update_settings(json!({
            "searchableAttributes": ["product"],
            "localizedAttributes": [
                // force swedish
                {"attributePatterns": ["product"], "locales": ["deu"]}
            ]
        }))
        .await;
    index.wait_task(_response.uid()).await.succeeded();

    // infer swedish
    index
        .search(
            json!({"q": "kulturalität", "attributesToRetrieve": ["product"]}),
            |response, code| {
                snapshot!(response, @r###"
            {
              "hits": [
                {
                  "product": "Interkulturalität"
                }
              ],
              "query": "kulturalität",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 1
            }
            "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;

    index
        .search(
            json!({"q": "organisation", "attributesToRetrieve": ["product"]}),
            |response, code| {
                snapshot!(response, @r###"
            {
              "hits": [
                {
                  "product": "Wissensorganisation"
                }
              ],
              "query": "organisation",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 1
            }
            "###);
                snapshot!(code, @"200 OK");
            },
        )
        .await;
}
