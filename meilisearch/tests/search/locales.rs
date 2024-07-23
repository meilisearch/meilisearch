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
    let server = Server::new().await;

    let index = server.index("test");
    let documents = DOCUMENTS.clone();
    index
        .update_settings(
            json!({"searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"]}),
        )
        .await;
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

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
            json!({"q": "進撃", "attributesToRetrieve": ["id"], "locales": ["jpn"]}),
            |response, code| {
                snapshot!(response, @r###"
            {
              "hits": [
                {
                  "id": 852
                },
                {
                  "id": 853
                }
              ],
              "query": "進撃",
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

    // chinese
    index
        .search(json!({"q": "进击", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(response, @r###"
            {
              "hits": [
                {
                  "id": 853
                },
                {
                  "id": 852
                }
              ],
              "query": "进击",
              "processingTimeMs": "[duration]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 2
            }
            "###);
            snapshot!(code, @"200 OK");
        })
        .await;
}

#[actix_rt::test]
async fn force_locales() {
    let server = Server::new().await;

    let index = server.index("test");
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
    snapshot!(response, @r###"
    {
      "taskUid": 0,
      "indexUid": "test",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    // chinese detection
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

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"], "locales": ["jpn"]}),
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
    let server = Server::new().await;

    let index = server.index("test");
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
    snapshot!(response, @r###"
    {
      "taskUid": 0,
      "indexUid": "test",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    // chinese detection
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

    // force japanese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"], "locales": ["jpn"]}),
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
    let server = Server::new().await;

    let index = server.index("test");
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
    snapshot!(response, @r###"
    {
      "taskUid": 0,
      "indexUid": "test",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    // chinese
    index
        .search(
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"], "locales": ["cmn"]}),
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
            json!({"q": "\"进击的巨人\"", "attributesToRetrieve": ["id"], "locales": ["jpn"]}),
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
async fn invalid_locales() {
    let server = Server::new().await;

    let index = server.index("test");
    let documents = DOCUMENTS.clone();
    index
        .update_settings(
            json!({"searchableAttributes": ["name_en", "name_ja", "name_zh", "author_en", "author_ja", "author_zh", "description_en", "description_ja", "description_zh"]}),
        )
        .await;
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) = index
        .search_post(json!({"q": "Atta", "attributesToRetrieve": ["id"], "locales": ["invalid"]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown value `invalid` at `.locales[0]`: expected one of `epo`, `eng`, `rus`, `cmn`, `spa`, `por`, `ita`, `ben`, `fra`, `deu`, `ukr`, `kat`, `ara`, `hin`, `jpn`, `heb`, `yid`, `pol`, `amh`, `jav`, `kor`, `nob`, `dan`, `swe`, `fin`, `tur`, `nld`, `hun`, `ces`, `ell`, `bul`, `bel`, `mar`, `kan`, `ron`, `slv`, `hrv`, `srp`, `mkd`, `lit`, `lav`, `est`, `tam`, `vie`, `urd`, `tha`, `guj`, `uzb`, `pan`, `aze`, `ind`, `tel`, `pes`, `mal`, `ori`, `mya`, `nep`, `sin`, `khm`, `tuk`, `aka`, `zul`, `sna`, `afr`, `lat`, `slk`, `cat`, `tgl`, `hye`",
      "code": "invalid_search_locales",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_locales"
    }
    "###);

    let (response, code) = index
        .search_get(
            &yaup::to_string(
                &json!({"q": "Atta", "attributesToRetrieve": ["id"], "locales": ["invalid"]}),
            )
            .unwrap(),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `locales`: Unknown value `invalid`, expected one of `epo`, `eng`, `rus`, `cmn`, `spa`, `por`, `ita`, `ben`, `fra`, `deu`, `ukr`, `kat`, `ara`, `hin`, `jpn`, `heb`, `yid`, `pol`, `amh`, `jav`, `kor`, `nob`, `dan`, `swe`, `fin`, `tur`, `nld`, `hun`, `ces`, `ell`, `bul`, `bel`, `mar`, `kan`, `ron`, `slv`, `hrv`, `srp`, `mkd`, `lit`, `lav`, `est`, `tam`, `vie`, `urd`, `tha`, `guj`, `uzb`, `pan`, `aze`, `ind`, `tel`, `pes`, `mal`, `ori`, `mya`, `nep`, `sin`, `khm`, `tuk`, `aka`, `zul`, `sna`, `afr`, `lat`, `slk`, `cat`, `tgl`, `hye`",
      "code": "invalid_search_locales",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_locales"
    }
    "###);
}

#[actix_rt::test]
async fn invalid_localized_attributes_rules() {
    let server = Server::new().await;

    let index = server.index("test");
    let (response, _) = index
        .update_settings(json!({
            "localizedAttributes": [
                {"attributePatterns": ["*_ja", "*_zh"], "locales": ["japan"]}
            ]
        }))
        .await;
    snapshot!(response, @r###"
    {
      "message": "Unknown value `japan` at `.localizedAttributes[0].locales[0]`: expected one of `epo`, `eng`, `rus`, `cmn`, `spa`, `por`, `ita`, `ben`, `fra`, `deu`, `ukr`, `kat`, `ara`, `hin`, `jpn`, `heb`, `yid`, `pol`, `amh`, `jav`, `kor`, `nob`, `dan`, `swe`, `fin`, `tur`, `nld`, `hun`, `ces`, `ell`, `bul`, `bel`, `mar`, `kan`, `ron`, `slv`, `hrv`, `srp`, `mkd`, `lit`, `lav`, `est`, `tam`, `vie`, `urd`, `tha`, `guj`, `uzb`, `pan`, `aze`, `ind`, `tel`, `pes`, `mal`, `ori`, `mya`, `nep`, `sin`, `khm`, `tuk`, `aka`, `zul`, `sna`, `afr`, `lat`, `slk`, `cat`, `tgl`, `hye`",
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
    let server = Server::new().await;

    let index = server.index("test");
    let documents = DOCUMENTS.clone();
    let (response, _) = index
        .update_settings(json!({
            "filterableAttributes": ["name_en", "name_ja", "name_zh"],
        }))
        .await;
    snapshot!(response, @r###"
    {
      "taskUid": 0,
      "indexUid": "test",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

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
    let server = Server::new().await;

    let index = server.index("test");
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
    snapshot!(response, @r###"
    {
      "taskUid": 0,
      "indexUid": "test",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

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
