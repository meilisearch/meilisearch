// This modules contains all the test concerning search. Each particular feature of the search
// should be tested in its own module to isolate tests and keep the tests readable.

mod distinct;
mod errors;
mod facet_search;
mod formatted;
mod geo;
mod hybrid;
mod multi;
mod pagination;
mod restrict_searchable;

use once_cell::sync::Lazy;

use crate::common::{Server, Value};
use crate::json;

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "id": "287947",
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "title": "Captain Marvel",
            "id": "299537",
            "_vectors": { "manual": [1, 2, 54] },
        },
        {
            "title": "Escape Room",
            "id": "522681",
            "_vectors": { "manual": [10, -23, 32] },
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "id": "166428",
            "_vectors": { "manual": [-100, 231, 32] },
        },
        {
            "title": "Gläss",
            "id": "450465",
            "_vectors": { "manual": [-100, 340, 90] },
        }
    ])
});

static NESTED_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "id": 852,
            "father": "jean",
            "mother": "michelle",
            "doggos": [
                {
                    "name": "bobby",
                    "age": 2,
                },
                {
                    "name": "buddy",
                    "age": 4,
                },
            ],
            "cattos": "pésti",
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "id": 654,
            "father": "pierre",
            "mother": "sabine",
            "doggos": [
                {
                    "name": "gros bill",
                    "age": 8,
                },
            ],
            "cattos": ["simba", "pestiféré"],
            "_vectors": { "manual": [1, 2, 54] },
        },
        {
            "id": 750,
            "father": "romain",
            "mother": "michelle",
            "cattos": ["enigma"],
            "_vectors": { "manual": [10, 23, 32] },
        },
        {
            "id": 951,
            "father": "jean-baptiste",
            "mother": "sophie",
            "doggos": [
                {
                    "name": "turbo",
                    "age": 5,
                },
                {
                    "name": "fast",
                    "age": 6,
                },
            ],
            "cattos": ["moumoute", "gomez"],
            "_vectors": { "manual": [10, 23, 32] },
        },
    ])
});

#[actix_rt::test]
async fn simple_placeholder_search() {
    let server = Server::new().await;
    let index = server.index("basic");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 5);
        })
        .await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 4);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"q": "glass"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(json!({"q": "pésti"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 2);
        })
        .await;
}

#[actix_rt::test]
async fn phrase_search_with_stop_word() {
    // related to https://github.com/meilisearch/meilisearch/issues/3521
    let server = Server::new().await;
    let index = server.index("test");

    let (_, code) = index.update_settings(json!({"stopWords": ["the", "of"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(json!({"q": "how \"to\" train \"the" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn test_kanji_language_detection() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
        { "id": 1, "title": "東京のお寿司。" },
        { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
    ]);
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"q": "東京"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn test_thai_language() {
    let server = Server::new().await;
    let index = server.index("test");

    // We don't need documents, the issue is on the query side only.
    let documents = json!([
        { "id": 0, "title": "สบู่สมุนไพรดอกดาวเรือง 100 กรัม จำนวน 6 ก้อน" },
        { "id": 1, "title": "สบู่สมุนไพรชาเขียว 100 กรัม จำนวน 6 ก้อน" },
        { "id": 2, "title": "สบู่สมุนไพรฝางแดงผสมว่านหางจรเข้ 100 กรัม จำนวน 6 ก้อน" }
    ]);
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index.update_settings(json!({"rankingRules": ["exactness"]})).await;
    index.wait_task(1).await;

    index
        .search(json!({"q": "สบู"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
        })
        .await;
}

#[actix_rt::test]
async fn search_multiple_params() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(
            json!({
                "q": "glass",
                "attributesToCrop": ["title:2"],
                "attributesToHighlight": ["title"],
                "limit": 1,
                "offset": 0,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "q": "pésti",
                "attributesToCrop": ["catto:2"],
                "attributesToHighlight": ["catto"],
                "limit": 2,
                "offset": 0,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_filter_string_notation() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_, code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (_, code) = index.add_documents(documents, None).await;
    meili_snap::snapshot!(code, @"202 Accepted");
    let res = index.wait_task(1).await;
    meili_snap::snapshot!(res["status"], @r###""succeeded""###);

    index
        .search(
            json!({
                "filter": "title = Gläss"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;

    let index = server.index("nested");

    let (_, code) =
        index.update_settings(json!({"filterableAttributes": ["cattos", "doggos.age"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = NESTED_DOCUMENTS.clone();
    let (_, code) = index.add_documents(documents, None).await;
    meili_snap::snapshot!(code, @"202 Accepted");
    let res = index.wait_task(3).await;
    meili_snap::snapshot!(res["status"], @r###""succeeded""###);

    index
        .search(
            json!({
                "filter": "cattos = pésti"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
                assert_eq!(response["hits"][0]["id"], json!(852));
            },
        )
        .await;

    index
        .search(
            json!({
                "filter": "doggos.age > 5"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
                assert_eq!(response["hits"][0]["id"], json!(654));
                assert_eq!(response["hits"][1]["id"], json!(951));
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_filter_array_notation() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) = index
        .search_post(json!({
            "filter": ["title = Gläss"]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 1);

    let (response, code) = index
        .search_post(json!({
            "filter": [["title = Gläss", "title = \"Shazam!\"", "title = \"Escape Room\""]]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 3);
}

#[actix_rt::test]
async fn search_with_sort_on_numbers() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"sortableAttributes": ["id"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "sort": ["id:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;

    let index = server.index("nested");

    index.update_settings(json!({"sortableAttributes": ["doggos.age"]})).await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(3).await;

    index
        .search(
            json!({
                "sort": ["doggos.age:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_sort_on_strings() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"sortableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "sort": ["title:desc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;

    let index = server.index("nested");

    index.update_settings(json!({"sortableAttributes": ["doggos.name"]})).await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(3).await;

    index
        .search(
            json!({
                "sort": ["doggos.name:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_multiple_sort() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"sortableAttributes": ["id", "title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) = index
        .search_post(json!({
            "sort": ["id:asc", "title:desc"]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 5);
}

#[actix_rt::test]
async fn search_facet_distribution() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "facets": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert!(dist.get("title").is_some());
            },
        )
        .await;

    let index = server.index("nested");

    index.update_settings(json!({"filterableAttributes": ["father", "doggos.name"]})).await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(3).await;

    // TODO: TAMO: fix the test
    index
        .search(
            json!({
                // "facets": ["father", "doggos.name"]
                "facets": ["father"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["father"],
                    json!({ "jean": 1, "pierre": 1, "romain": 1, "jean-baptiste": 1})
                );
                /*
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
                */
            },
        )
        .await;

    index.update_settings(json!({"filterableAttributes": ["doggos"]})).await;
    index.wait_task(4).await;

    index
        .search(
            json!({
                "facets": ["doggos.name"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
            },
        )
        .await;

    index
        .search(
            json!({
                "facets": ["doggos"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 3);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
                assert_eq!(dist["doggos.age"], json!({ "2": 1, "4": 1, "5": 1, "6": 1, "8": 1}));
            },
        )
        .await;
}

#[actix_rt::test]
async fn displayed_attributes() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({ "displayedAttributes": ["title"] })).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) =
        index.search_post(json!({ "attributesToRetrieve": ["title", "id"] })).await;
    assert_eq!(code, 200, "{}", response);
    assert!(response["hits"][0].get("title").is_some());
}

#[actix_rt::test]
async fn placeholder_search_is_hard_limited() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents: Vec<_> = (0..1200).map(|i| json!({ "id": i, "text": "I am unique!" })).collect();
    index.add_documents(documents.into(), None).await;
    index.wait_task(0).await;

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1000);
            },
        )
        .await;

    index
        .search(
            json!({
                "offset": 800,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    index.update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } })).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1200);
            },
        )
        .await;

    index
        .search(
            json!({
                "offset": 1000,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_is_hard_limited() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents: Vec<_> = (0..1200).map(|i| json!({ "id": i, "text": "I am unique!" })).collect();
    index.add_documents(documents.into(), None).await;
    index.wait_task(0).await;

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1000);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": "unique",
                "offset": 800,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    index.update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } })).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1200);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": "unique",
                "offset": 1000,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn faceting_max_values_per_facet() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({ "filterableAttributes": ["number"] })).await;

    let documents: Vec<_> = (0..10_000).map(|id| json!({ "id": id, "number": id * 10 })).collect();
    index.add_documents(json!(documents), None).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let numbers = response["facetDistribution"]["number"].as_object().unwrap();
                assert_eq!(numbers.len(), 100);
            },
        )
        .await;

    index.update_settings(json!({ "faceting": { "maxValuesPerFacet": 10_000 } })).await;
    index.wait_task(2).await;

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let numbers = &response["facetDistribution"]["number"].as_object().unwrap();
                assert_eq!(numbers.len(), 10_000);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_score_details() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();

    let res = index.add_documents(json!(documents), None).await;
    index.wait_task(res.0.uid()).await;

    index
        .search(
            json!({
                "q": "train dragon",
                "showRankingScoreDetails": true,
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "How to Train Your Dragon: The Hidden World",
                    "id": "166428",
                    "_vectors": {
                      "manual": [
                        -100,
                        231,
                        32
                      ]
                    },
                    "_rankingScoreDetails": {
                      "words": {
                        "order": 0,
                        "matchingWords": 2,
                        "maxMatchingWords": 2,
                        "score": 1.0
                      },
                      "typo": {
                        "order": 1,
                        "typoCount": 0,
                        "maxTypoCount": 2,
                        "score": 1.0
                      },
                      "proximity": {
                        "order": 2,
                        "score": 0.75
                      },
                      "attribute": {
                        "order": 3,
                        "attributeRankingOrderScore": 1.0,
                        "queryWordDistanceScore": 0.8095238095238095,
                        "score": 0.9727891156462584
                      },
                      "exactness": {
                        "order": 4,
                        "matchType": "noExactMatch",
                        "matchingWords": 2,
                        "maxMatchingWords": 2,
                        "score": 0.3333333333333333
                      }
                    }
                  }
                ]
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_degraded_score_details() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = NESTED_DOCUMENTS.clone();

    index.add_documents(json!(documents), None).await;
    // We can't really use anything else than 0ms here; otherwise, the test will get flaky.
    let (res, _code) = index.update_settings(json!({ "searchCutoff": 0 })).await;
    index.wait_task(res.uid()).await;

    index
        .search(
            json!({
                "q": "b",
                "attributesToRetrieve": ["doggos.name", "cattos"],
                "showRankingScoreDetails": true,
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
                {
                  "hits": [
                    {
                      "doggos": [
                        {
                          "name": "bobby"
                        },
                        {
                          "name": "buddy"
                        }
                      ],
                      "cattos": "pésti",
                      "_rankingScoreDetails": {
                        "skipped": 0.0
                      }
                    },
                    {
                      "doggos": [
                        {
                          "name": "gros bill"
                        }
                      ],
                      "cattos": [
                        "simba",
                        "pestiféré"
                      ],
                      "_rankingScoreDetails": {
                        "skipped": 0.0
                      }
                    },
                    {
                      "doggos": [
                        {
                          "name": "turbo"
                        },
                        {
                          "name": "fast"
                        }
                      ],
                      "cattos": [
                        "moumoute",
                        "gomez"
                      ],
                      "_rankingScoreDetails": {
                        "skipped": 0.0
                      }
                    }
                  ],
                  "query": "b",
                  "processingTimeMs": 0,
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 3
                }
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn experimental_feature_vector_store() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();

    index.add_documents(json!(documents), None).await;
    index.wait_task(0).await;

    let (response, code) = index
        .search_post(json!({
            "vector": [1.0, 2.0, 3.0],
        }))
        .await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Passing `vector` as a query parameter requires enabling the `vector store` experimental feature. See https://github.com/meilisearch/product/discussions/677",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    let (response, code) = server.set_features(json!({"vectorStore": true})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response["vectorStore"], @"true");

    let (response, code) = index
        .update_settings(json!({"embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        }}))
        .await;

    meili_snap::snapshot!(response, @r###"
    {
      "taskUid": 1,
      "indexUid": "test",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    meili_snap::snapshot!(code, @"202 Accepted");
    let response = index.wait_task(response.uid()).await;

    meili_snap::snapshot!(meili_snap::json_string!(response["status"]), @"\"succeeded\"");

    let (response, code) = index
        .search_post(json!({
            "vector": [1.0, 2.0, 3.0],
        }))
        .await;

    meili_snap::snapshot!(code, @"200 OK");
    // vector search returns all documents that don't have vectors in the last bucket, like all sorts
    meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
    [
      {
        "title": "Shazam!",
        "id": "287947",
        "_vectors": {
          "manual": [
            1,
            2,
            3
          ]
        },
        "_semanticScore": 1.0
      },
      {
        "title": "Captain Marvel",
        "id": "299537",
        "_vectors": {
          "manual": [
            1,
            2,
            54
          ]
        },
        "_semanticScore": 0.9129112
      },
      {
        "title": "Gläss",
        "id": "450465",
        "_vectors": {
          "manual": [
            -100,
            340,
            90
          ]
        },
        "_semanticScore": 0.8106413
      },
      {
        "title": "How to Train Your Dragon: The Hidden World",
        "id": "166428",
        "_vectors": {
          "manual": [
            -100,
            231,
            32
          ]
        },
        "_semanticScore": 0.74120104
      },
      {
        "title": "Escape Room",
        "id": "522681",
        "_vectors": {
          "manual": [
            10,
            -23,
            32
          ]
        }
      }
    ]
    "###);
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn camelcased_words() {
    let server = Server::new().await;
    let index = server.index("test");

    // related to https://github.com/meilisearch/meilisearch/issues/3818
    let documents = json!([
        { "id": 0, "title": "DeLonghi" },
        { "id": 1, "title": "delonghi" },
        { "id": 2, "title": "TestAB" },
        { "id": 3, "title": "TestAb" },
        { "id": 4, "title": "testab" },
    ]);
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"q": "deLonghi"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 0,
                "title": "DeLonghi"
              },
              {
                "id": 1,
                "title": "delonghi"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "dellonghi"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 0,
                "title": "DeLonghi"
              },
              {
                "id": 1,
                "title": "delonghi"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "testa"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "testab"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TestaB"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "Testab"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TestAb"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    // with Typos
    index
        .search(json!({"q": "dellonghi"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 0,
                "title": "DeLonghi"
              },
              {
                "id": 1,
                "title": "delonghi"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TetsAB"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TetsAB"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search_with_strange_synonyms() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({ "synonyms": {"&": ["to"], "to": ["&"]} })).await;
    let r = index.wait_task(0).await;
    meili_snap::snapshot!(r["status"], @r###""succeeded""###);

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(json!({"q": "How to train"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "_vectors": {
                  "manual": [
                    -100,
                    231,
                    32
                  ]
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "How & train"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "_vectors": {
                  "manual": [
                    -100,
                    231,
                    32
                  ]
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "to"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "_vectors": {
                  "manual": [
                    -100,
                    231,
                    32
                  ]
                }
              }
            ]
            "###);
        })
        .await;
}
