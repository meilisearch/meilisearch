// This module contains all the test concerning search. Each particular feature of the search
// should be tested in its own module to isolate tests and keep the tests readable.

mod distinct;
mod errors;
mod facet_search;
mod filters;
mod formatted;
mod geo;
mod hybrid;
#[cfg(not(feature = "chinese-pinyin"))]
mod locales;
mod matching_strategy;
mod multi;
mod pagination;
mod restrict_searchable;
mod search_queue;

use meili_snap::{json_string, snapshot};

use crate::common::{
    shared_index_with_documents, shared_index_with_nested_documents,
    shared_index_with_score_documents, Server, Value, DOCUMENTS, FRUITS_DOCUMENTS,
    NESTED_DOCUMENTS, SCORE_DOCUMENTS, VECTOR_DOCUMENTS,
};
use crate::json;

async fn test_settings_documents_indexing_swapping_and_search(
    documents: &Value,
    settings: &Value,
    query: &Value,
    test: impl Fn(Value, actix_http::StatusCode) + std::panic::UnwindSafe + Clone,
) {
    let server = Server::new_shared();

    eprintln!("Documents -> Settings -> test");
    let index = server.unique_index();

    let (task, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index.update_settings(settings.clone()).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    index.search(query.clone(), test.clone()).await;

    eprintln!("Settings -> Documents -> test");
    let index = server.unique_index();

    let (task, code) = index.update_settings(settings.clone()).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    index.search(query.clone(), test.clone()).await;
}

#[actix_rt::test]
async fn simple_placeholder_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 5);
        })
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 4);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "glass"}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(json!({"q": "pésti"}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 2);
        })
        .await;
}

/// See <https://github.com/meilisearch/meilisearch/issues/5547>
#[actix_rt::test]
async fn bug_5547() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _code) = index.create(None).await;
    server.wait_task(response.uid()).await.succeeded();

    let mut documents = Vec::new();
    for i in 0..65_535 {
        documents.push(json!({"id": i, "title": format!("title{i}")}));
    }

    let (response, _code) = index.add_documents(json!(documents), Some("id")).await;
    server.wait_task(response.uid()).await.succeeded();
    let (response, code) = index.search_post(json!({"q": "title"})).await;
    assert_eq!(code, 200);
    snapshot!(response["hits"], @r###"[{"id":0,"title":"title0"},{"id":1,"title":"title1"},{"id":10,"title":"title10"},{"id":100,"title":"title100"},{"id":101,"title":"title101"},{"id":102,"title":"title102"},{"id":103,"title":"title103"},{"id":104,"title":"title104"},{"id":105,"title":"title105"},{"id":106,"title":"title106"},{"id":107,"title":"title107"},{"id":108,"title":"title108"},{"id":1000,"title":"title1000"},{"id":1001,"title":"title1001"},{"id":1002,"title":"title1002"},{"id":1003,"title":"title1003"},{"id":1004,"title":"title1004"},{"id":1005,"title":"title1005"},{"id":1006,"title":"title1006"},{"id":1007,"title":"title1007"}]"###);
}

#[actix_rt::test]
async fn search_with_stop_word() {
    // related to https://github.com/meilisearch/meilisearch/issues/4984
    let server = Server::new_shared();
    let index = server.unique_index();

    let (_, code) = index
        .update_settings(json!({"stopWords": ["the", "The", "a", "an", "to", "in", "of"]}))
        .await;
    snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    // prefix search
    index
        .search(json!({"q": "to the", "attributesToHighlight": ["title"], "attributesToRetrieve": ["title"] }), |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @"[]");
        })
        .await;

    // non-prefix search
    index
          .search(json!({"q": "to the ", "attributesToHighlight": ["title"], "attributesToRetrieve": ["title"] }), |response, code| {
              assert_eq!(code, 200, "{response}");
              snapshot!(json_string!(response["hits"]), @r###"
              [
                {
                  "title": "Shazam!",
                  "_formatted": {
                    "title": "Shazam!"
                  }
                },
                {
                  "title": "Captain Marvel",
                  "_formatted": {
                    "title": "Captain Marvel"
                  }
                },
                {
                  "title": "Escape Room",
                  "_formatted": {
                    "title": "Escape Room"
                  }
                },
                {
                  "title": "How to Train Your Dragon: The Hidden World",
                  "_formatted": {
                    "title": "How to Train Your Dragon: The Hidden World"
                  }
                },
                {
                  "title": "Gläss",
                  "_formatted": {
                    "title": "Gläss"
                  }
                }
              ]
              "###);
          })
          .await;
}

#[actix_rt::test]
async fn search_with_typo_settings() {
    // related to https://github.com/meilisearch/meilisearch/issues/5240
    let server = Server::new_shared();
    let index = server.unique_index();

    let (_, code) = index
        .update_settings(json!({"typoTolerance": { "disableOnAttributes": ["title", "id"]}}))
        .await;
    snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "287947" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "Shazam!",
                "id": "287947",
                "color": [
                  "green",
                  "blue"
                ]
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn phrase_search_with_stop_word() {
    // related to https://github.com/meilisearch/meilisearch/issues/3521
    let server = Server::new_shared();
    let index = server.unique_index();

    let (_, code) = index.update_settings(json!({"stopWords": ["the", "of"]})).await;
    snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "how \"to\" train \"the" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[actix_rt::test]
async fn negative_phrase_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "-\"train your dragon\"" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 4);
            assert_eq!(hits[0]["id"], "287947");
            assert_eq!(hits[1]["id"], "299537");
            assert_eq!(hits[2]["id"], "522681");
            assert_eq!(hits[3]["id"], "450465");
        })
        .await;
}

#[actix_rt::test]
async fn negative_word_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "-escape" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 4);
            assert_eq!(hits[0]["id"], "287947");
            assert_eq!(hits[1]["id"], "299537");
            assert_eq!(hits[2]["id"], "166428");
            assert_eq!(hits[3]["id"], "450465");
        })
        .await;

    // Everything that contains derivates of escape but not escape: nothing
    index
        .search(json!({"q": "-escape escape" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 0);
        })
        .await;
}

#[actix_rt::test]
async fn non_negative_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "- escape" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["id"], "522681");
        })
        .await;

    index
        .search(json!({"q": "- \"train your dragon\"" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["id"], "166428");
        })
        .await;
}

#[actix_rt::test]
async fn negative_special_cases_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) =
        index.update_settings(json!({"synonyms": { "escape": ["gläss"] }})).await;
    server.wait_task(task.uid()).await.succeeded();

    // There is a synonym for escape -> glass but we don't want "escape", only the derivates: glass
    index
        .search(json!({"q": "-escape escape" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["id"], "450465");
        })
        .await;
}

#[cfg(feature = "default")]
#[cfg(not(feature = "chinese-pinyin"))]
#[actix_rt::test]
async fn test_kanji_language_detection() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
        { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
        { "id": 1, "title": "東京のお寿司。" },
        { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "東京"}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn test_thai_language() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // We don't need documents, the issue is on the query side only.
    let documents = json!([
        { "id": 0, "title": "สบู่สมุนไพรดอกดาวเรือง 100 กรัม จำนวน 6 ก้อน" },
        { "id": 1, "title": "สบู่สมุนไพรชาเขียว 100 กรัม จำนวน 6 ก้อน" },
        { "id": 2, "title": "สบู่สมุนไพรฝางแดงผสมว่านหางจรเข้ 100 กรัม จำนวน 6 ก้อน" }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) = index.update_settings(json!({"rankingRules": ["exactness"]})).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "สบู"}), |response, code| {
            assert_eq!(code, 200, "{response}");
        })
        .await;
}

#[actix_rt::test]
async fn search_multiple_params() {
    let index = shared_index_with_documents().await;
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
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;
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
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_sort_on_numbers() {
    let index = shared_index_with_documents().await;
    index
        .search(
            json!({
                "sort": ["id:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(
            json!({
                "sort": ["doggos.age:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_sort_on_strings() {
    let index = shared_index_with_documents().await;
    index
        .search(
            json!({
                "sort": ["title:desc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(
            json!({
                "sort": ["doggos.name:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_multiple_sort() {
    let index = shared_index_with_documents().await;
    let (response, code) = index
        .search_post(json!({
            "sort": ["id:asc", "title:desc"]
        }))
        .await;
    assert_eq!(code, 200, "{response}");
    assert_eq!(response["hits"].as_array().unwrap().len(), 5);
}

#[actix_rt::test]
async fn search_facet_distribution() {
    let index = shared_index_with_documents().await;
    index
        .search(
            json!({
                "facets": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert!(dist.get("title").is_some());
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;

    // TODO: TAMO: fix the test
    index
        .search(
            json!({
                // "facets": ["father", "doggos.name"]
                "facets": ["father"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
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

    index
        .search(
            json!({
                "facets": ["doggos.name"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1, "{dist:?}");
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
                assert_eq!(code, 200, "{response}");
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 3, "{dist:?}");
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
                assert_eq!(dist["doggos.age"], json!({ "2": 1, "4": 1, "5": 1, "6": 1, "8": 1}));
            },
        )
        .await;

    index
        .search(
            json!({
                "facets": ["doggos.name"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
            },
        )
        .await;
}

#[actix_rt::test]
async fn displayed_attributes() {
    let server = Server::new_shared();
    let index = server.unique_index();

    index.update_settings(json!({ "displayedAttributes": ["title"] })).await;

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.search_post(json!({ "attributesToRetrieve": ["title", "id"] })).await;
    assert_eq!(code, 200, "{response}");
    assert!(response["hits"][0].get("title").is_some());
}

#[actix_rt::test]
async fn placeholder_search_is_hard_limited() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents: Vec<_> = (0..1200).map(|i| json!({ "id": i, "text": "I am unique!" })).collect();
    let (task, _status_code) = index.add_documents(documents.into(), None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
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
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    let (task, _status_code) =
        index.update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } })).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
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
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_is_hard_limited() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents: Vec<_> = (0..1200).map(|i| json!({ "id": i, "text": "I am unique!" })).collect();
    let (task, _status_code) = index.add_documents(documents.into(), None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
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
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    let (task, _status_code) =
        index.update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } })).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
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
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn faceting_max_values_per_facet() {
    let server = Server::new_shared();
    let index = server.unique_index();

    index.update_settings(json!({ "filterableAttributes": ["number"] })).await;

    let documents: Vec<_> = (0..10_000).map(|id| json!({ "id": id, "number": id * 10 })).collect();
    let (task, _status_code) = index.add_documents(json!(documents), None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                let numbers = response["facetDistribution"]["number"].as_object().unwrap();
                assert_eq!(numbers.len(), 100);
            },
        )
        .await;

    let (task, _status_code) =
        index.update_settings(json!({ "faceting": { "maxValuesPerFacet": 10_000 } })).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                let numbers = &response["facetDistribution"]["number"].as_object().unwrap();
                assert_eq!(numbers.len(), 10_000);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_score_details() {
    let index = shared_index_with_documents().await;

    index
        .search(
            json!({
                "q": "train dragon",
                "showRankingScoreDetails": true,
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "How to Train Your Dragon: The Hidden World",
                    "id": "166428",
                    "color": [
                      "green",
                      "red"
                    ],
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
                        "score": 0.8095238095238095
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
async fn test_score() {
    let index = shared_index_with_score_documents().await;

    index
        .search(
            json!({
                "q": "Badman the dark knight returns 1",
                "showRankingScore": true,
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.9746605609456898
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.8055252965383685
                  },
                  {
                    "title": "Badman",
                    "id": "E",
                    "_rankingScore": 0.16666666666666666
                  },
                  {
                    "title": "Batman Returns",
                    "id": "C",
                    "_rankingScore": 0.07702020202020202
                  },
                  {
                    "title": "Batman",
                    "id": "D",
                    "_rankingScore": 0.07702020202020202
                  }
                ]
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_score_threshold() {
    let query = "Badman dark returns 1";
    let index = shared_index_with_score_documents().await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.0
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["estimatedTotalHits"]), @"5");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.6685627880184332
                  },
                  {
                    "title": "Badman",
                    "id": "E",
                    "_rankingScore": 0.25
                  },
                  {
                    "title": "Batman Returns",
                    "id": "C",
                    "_rankingScore": 0.11553030303030302
                  },
                  {
                    "title": "Batman",
                    "id": "D",
                    "_rankingScore": 0.11553030303030302
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.2
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["estimatedTotalHits"]), @r###"3"###);
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.6685627880184332
                  },
                  {
                    "title": "Badman",
                    "id": "E",
                    "_rankingScore": 0.25
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.5
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["estimatedTotalHits"]), @r###"2"###);
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.6685627880184332
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.8
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["estimatedTotalHits"]), @r###"1"###);
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 1.0
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["estimatedTotalHits"]), @r###"0"###);
                // nobody is perfect
                snapshot!(json_string!(response["hits"]), @"[]");
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_degraded_score_details() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = NESTED_DOCUMENTS.clone();

    index.add_documents(json!(documents), None).await;
    // We can't really use anything else than 0ms here; otherwise, the test will get flaky.
    let (res, _code) = index.update_settings(json!({ "searchCutoffMs": 0 })).await;
    server.wait_task(res.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "b",
                "attributesToRetrieve": ["doggos.name", "cattos"],
                "showRankingScoreDetails": true,
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
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
                        "skipped": {
                          "order": 0
                        }
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
                        "skipped": {
                          "order": 0
                        }
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
                        "skipped": {
                          "order": 0
                        }
                      }
                    }
                  ],
                  "query": "b",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 3,
                  "requestUid": "[uuid]"
                }
                "###);
            },
        )
        .await;
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn camelcased_words() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // related to https://github.com/meilisearch/meilisearch/issues/3818
    let documents = json!([
        { "id": 0, "title": "DeLonghi" },
        { "id": 1, "title": "delonghi" },
        { "id": 2, "title": "TestAB" },
        { "id": 3, "title": "TestAb" },
        { "id": 4, "title": "testab" },
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "deLonghi"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
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
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _status_code) =
        index.update_settings(json!({ "synonyms": {"&": ["to"], "to": ["&"]} })).await;
    let r = server.wait_task(task.uid()).await.succeeded();
    snapshot!(r["status"], @r###""succeeded""###);

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "How to train"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "color": [
                  "green",
                  "red"
                ]
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "How & train"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "color": [
                  "green",
                  "red"
                ]
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "to"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "color": [
                  "green",
                  "red"
                ]
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn change_attributes_settings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    index.update_settings(json!({ "searchableAttributes": ["father", "mother"] })).await;

    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(json!(documents), None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task,_status_code) =
        index.update_settings(json!({ "searchableAttributes": ["father", "mother", "doggos"], "filterableAttributes": ["doggos"] })).await;
    server.wait_task(task.uid()).await.succeeded();

    // search
    index
        .search(
            json!({
                "q": "bobby",
                "attributesToRetrieve": ["id", "doggos"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ]
                  }
                ]
                "###);
            },
        )
        .await;

    // filter
    index
        .search(
            json!({
                "q": "",
                "filter": "doggos.age < 5",
                "attributesToRetrieve": ["id", "doggos"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ]
                  }
                ]
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_nested_fields() {
    let documents = json!([
        {
            "id": 0,
            "title": "The zeroth document",
        },
        {
            "id": 1,
            "title": "The first document",
            "nested": {
                "object": "field",
                "machin": "bidule",
            },
        },
        {
            "id": 2,
            "title": "The second document",
            "nested": [
                "array",
                {
                    "object": "field",
                },
                {
                    "prout": "truc",
                    "machin": "lol",
                },
            ],
        },
        {
            "id": 3,
            "title": "The third document",
            "nested": "I lied",
        },
    ]);

    let settings = json!({
        "searchableAttributes": ["title", "nested.object", "nested.machin"],
        "filterableAttributes": ["title", "nested.object", "nested.machin"]
    });

    // Test empty search returns all documents
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"q": "document"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
        [
          {
            "id": 0,
            "title": "The zeroth document"
          },
          {
            "id": 1,
            "title": "The first document",
            "nested": {
              "object": "field",
              "machin": "bidule"
            }
          },
          {
            "id": 2,
            "title": "The second document",
            "nested": [
              "array",
              {
                "object": "field"
              },
              {
                "prout": "truc",
                "machin": "lol"
              }
            ]
          },
          {
            "id": 3,
            "title": "The third document",
            "nested": "I lied"
          }
        ]
        "###);
        },
    )
    .await;

    // Test searching specific documents
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"q": "zeroth"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
        [
          {
            "id": 0,
            "title": "The zeroth document"
          }
        ]
        "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"q": "first"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
        [
          {
            "id": 1,
            "title": "The first document",
            "nested": {
              "object": "field",
              "machin": "bidule"
            }
          }
        ]
        "###);
        },
    )
    .await;

    // Test searching nested fields
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"q": "field"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
      [
        {
          "id": 1,
          "title": "The first document",
          "nested": {
            "object": "field",
            "machin": "bidule"
          }
        },
        {
          "id": 2,
          "title": "The second document",
          "nested": [
            "array",
            {
              "object": "field"
            },
            {
              "prout": "truc",
              "machin": "lol"
            }
          ]
        }
      ]
      "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"q": "array"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            // nested is not searchable
            snapshot!(json_string!(response["hits"]), @"[]");
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"q": "lied"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            // nested is not searchable
            snapshot!(json_string!(response["hits"]), @"[]");
        },
    )
    .await;

    // Test filtering on nested fields
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"filter": "nested.object = field"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
        [
          {
            "id": 1,
            "title": "The first document",
            "nested": {
              "object": "field",
              "machin": "bidule"
            }
          },
          {
            "id": 2,
            "title": "The second document",
            "nested": [
              "array",
              {
                "object": "field"
              },
              {
                "prout": "truc",
                "machin": "lol"
              }
            ]
          }
        ]
        "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"filter": "nested.machin = bidule"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
        [
          {
            "id": 1,
            "title": "The first document",
            "nested": {
              "object": "field",
              "machin": "bidule"
            }
          }
        ]
        "###);
        },
    )
    .await;

    // Test filtering on non-filterable nested field fails
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"filter": "nested = array"}),
        |response, code| {
            assert_eq!(code, 400, "{response}");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `[uuid]`: Attribute `nested` is not filterable. Available filterable attribute patterns are: `nested.machin`, `nested.object`, `title`.\n1:7 nested = array",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
        },
    )
    .await;

    // Test filtering on non-filterable nested field fails
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &settings,
        &json!({"filter": r#"nested = "I lied""#}),
        |response, code| {
            assert_eq!(code, 400, "{response}");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `[uuid]`: Attribute `nested` is not filterable. Available filterable attribute patterns are: `nested.machin`, `nested.object`, `title`.\n1:7 nested = \"I lied\"",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
        },
    )
    .await;
}

#[actix_rt::test]
async fn test_typo_settings() {
    let documents = json!([
        {
            "id": 0,
            "title": "The zeroth document",
        },
        {
            "id": 1,
            "title": "The first document",
            "nested": {
                "object": "field",
                "machin": "bidule",
            },
        },
        {
            "id": 2,
            "title": "The second document",
            "nested": [
                "array",
                {
                    "object": "field",
                },
                {
                    "prout": "truc",
                    "machin": "lol",
                },
            ],
        },
        {
            "id": 3,
            "title": "The third document",
            "nested": "I lied",
        },
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "searchableAttributes": ["title", "nested.object", "nested.machin"],
            "typoTolerance": {
              "enabled": true,
              "disableOnAttributes": ["title"]
            }
        }),
        &json!({"q": "document"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 0,
                "title": "The zeroth document"
              },
              {
                "id": 1,
                "title": "The first document",
                "nested": {
                  "object": "field",
                  "machin": "bidule"
                }
              },
              {
                "id": 2,
                "title": "The second document",
                "nested": [
                  "array",
                  {
                    "object": "field"
                  },
                  {
                    "prout": "truc",
                    "machin": "lol"
                  }
                ]
              },
              {
                "id": 3,
                "title": "The third document",
                "nested": "I lied"
              }
            ]
            "###);
        },
    )
    .await;

    // Test prefix search
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "searchableAttributes": ["title", "nested.object", "nested.machin"],
            "typoTolerance": {
              "enabled": true,
              "disableOnAttributes": ["title"]
            }
        }),
        &json!({"q": "docume"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
          [
            {
              "id": 0,
              "title": "The zeroth document"
            },
            {
              "id": 1,
              "title": "The first document",
              "nested": {
                "object": "field",
                "machin": "bidule"
              }
            },
            {
              "id": 2,
              "title": "The second document",
              "nested": [
                "array",
                {
                  "object": "field"
                },
                {
                  "prout": "truc",
                  "machin": "lol"
                }
              ]
            },
            {
              "id": 3,
              "title": "The third document",
              "nested": "I lied"
            }
          ]
          "###);
        },
    )
    .await;
}

/// Modifying facets with different casing should work correctly
#[actix_rt::test]
async fn change_facet_casing() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!({
            "filterableAttributes": ["dog"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    server.wait_task(response.uid()).await.succeeded();

    let (response, _code) = index
        .add_documents(
            json!([
                {
                    "id": 1,
                    "dog": "Bouvier Bernois"
                }
            ]),
            None,
        )
        .await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, _code) = index
        .add_documents(
            json!([
                {
                    "id": 1,
                    "dog": "bouvier bernois"
                }
            ]),
            None,
        )
        .await;
    server.wait_task(response.uid()).await.succeeded();

    index
        .search(json!({ "facets": ["dog"] }), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["facetDistribution"]), @r###"
            {
              "dog": {
                "bouvier bernois": 1
              }
            }
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn test_exact_typos_terms() {
    let documents = json!([
        {
            "id": 0,
            "title": "The zeroth document 1298484",
        },
        {
            "id": 1,
            "title": "The first document 234342",
            "nested": {
                "object": "field 22231",
                "machin": "bidule 23443.32111",
            },
        },
        {
            "id": 2,
            "title": "The second document 3398499",
            "nested": [
                "array",
                {
                    "object": "field 23245121,23223",
                },
                {
                    "prout": "truc 123980612321",
                    "machin": "lol 12345645333447879",
                },
            ],
        },
        {
            "id": 3,
            "title": "The third document 12333",
            "nested": "I lied 98878",
        },
    ]);

    // Test prefix search
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "searchableAttributes": ["title", "nested.object", "nested.machin"],
            "typoTolerance": {
              "enabled": true,
              "disableOnNumbers": true
            }
        }),
        &json!({"q": "12345"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "The second document 3398499",
                "nested": [
                  "array",
                  {
                    "object": "field 23245121,23223"
                  },
                  {
                    "prout": "truc 123980612321",
                    "machin": "lol 12345645333447879"
                  }
                ]
              }
            ]
            "###);
        },
    )
    .await;

    // Test typo search
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "searchableAttributes": ["title", "nested.object", "nested.machin"],
            "typoTolerance": {
              "enabled": true,
              "disableOnNumbers": true
            }
        }),
        &json!({"q": "123457"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response["hits"]), @r###"[]"###);
        },
    )
    .await;
}

#[actix_rt::test]
async fn simple_search_changing_unrelated_settings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "Dragon"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "color": [
                  "green",
                  "red"
                ]
              }
            ]
            "###);
        })
        .await;

    let (task, _status_code) =
        index.update_settings(json!({ "filterableAttributes": ["title"] })).await;
    let r = server.wait_task(task.uid()).await.succeeded();
    snapshot!(r["status"], @r###""succeeded""###);

    index
        .search(json!({"q": "Dragon"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "How to Train Your Dragon: The Hidden World",
                    "id": "166428",
                    "color": [
                      "green",
                      "red"
                    ]
                  }
                ]
                "###);
        })
        .await;

    let (task, _status_code) = index.update_settings(json!({ "filterableAttributes": [] })).await;
    let r = server.wait_task(task.uid()).await.succeeded();
    snapshot!(r["status"], @r###""succeeded""###);

    index
        .search(json!({"q": "Dragon"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
                    [
                      {
                        "title": "How to Train Your Dragon: The Hidden World",
                        "id": "166428",
                        "color": [
                          "green",
                          "red"
                        ]
                      }
                    ]
                    "###);
        })
        .await;
}

#[actix_rt::test]
async fn ranking_score_bug_with_sort() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create documents with a "created" field for sorting
    let documents = json!([
        {
            "id": "1",
            "title": "Coffee Mug",
            "created": "2023-01-01T00:00:00Z"
        },
        {
            "id": "2",
            "title": "Water Bottle",
            "created": "2023-01-02T00:00:00Z"
        },
        {
            "id": "3",
            "title": "Tumbler Cup",
            "created": "2023-01-03T00:00:00Z"
        },
        {
            "id": "4",
            "title": "Stainless Steel Tumbler",
            "created": "2023-01-04T00:00:00Z"
        }
    ]);

    // Add documents
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    // Configure sortable attributes
    let (task, code) = index
        .update_settings(json!({
            "sortableAttributes": ["created"]
        }))
        .await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    // Test 1: Search without sort - should have proper ranking scores
    index
        .search(
            json!({
                "q": "tumbler",
                "showRankingScore": true,
                "rankingScoreThreshold": 0.0,
                "attributesToRetrieve": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Tumbler Cup",
                    "_rankingScore": 0.9848484848484848
                  },
                  {
                    "title": "Stainless Steel Tumbler",
                    "_rankingScore": 0.8787878787878788
                  }
                ]
                "###);
            },
        )
        .await;

    // Test 2: Search with sort - this is where the bug occurs
    index
        .search(
            json!({
                "q": "tumbler",
                "showRankingScore": true,
                "rankingScoreThreshold": 0.0,
                "sort": ["created:desc"],
                "attributesToRetrieve": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Tumbler Cup",
                    "_rankingScore": 0.9848484848484848
                  },
                  {
                    "title": "Stainless Steel Tumbler",
                    "_rankingScore": 0.8787878787878788
                  }
                ]
                "###);
            },
        )
        .await;
}
