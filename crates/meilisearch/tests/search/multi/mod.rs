use meili_snap::{json_string, snapshot};
use tokio::sync::OnceCell;

use super::{DOCUMENTS, FRUITS_DOCUMENTS, NESTED_DOCUMENTS};
use crate::common::index::Index;
use crate::common::{
    shared_index_with_documents, shared_index_with_nested_documents,
    shared_index_with_score_documents, Server, Shared,
};
use crate::json;
use crate::search::{SCORE_DOCUMENTS, VECTOR_DOCUMENTS};

mod proxy;

pub async fn shared_movies_index() -> &'static Index<'static, Shared> {
    static INDEX: OnceCell<Index<'static, Shared>> = OnceCell::const_new();
    INDEX
        .get_or_init(|| async {
            let server = Server::new_shared();
            let movies_index = server.unique_index_with_prefix("movies");

            let documents = DOCUMENTS.clone();
            let (response, _code) = movies_index.add_documents(documents, None).await;
            server.wait_task(response.uid()).await.succeeded();

            let (value, _) = movies_index
                .update_settings(json!({
                    "sortableAttributes": ["title"],
                    "filterableAttributes": ["title", "color"],
                    "rankingRules": [
                        "sort",
                        "words",
                        "typo",
                        "proximity",
                        "attribute",
                        "exactness"
                    ]
                }))
                .await;
            server.wait_task(value.uid()).await.succeeded();
            movies_index.to_shared()
        })
        .await
}

pub async fn shared_batman_index() -> &'static Index<'static, Shared> {
    static INDEX: OnceCell<Index<'static, Shared>> = OnceCell::const_new();
    INDEX
        .get_or_init(|| async {
            let server = Server::new_shared();
            let batman_index = server.unique_index_with_prefix("batman");

            let documents = SCORE_DOCUMENTS.clone();
            let (response, _code) = batman_index.add_documents(documents, None).await;
            server.wait_task(response.uid()).await.succeeded();

            let (value, _) = batman_index
                .update_settings(json!({
                    "sortableAttributes": ["id", "title"],
                    "filterableAttributes": ["title"],
                    "rankingRules": [
                        "sort",
                        "words",
                        "typo",
                        "proximity",
                        "attribute",
                        "exactness"
                    ]
                }))
                .await;
            server.wait_task(value.uid()).await.succeeded();
            batman_index.to_shared()
        })
        .await
}

#[actix_rt::test]
async fn search_empty_list() {
    let server = Server::new_shared();

    let (response, code) = server.multi_search(json!({"queries": []})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "results": []
    }
    "###);
}

#[actix_rt::test]
async fn federation_empty_list() {
    let server = Server::new_shared();

    let (response, code) = server.multi_search(json!({"federation": {}, "queries": []})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn search_json_object() {
    let server = Server::new_shared();

    let (response, code) = server.multi_search(json!({})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `queries`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn federation_no_queries() {
    let server = Server::new_shared();

    let (response, code) = server.multi_search(json!({"federation": {}})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `queries`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn search_json_array() {
    let server = Server::new_shared();

    let (response, code) = server.multi_search(json!([])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an object, but found an array: `[]`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_single_index() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid": index.uid, "q": "glass"},
        {"indexUid": index.uid, "q": "captain"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["results"], { ".**.processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    [
      {
        "indexUid": "SHARED_DOCUMENTS",
        "hits": [
          {
            "title": "Gläss",
            "id": "450465",
            "color": [
              "blue",
              "red"
            ]
          }
        ],
        "query": "glass",
        "processingTimeMs": "[duration]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1,
        "requestUid": "[uuid]"
      },
      {
        "indexUid": "SHARED_DOCUMENTS",
        "hits": [
          {
            "title": "Captain Marvel",
            "id": "299537",
            "color": [
              "yellow",
              "blue"
            ]
          }
        ],
        "query": "captain",
        "processingTimeMs": "[duration]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1,
        "requestUid": "[uuid]"
      }
    ]
    "###);
}

#[actix_rt::test]
async fn federation_single_search_single_index() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_search_single_index() {
    let server = Server::new_shared();
    let index = shared_index_with_score_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid": index.uid, "q": "the bat"},
        {"indexUid": index.uid, "q": "badman returns"},
        {"indexUid" : index.uid, "q": "batman"},
        {"indexUid": index.uid, "q": "batman returns"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 3,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 0.5
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_two_search_single_index() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": index.uid, "q": "captain"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_missing_index_uid() {
    let server = Server::new_shared();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Missing field `indexUid` inside `.queries[0]`",
      "code": "missing_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn federation_simple_search_missing_index_uid() {
    let server = Server::new_shared();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Missing field `indexUid` inside `.queries[0]`",
      "code": "missing_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_illegal_index_uid() {
    let server = Server::new_shared();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid": "hé", "q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.queries[0].indexUid`: `hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn federation_search_illegal_index_uid() {
    let server = Server::new_shared();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid": "hé", "q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.queries[0].indexUid`: `hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_two_indexes() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": nested_index.uid, "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["results"], { ".**.processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    [
      {
        "indexUid": "SHARED_DOCUMENTS",
        "hits": [
          {
            "title": "Gläss",
            "id": "450465",
            "color": [
              "blue",
              "red"
            ]
          }
        ],
        "query": "glass",
        "processingTimeMs": "[duration]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1,
        "requestUid": "[uuid]"
      },
      {
        "indexUid": "SHARED_NESTED_DOCUMENTS",
        "hits": [
          {
            "id": 852,
            "father": "jean",
            "mother": "michelle",
            "doggos": [
              {
                "name": "bobby",
                "age": 2
              },
              {
                "name": "buddy",
                "age": 4
              }
            ],
            "cattos": "pésti"
          },
          {
            "id": 654,
            "father": "pierre",
            "mother": "sabine",
            "doggos": [
              {
                "name": "gros bill",
                "age": 8
              }
            ],
            "cattos": [
              "simba",
              "pestiféré"
            ]
          }
        ],
        "query": "pésti",
        "processingTimeMs": "[duration]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 2,
        "requestUid": "[uuid]"
      }
    ]
    "###);
}

#[actix_rt::test]
async fn federation_two_search_two_indexes() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": nested_index.uid, "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "SHARED_NESTED_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "SHARED_NESTED_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7803030303030303
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_search_multiple_indexes() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let nested_index = shared_index_with_nested_documents().await;

    let score_index = shared_index_with_score_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid" : index.uid, "q": "captain"},
        {"indexUid": nested_index.uid, "q": "pésti"},
        {"indexUid" : index.uid, "q": "Escape"},
        {"indexUid": nested_index.uid, "q": "jean"},
        {"indexUid": score_index.uid, "q": "jean"},
        {"indexUid": index.uid, "q": "the bat"},
        {"indexUid": score_index.uid, "q": "the bat"},
        {"indexUid": score_index.uid, "q": "badman returns"},
        {"indexUid" : score_index.uid, "q": "batman"},
        {"indexUid": score_index.uid, "q": "batman returns"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "SHARED_NESTED_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 9,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 10,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 3,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "SHARED_NESTED_DOCUMENTS",
            "queriesPosition": 4,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 9,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 9,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "SHARED_NESTED_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 0.7803030303030303
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 8,
            "weightedRankingScore": 0.5
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "SHARED_DOCUMENTS",
            "queriesPosition": 6,
            "weightedRankingScore": 0.4166666666666667
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 12,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn search_one_index_doesnt_exist() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn federation_one_index_doesnt_exist() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn search_multiple_indexes_dont_exist() {
    let server = Server::new_shared();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Index `test` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_indexes_dont_exist() {
    let server = Server::new_shared();

    let index_1 = server.unique_index_with_prefix("index_1");
    let index_2 = server.unique_index_with_prefix("index_2");

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index_1.uid, "q": "glass"},
        {"indexUid": index_2.uid, "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    // order of indexes that are not found depends on the alphabetical order of index names
    // the query index is the lowest index with that index
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Index `index_1-[uuid]` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn search_one_query_error() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass", "facets": ["color"]},
        {"indexUid": nested_index.uid, "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Invalid facet distribution: Attribute `color` is not filterable. Available filterable attributes patterns are: `id, title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn federation_one_query_error() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;

    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": nested_index.uid, "q": "pésti", "filter": ["title = toto"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `SHARED_NESTED_DOCUMENTS`: Attribute `title` is not filterable. Available filterable attribute patterns are: `cattos`, `doggos`, `father`.\n1:6 title = toto",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "###);
}

#[actix_rt::test]
async fn federation_one_query_sort_error() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": nested_index.uid, "q": "pésti", "sort": ["mother:desc"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `SHARED_NESTED_DOCUMENTS`: Attribute `mother` is not sortable. Available sortable attributes are: `doggos`.",
      "code": "invalid_search_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    }
    "###);
}

#[actix_rt::test]
async fn search_multiple_query_errors() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass", "facets": ["color"]},
        {"indexUid": nested_index.uid, "q": "pésti", "facets": ["doggos"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Invalid facet distribution: Attribute `color` is not filterable. Available filterable attributes patterns are: `id, title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_errors() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass", "filter": ["color = toto"]},
        {"indexUid": nested_index.uid, "q": "pésti", "filter": ["mother IN [intel, kefir]"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Index `SHARED_DOCUMENTS`: Attribute `color` is not filterable. Available filterable attribute patterns are: `id`, `title`.\n1:6 color = toto",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_sort_errors() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass", "sort": ["color:desc"]},
        {"indexUid": nested_index.uid, "q": "pésti", "sort": ["doggos:desc"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Index `SHARED_DOCUMENTS`: Attribute `color` is not sortable. Available sortable attributes are: `id, title`.",
      "code": "invalid_search_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_errors_interleaved() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": nested_index.uid, "q": "pésti", "filter": ["mother IN [intel, kefir]"]},
        {"indexUid" : index.uid, "q": "glass", "filter": ["title = toto"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `SHARED_NESTED_DOCUMENTS`: Attribute `mother` is not filterable. Available filterable attribute patterns are: `cattos`, `doggos`, `father`.\n1:7 mother IN [intel, kefir]",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_sort_errors_interleaved() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid, "q": "glass"},
        {"indexUid": nested_index.uid, "q": "pésti", "sort": ["mother:desc"]},
        {"indexUid" : index.uid, "q": "glass", "sort": ["title:desc"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `SHARED_NESTED_DOCUMENTS`: Attribute `mother` is not sortable. Available sortable attributes are: `doggos`.",
      "code": "invalid_search_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    }
    "###);
}

#[actix_rt::test]
async fn federation_filter() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red", "filter": "BOOST = true", "showRankingScore": true, "federationOptions": {"weight": 3.0}},
        {"indexUid": index.uid, "q": "apple red", "showRankingScore": true},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "Exclusive sale: Red delicious apple",
          "id": "red-delicious-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 2.7281746031746033
          },
          "_rankingScore": 0.9093915343915344
        },
        {
          "name": "Exclusive sale: green apple",
          "id": "green-apple-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.318181818181818
          },
          "_rankingScore": 0.4393939393939394
        },
        {
          "name": "Red apple gala",
          "id": "red-apple-gala",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.953042328042328
          },
          "_rankingScore": 0.953042328042328
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_same_criterion_same_direction() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    // two identical placeholder searches should have all results from the first query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "", "sort": ["mother:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 750,
          "father": "romain",
          "mother": "michelle",
          "cattos": [
            "enigma"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "requestUid": "[uuid]"
    }
    "###);

    // mix and match query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "pésti", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "jean", "sort": ["mother:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7803030303030303
          },
          "_rankingScore": 0.7803030303030303
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScore": 0.9848484848484848
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_same_criterion_opposite_direction() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    // two identical placeholder searches should have all results from the first query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "", "sort": ["mother:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `[uuid].rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `[uuid].rankingRules[0]`: descending sort rule(s) on field `mother`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: should be ranked by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "pésti", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "jean", "sort": ["mother:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `[uuid].rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `[uuid].rankingRules[0]`: descending sort rule(s) on field `mother`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_different_criterion_same_direction() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother", "father"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    // return mothers and fathers ordered across fields.
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "", "sort": ["father:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 750,
          "father": "romain",
          "mother": "michelle",
          "cattos": [
            "enigma"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "requestUid": "[uuid]"
    }
    "###);

    // mix and match query: will be sorted across mother and father names
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "pésti", "sort": ["mother:desc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "jean-bap", "sort": ["father:desc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "jea", "sort": ["father:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7803030303030303
          },
          "_rankingScore": 0.7803030303030303
        },
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9991181657848324
          },
          "_rankingScore": 0.9991181657848324
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_different_criterion_opposite_direction() {
    let server = Server::new_shared();
    let index = server.unique_index_with_prefix("nested");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother", "father"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    // two identical placeholder searches should have all results from the first query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "", "sort": ["father:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `nested-[uuid].rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `nested-[uuid].rankingRules[0]`: descending sort rule(s) on field `father`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: should be ranked by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "q": "pésti", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : index.uid, "q": "jean", "sort": ["father:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `nested-[uuid].rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `nested-[uuid].rankingRules[0]`: descending sort rule(s) on field `father`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_same_criterion_same_direction() {
    let server = Server::new_shared();
    let movies_index = shared_movies_index().await;
    let batman_index = shared_batman_index().await;

    // return titles ordered across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : movies_index.uid, "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "", "sort": ["title:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Shazam!",
          "id": "287947",
          "color": [
            "green",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 10,
      "requestUid": "[uuid]"
    }
    "###);

    // mix and match query: will be sorted across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : batman_index.uid, "q": "badman returns", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : movies_index.uid, "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "the bat", "sort": ["title:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScore": 0.9848484848484848
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8317901234567902
          },
          "_rankingScore": 0.8317901234567902
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605
          },
          "_rankingScore": 0.23106060606060605
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.5
          },
          "_rankingScore": 0.5
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 6,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_ranking_rules() {
    let server = Server::new_shared();

    let movies_index = shared_movies_index().await;

    let batman_index = shared_index_with_score_documents().await;

    // return titles ordered across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : movies_index.uid, "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "", "sort": ["title:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Shazam!",
          "id": "287947",
          "color": [
            "green",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 10,
      "requestUid": "[uuid]"
    }
    "###);

    // mix and match query: order difficult to understand
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : batman_index.uid, "q": "badman returns", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : movies_index.uid, "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "the bat", "sort": ["title:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #2 and #1 are incompatible: \n  1. `queries[2]`, `SHARED_SCORE_DOCUMENTS.rankingRules[0..=3]`: relevancy rule(s) words, typo, proximity, attribute\n  2. `queries[1].sort[0]`, `movies-[uuid].rankingRules[0]`: descending sort rule(s) on field `title`\n  - cannot compare a relevancy rule with a sort rule\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_same_criterion_opposite_direction() {
    let server = Server::new_shared();
    let movies_index = shared_movies_index().await;
    let batman_index = shared_batman_index().await;

    // all results from query 0
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : movies_index.uid, "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "", "sort": ["title:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: The results of queries #1 and #0 are incompatible: \n  1. `queries[1].sort[0]`, `batman-[uuid].rankingRules[0]`: descending sort rule(s) on field `title`\n  2. `queries[0].sort[0]`, `movies-[uuid].rankingRules[0]`: ascending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: will be sorted by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : batman_index.uid, "q": "badman returns", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : movies_index.uid, "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "the bat", "sort": ["title:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #2 and #1 are incompatible: \n  1. `queries[2].sort[0]`, `batman-[uuid].rankingRules[0]`: ascending sort rule(s) on field `title`\n  2. `queries[1].sort[0]`, `movies-[uuid].rankingRules[0]`: descending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_different_criterion_same_direction() {
    let server = Server::new_shared();
    let movies_index = shared_movies_index().await;
    let batman_index = shared_batman_index().await;

    // return titles ordered across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : movies_index.uid, "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "", "sort": ["id:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Shazam!",
          "id": "287947",
          "color": [
            "green",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 10,
      "requestUid": "[uuid]"
    }
    "###);

    // mix and match query: will be sorted across indexes and criterion
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : batman_index.uid, "q": "badman returns", "sort": ["id:desc"], "showRankingScore": true },
          {"indexUid" : movies_index.uid, "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "the bat", "sort": ["id:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.5
          },
          "_rankingScore": 0.5
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605
          },
          "_rankingScore": 0.23106060606060605
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScore": 0.9848484848484848
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8317901234567902
          },
          "_rankingScore": 0.8317901234567902
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 6,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_different_criterion_opposite_direction() {
    let server = Server::new_shared();
    let movies_index = shared_movies_index().await;
    let batman_index = shared_batman_index().await;

    // all results from query 0 first
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : movies_index.uid, "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "", "sort": ["id:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: The results of queries #1 and #0 are incompatible: \n  1. `queries[1].sort[0]`, `batman-[uuid].rankingRules[0]`: descending sort rule(s) on field `id`\n  2. `queries[0].sort[0]`, `movies-[uuid].rankingRules[0]`: ascending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: more or less by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : batman_index.uid, "q": "badman returns", "sort": ["id:desc"], "showRankingScore": true },
          {"indexUid" : movies_index.uid, "q": "captain", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : batman_index.uid, "q": "the bat", "sort": ["id:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #2 and #1 are incompatible: \n  1. `queries[2].sort[0]`, `batman-[uuid].rankingRules[0]`: descending sort rule(s) on field `id`\n  2. `queries[1].sort[0]`, `movies-[uuid].rankingRules[0]`: ascending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_limit_offset() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;
    let score_index = shared_index_with_score_documents().await;

    {
        let (response, code) = server
            .multi_search(json!({"federation": {}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              }
            },
            {
              "id": 852,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 2,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              }
            }
          ],
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 0,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"limit": 1}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              }
            }
          ],
          "processingTimeMs": "[duration]",
          "limit": 1,
          "offset": 0,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 2}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
        {
          "hits": [
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              }
            }
          ],
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 2,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 12}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
        {
          "hits": [],
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 12,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }
}

#[actix_rt::test]
async fn federation_formatting() {
    let server = Server::new_shared();
    let index = shared_index_with_documents().await;
    let nested_index = shared_index_with_nested_documents().await;
    let score_index = shared_index_with_score_documents().await;

    {
        let (response, code) = server
            .multi_search(json!({"federation": {}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              },
              "_formatted": {
                "title": "<em>Gläss</em>"
              }
            },
            {
              "id": 852,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 2,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              },
              "_formatted": {
                "title": "<em>Batman</em>"
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              },
              "_formatted": {
                "title": "<em>Batman</em> <em>Returns</em>"
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Captain</em> Marvel"
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Escape</em> Room"
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Batman</em> the dark knight returns: Part 1"
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Batman</em> the dark knight returns: Part 2"
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              },
              "_formatted": {
                "title": "<em>Badman</em>"
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              },
              "_formatted": {
                "title": "How to Train Your Dragon: <em>The</em> Hidden World"
              }
            }
          ],
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 0,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"limit": 1}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              }
            }
          ],
          "processingTimeMs": "[duration]",
          "limit": 1,
          "offset": 0,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 2}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
        {
          "hits": [
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "SHARED_NESTED_DOCUMENTS",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "SHARED_SCORE_DOCUMENTS",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "SHARED_DOCUMENTS",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              }
            }
          ],
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 2,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 12}, "queries": [
            {"indexUid" : index.uid, "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : index.uid, "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid" : nested_index.uid, "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid" : score_index.uid, "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid" : index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid" : score_index.uid, "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
        {
          "hits": [],
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 12,
          "estimatedTotalHits": 12,
          "requestUid": "[uuid]"
        }
        "###);
    }
}

#[actix_rt::test]
async fn federation_invalid_weight() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red", "filter": "BOOST = true", "showRankingScore": true, "federationOptions": {"weight": 3.0}},
        {"indexUid": index.uid, "q": "apple red", "showRankingScore": true, "federationOptions": {"weight": -12}},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.queries[1].federationOptions.weight`: the value of `weight` is invalid, expected a positive float (>= 0.0).",
      "code": "invalid_multi_search_weight",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_weight"
    }
    "###);
}

#[actix_rt::test]
async fn federation_null_weight() {
    let server = Server::new_shared();

    let index = server.unique_index_with_prefix("fruits");

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red", "filter": "BOOST = true", "showRankingScore": true, "federationOptions": {"weight": 3.0}},
        {"indexUid": index.uid, "q": "apple red", "showRankingScore": true, "federationOptions": {"weight": 0.0} },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "Exclusive sale: Red delicious apple",
          "id": "red-delicious-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 2.7281746031746033
          },
          "_rankingScore": 0.9093915343915344
        },
        {
          "name": "Exclusive sale: green apple",
          "id": "green-apple-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.318181818181818
          },
          "_rankingScore": 0.4393939393939394
        },
        {
          "name": "Red apple gala",
          "id": "red-apple-gala",
          "_federation": {
            "indexUid": "fruits-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.0
          },
          "_rankingScore": 0.953042328042328
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_federated_contains_pagination() {
    let server = Server::new_shared();

    let index = server.unique_index_with_prefix("fruits");

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    // fail when a federated query contains "limit"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red"},
        {"indexUid": index.uid, "q": "apple red", "limit": 5},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `limit` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
    // fail when a federated query contains "offset"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red"},
        {"indexUid": index.uid, "q": "apple red", "offset": 5},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `offset` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
    // fail when a federated query contains "page"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red"},
        {"indexUid": index.uid, "q": "apple red", "page": 2},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `page` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
    // fail when a federated query contains "hitsPerPage"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red"},
        {"indexUid": index.uid, "q": "apple red", "hitsPerPage": 5},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `hitsPerPage` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
}

#[actix_rt::test]
async fn federation_federated_contains_facets() {
    let server = Server::new_shared();

    let index = server.unique_index_with_prefix("fruits");

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;

    server.wait_task(value.uid()).await.succeeded();

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    // empty facets are actually OK
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid, "q": "apple red"},
        {"indexUid": index.uid, "q": "apple red", "facets": []},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "Red apple gala",
          "id": "red-apple-gala",
          "_federation": {
            "indexUid": "fruits-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.953042328042328
          }
        },
        {
          "name": "Exclusive sale: Red delicious apple",
          "id": "red-delicious-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9093915343915344
          }
        },
        {
          "name": "Exclusive sale: green apple",
          "id": "green-apple-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.4393939393939394
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);

    // fails
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid": index.uid, "q": "apple red"},
        {"indexUid": index.uid, "q": "apple red", "facets": ["BOOSTED"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Using facet options is not allowed in federated queries.\n - Hint: remove `facets` from query #1 or remove `federation` from the request\n - Hint: pass `federation.facetsByIndex.fruits-[uuid]: [\"BOOSTED\"]` for facets in federated search",
      "code": "invalid_multi_search_query_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_facets"
    }
    "###);
}

#[actix_rt::test]
async fn federation_non_faceted_for_an_index() {
    let server = Server::new_shared();

    let fruits_index = server.unique_index_with_prefix("fruits");

    let (value, _) = fruits_index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST", "id", "name"]}),
        )
        .await;

    server.wait_task(value.uid()).await.succeeded();

    let fruits_no_name_index = server.unique_index_with_prefix("fruits-no-name");

    let (value, _) = fruits_no_name_index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST", "id"]}),
        )
        .await;

    server.wait_task(value.uid()).await.succeeded();

    let fruits_no_facets_index = server.unique_index_with_prefix("fruits-no-facets");

    let (value, _) =
        fruits_no_facets_index.update_settings(json!({"searchableAttributes": ["name"]})).await;

    server.wait_task(value.uid()).await.succeeded();

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = fruits_no_facets_index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    // fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            fruits_index.uid.clone(): ["BOOST", "id", "name"],
            fruits_no_name_index.uid.clone(): ["BOOST", "id", "name"],
          }
        }, "queries": [
        {"indexUid" : fruits_index.uid.clone(), "q": "apple red"},
        {"indexUid": fruits_no_name_index.uid.clone(), "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.federation.facetsByIndex.fruits-no-name-[uuid]`: Invalid facet distribution: Attribute `name` is not filterable. Available filterable attributes patterns are: `BOOST, id`.\n - Note: index `fruits-no-name-[uuid]` used in `.queries[1]`",
      "code": "invalid_multi_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facets"
    }
    "###);

    // still fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            fruits_index.uid.clone(): ["BOOST", "id", "name"],
            fruits_no_name_index.uid.clone(): ["BOOST", "id", "name"],
          }
        }, "queries": [
        {"indexUid" : fruits_index.uid.clone(), "q": "apple red"},
        {"indexUid": fruits_index.uid.clone(), "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.federation.facetsByIndex.fruits-no-name-[uuid]`: Invalid facet distribution: Attribute `name` is not filterable. Available filterable attributes patterns are: `BOOST, id`.\n - Note: index `fruits-no-name-[uuid]` is not used in queries",
      "code": "invalid_multi_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facets"
    }
    "###);

    // fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            fruits_index.uid.clone(): ["BOOST", "id", "name"],
            fruits_no_name_index.uid.clone(): ["BOOST", "id"],
            fruits_no_facets_index.uid.clone(): ["BOOST", "id"],
          }
        }, "queries": [
        {"indexUid" : fruits_index.uid.clone(), "q": "apple red"},
        {"indexUid": fruits_index.uid.clone(), "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Inside `.federation.facetsByIndex.fruits-no-facets-[uuid]`: Invalid facet distribution: Attributes `BOOST, id` are not filterable. This index does not have configured filterable attributes.\n - Note: index `fruits-no-facets-[uuid]` is not used in queries",
      "code": "invalid_multi_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facets"
    }
    "#);

    // also fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "zorglub": ["BOOST", "id", "name"],
            fruits_index.uid.clone(): ["BOOST", "id", "name"],
          }
        }, "queries": [
        {"indexUid" : fruits_index.uid.clone(), "q": "apple red"},
        {"indexUid": fruits_index.uid.clone(), "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Inside `.federation.facetsByIndex.zorglub`: Index `zorglub` not found.\n - Note: index `zorglub` is not used in queries",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn federation_non_federated_contains_federation_option() {
    let server = Server::new_shared();

    let index = server.unique_index();

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    // fail when a non-federated query contains "federationOptions"
    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : index.uid.clone(), "q": "apple red"},
        {"indexUid": index.uid.clone(), "q": "apple red", "federationOptions": {}},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Inside `.queries[1]`: Using `federationOptions` is not allowed in a non-federated search.\n - Hint: remove `federationOptions` from query #1 or add `federation` to the request.",
      "code": "invalid_multi_search_federation_options",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_federation_options"
    }
    "###);
}

#[actix_rt::test]
async fn federation_vector_single_index() {
    let server = Server::new_shared();

    let index = server.unique_index();

    let (value, _) = index
        .update_settings(json!({"embedders": {
          "animal": {
            "source": "userProvided",
            "dimensions": 3
          },
          "sentiment": {
            "source": "userProvided",
            "dimensions": 2
          }
        }}))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let documents = VECTOR_DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(value.uid()).await.succeeded();

    // same embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid.clone(), "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}},
        {"indexUid": index.uid.clone(), "vector": [0.5, 0.5, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9870882034301758
          }
        },
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9728479385375975
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9191691875457764
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 4,
      "requestUid": "[uuid]"
    }
    "###);

    // distinct embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : index.uid.clone(), "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}},
        // joyful and energetic first
        {"indexUid": index.uid.clone(), "vector": [0.8, 0.6], "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"}},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.979868710041046
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          }
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8432406187057495
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 4,
      "requestUid": "[uuid]"
    }
    "###);

    // hybrid search, distinct embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : index.uid, "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}, "showRankingScore": true},
          // joyful and energetic first
          {"indexUid": index.uid, "vector": [0.8, 0.6], "q": "beagle", "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"},"showRankingScore": true},
          {"indexUid": index.uid, "q": "dog", "showRankingScore": true},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.979868710041046
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9242424242424242
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          },
          "_rankingScore": "[score]"
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_vector_two_indexes() {
    let server = Server::new_shared();

    let vectors_animal_index = server.unique_index_with_prefix("vectors-animal");

    let (value, _) = vectors_animal_index
        .update_settings(json!({"embedders": {
          "animal": {
            "source": "userProvided",
            "dimensions": 3
          },
        }}))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let documents = VECTOR_DOCUMENTS.clone();
    let (value, code) = vectors_animal_index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(value.uid()).await.succeeded();

    let vectors_sentiment_index = server.unique_index_with_prefix("vectors-sentiment");

    let (value, _) = vectors_sentiment_index
        .update_settings(json!({"embedders": {
          "sentiment": {
            "source": "userProvided",
            "dimensions": 2
          }
        }}))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let documents = VECTOR_DOCUMENTS.clone();
    let (value, code) = vectors_sentiment_index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : vectors_animal_index.uid, "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}, "retrieveVectors": true},
        // joyful and energetic first
        {"indexUid": vectors_sentiment_index.uid, "vector": [0.8, 0.6], "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"}, "retrieveVectors": true},
        {"indexUid": vectors_sentiment_index.uid, "q": "dog", "retrieveVectors": true},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "animal": [
              0.8,
              0.09,
              0.8
            ],
            "sentiment": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.30000001192092896
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.979868710041046
          }
        },
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "sentiment": [
              0.8,
              0.3
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.09000000357627869,
                  0.800000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9728479385375975
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "sentiment": [
              -1.0,
              0.1
            ],
            "animal": {
              "embeddings": [
                [
                  0.8500000238418579,
                  0.019999999552965164,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "animal": [
              0.9,
              0.8,
              0.05
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.10000000149011612,
                  0.550000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9242424242424242
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "animal": [
              0.85,
              0.02,
              0.1
            ],
            "sentiment": {
              "embeddings": [
                [
                  -1.0,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9242424242424242
          }
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "sentiment": [
              -0.2,
              0.65
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.8999999761581421,
                  0.5
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "sentiment": [
              -0.1,
              0.55
            ],
            "animal": {
              "embeddings": [
                [
                  0.8999999761581421,
                  0.800000011920929,
                  0.05000000074505806
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8432406187057495
          }
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "animal": [
              0.8,
              0.9,
              0.5
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.20000000298023224,
                  0.6499999761581421
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.6690993905067444
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 8,
      "queryVectors": {
        "0": [
          1.0,
          0.0,
          0.5
        ],
        "1": [
          0.8,
          0.6
        ]
      },
      "semanticHitCount": 6,
      "requestUid": "[uuid]"
    }
    "###);

    // hybrid search, distinct embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : vectors_animal_index.uid, "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}, "showRankingScore": true, "retrieveVectors": true},
          {"indexUid": vectors_sentiment_index.uid, "vector": [-1, 0.6], "q": "beagle", "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"}, "showRankingScore": true, "retrieveVectors": true,},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]" }), @r#"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "sentiment": [
              0.8,
              0.3
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.09000000357627869,
                  0.800000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9728479385375975
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "sentiment": [
              -1.0,
              0.1
            ],
            "animal": {
              "embeddings": [
                [
                  0.8500000238418579,
                  0.019999999552965164,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "animal": [
              0.85,
              0.02,
              0.1
            ],
            "sentiment": {
              "embeddings": [
                [
                  -1.0,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9522157907485962
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "animal": [
              0.8,
              0.9,
              0.5
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.20000000298023224,
                  0.6499999761581421
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8719604015350342
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "sentiment": [
              -0.2,
              0.65
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.8999999761581421,
                  0.5
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "sentiment": [
              -0.1,
              0.55
            ],
            "animal": {
              "embeddings": [
                [
                  0.8999999761581421,
                  0.800000011920929,
                  0.05000000074505806
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8432406187057495
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "animal": [
              0.9,
              0.8,
              0.05
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.10000000149011612,
                  0.550000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8297949433326721
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "animal": [
              0.8,
              0.09,
              0.8
            ],
            "sentiment": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.30000001192092896
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.18887794017791748
          },
          "_rankingScore": "[score]"
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 8,
      "queryVectors": {
        "0": [
          1.0,
          0.0,
          0.5
        ],
        "1": [
          -1.0,
          0.6
        ]
      },
      "semanticHitCount": 8,
      "requestUid": "[uuid]"
    }
    "#);
}

#[actix_rt::test]
async fn federation_facets_different_indexes_same_facet() {
    let server = Server::new_shared();
    let movies_index = shared_movies_index().await;
    let batman_index = shared_batman_index().await;

    let batman_2_index = server.unique_index_with_prefix("batman_2");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = batman_2_index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = batman_2_index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    // return titles ordered across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            movies_index.uid.clone(): ["title", "color"],
            batman_index.uid.clone(): ["title"],
            batman_2_index.uid.clone(): ["title"],
          }
        }, "queries": [
          {"indexUid" : movies_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : batman_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : batman_2_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetsByIndex": {
        "batman-[uuid]": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "batman_2-[uuid]": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "movies-[uuid]": {
          "distribution": {
            "color": {
              "blue": 3,
              "green": 2,
              "red": 3,
              "yellow": 2
            },
            "title": {
              "Captain Marvel": 1,
              "Escape Room": 1,
              "Gläss": 1,
              "How to Train Your Dragon: The Hidden World": 1,
              "Shazam!": 1
            }
          },
          "stats": {}
        }
      },
      "requestUid": "[uuid]"
    }
    "###);

    let (response, code) = server
    .multi_search(json!({"federation": {
      "facetsByIndex": {
        movies_index.uid.clone(): ["title"],
        batman_index.uid.clone(): ["title"],
        batman_2_index.uid.clone(): ["title"]
      },
      "mergeFacets": {}
    }, "queries": [
      {"indexUid" : movies_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
      {"indexUid" : batman_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
      {"indexUid" : batman_2_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
    ]}))
    .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetDistribution": {
        "title": {
          "Badman": 2,
          "Batman": 2,
          "Batman Returns": 2,
          "Batman the dark knight returns: Part 1": 2,
          "Batman the dark knight returns: Part 2": 2,
          "Captain Marvel": 1,
          "Escape Room": 1,
          "Gläss": 1,
          "How to Train Your Dragon: The Hidden World": 1,
          "Shazam!": 1
        }
      },
      "facetStats": {},
      "requestUid": "[uuid]"
    }
    "###);

    // mix and match query: will be sorted across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            movies_index.uid.clone(): [],
            batman_index.uid.clone(): ["title"],
            batman_2_index.uid.clone(): ["title"]
          }
        }, "queries": [
          {"indexUid" : batman_index.uid.clone(), "q": "badman returns", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : batman_2_index.uid.clone(), "q": "badman returns", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : movies_index.uid.clone(), "q": "captain", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : batman_index.uid.clone(), "q": "the bat", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 3,
            "weightedRankingScore": 0.9528218694885362
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 3,
            "weightedRankingScore": 0.9528218694885362
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8317901234567902
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.23106060606060605
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.5
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.5
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 11,
      "facetsByIndex": {
        "batman-[uuid]": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "batman_2-[uuid]": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "movies-[uuid]": {
          "distribution": {},
          "stats": {}
        }
      },
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_facets_same_indexes() {
    let server = Server::new_shared();

    let doggos_index = server.unique_index_with_prefix("doggos");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = doggos_index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = doggos_index
        .update_settings(json!({
          "filterableAttributes": ["father", "mother", "doggos.age"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let doggos2_index = server.unique_index_with_prefix("doggos_2");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = doggos2_index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = doggos2_index
        .update_settings(json!({
          "filterableAttributes": ["father", "mother", "doggos.age"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            doggos_index.uid.clone(): ["father", "mother", "doggos.age"]
          }
        }, "queries": [
          {"indexUid" : doggos_index.uid.clone(), "q": "je", "attributesToRetrieve": ["id"] },
          {"indexUid" : doggos_index.uid.clone(), "q": "michel", "attributesToRetrieve": ["id"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 951,
          "_federation": {
            "indexUid": "doggos-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 750,
          "_federation": {
            "indexUid": "doggos-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "facetsByIndex": {
        "doggos-[uuid]": {
          "distribution": {
            "doggos.age": {
              "2": 1,
              "4": 1,
              "5": 1,
              "6": 1
            },
            "father": {
              "jean": 1,
              "jean-baptiste": 1,
              "romain": 1
            },
            "mother": {
              "michelle": 2,
              "sophie": 1
            }
          },
          "stats": {
            "doggos.age": {
              "min": 2.0,
              "max": 6.0
            }
          }
        }
      },
      "requestUid": "[uuid]"
    }
    "###);

    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            doggos_index.uid.clone(): ["father", "mother", "doggos.age"],
            doggos2_index.uid.clone(): ["father", "mother", "doggos.age"]
          }
        }, "queries": [
          {"indexUid" : doggos_index.uid.clone(), "q": "je", "attributesToRetrieve": ["id"] },
          {"indexUid" : doggos2_index.uid.clone(), "q": "michel", "attributesToRetrieve": ["id"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 951,
          "_federation": {
            "indexUid": "doggos-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 750,
          "_federation": {
            "indexUid": "doggos_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "facetsByIndex": {
        "doggos-[uuid]": {
          "distribution": {
            "doggos.age": {
              "2": 1,
              "4": 1,
              "5": 1,
              "6": 1
            },
            "father": {
              "jean": 1,
              "jean-baptiste": 1
            },
            "mother": {
              "michelle": 1,
              "sophie": 1
            }
          },
          "stats": {
            "doggos.age": {
              "min": 2.0,
              "max": 6.0
            }
          }
        },
        "doggos_2-[uuid]": {
          "distribution": {
            "doggos.age": {
              "2": 1,
              "4": 1
            },
            "father": {
              "jean": 1,
              "romain": 1
            },
            "mother": {
              "michelle": 2
            }
          },
          "stats": {
            "doggos.age": {
              "min": 2.0,
              "max": 4.0
            }
          }
        }
      },
      "requestUid": "[uuid]"
    }
    "###);

    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            doggos_index.uid.clone(): ["father", "mother", "doggos.age"],
            doggos2_index.uid.clone(): ["father", "mother", "doggos.age"]
          },
          "mergeFacets": {},
        }, "queries": [
          {"indexUid" : doggos_index.uid.clone(), "q": "je", "attributesToRetrieve": ["id"] },
          {"indexUid" : doggos2_index.uid.clone(), "q": "michel", "attributesToRetrieve": ["id"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 951,
          "_federation": {
            "indexUid": "doggos-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 750,
          "_federation": {
            "indexUid": "doggos_2-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "facetDistribution": {
        "doggos.age": {
          "2": 2,
          "4": 2,
          "5": 1,
          "6": 1
        },
        "father": {
          "jean": 2,
          "jean-baptiste": 1,
          "romain": 1
        },
        "mother": {
          "michelle": 3,
          "sophie": 1
        }
      },
      "facetStats": {
        "doggos.age": {
          "min": 2.0,
          "max": 6.0
        }
      },
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn federation_inconsistent_merge_order() {
    let server = Server::new_shared();

    let movies_index = shared_movies_index().await;

    let movies2_index = server.unique_index_with_prefix("movies_2");

    let documents = DOCUMENTS.clone();
    let (value, _) = movies2_index.add_documents(documents, None).await;
    server.wait_task(value.uid()).await.succeeded();

    let (value, _) = movies2_index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title", "color"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ],
          "faceting": {
            "sortFacetValuesBy": { "color": "count" }
          }
        }))
        .await;
    server.wait_task(value.uid()).await.succeeded();

    let batman_index = shared_batman_index().await;

    // without merging, it works
    let (response, code) = server
      .multi_search(json!({"federation": {
        "facetsByIndex": {
          movies_index.uid.clone(): ["title", "color"],
          batman_index.uid.clone(): ["title"],
          movies2_index.uid.clone(): ["title", "color"],
        }
      }, "queries": [
        {"indexUid" : movies_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
        {"indexUid" : batman_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
        {"indexUid" : movies2_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
      ]}))
      .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetsByIndex": {
        "batman-[uuid]": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "movies-[uuid]": {
          "distribution": {
            "color": {
              "blue": 3,
              "green": 2,
              "red": 3,
              "yellow": 2
            },
            "title": {
              "Captain Marvel": 1,
              "Escape Room": 1,
              "Gläss": 1,
              "How to Train Your Dragon: The Hidden World": 1,
              "Shazam!": 1
            }
          },
          "stats": {}
        },
        "movies_2-[uuid]": {
          "distribution": {
            "color": {
              "red": 3,
              "blue": 3,
              "yellow": 2,
              "green": 2
            },
            "title": {
              "Captain Marvel": 1,
              "Escape Room": 1,
              "Gläss": 1,
              "How to Train Your Dragon: The Hidden World": 1,
              "Shazam!": 1
            }
          },
          "stats": {}
        }
      },
      "requestUid": "[uuid]"
    }
    "###);

    // fails with merging
    let (response, code) = server
  .multi_search(json!({"federation": {
    "facetsByIndex": {
      movies_index.uid.clone(): ["title", "color"],
      batman_index.uid.clone(): ["title"],
      movies2_index.uid.clone(): ["title", "color"],
    },
    "mergeFacets": {}
  }, "queries": [
    {"indexUid" : movies_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
    {"indexUid" : batman_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
    {"indexUid" : movies2_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
  ]}))
  .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.federation.facetsByIndex.movies_2-[uuid]`: Inconsistent order for values in facet `color`: index `movies-[uuid]` orders alphabetically, but index `movies_2-[uuid]` orders by count.\n - Hint: Remove `federation.mergeFacets` or change `faceting.sortFacetValuesBy` to be consistent in settings.\n - Note: index `movies_2-[uuid]` used in `.queries[2]`",
      "code": "invalid_multi_search_facet_order",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facet_order"
    }
    "###);

    // can limit the number of values
    let (response, code) = server
 .multi_search(json!({"federation": {
   "facetsByIndex": {
     movies_index.uid.clone(): ["title", "color"],
     batman_index.uid.clone(): ["title"],
     movies2_index.uid.clone(): ["title"],
   },
   "mergeFacets": {
     "maxValuesPerFacet": 3,
   }
 }, "queries": [
   {"indexUid" : movies_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
   {"indexUid" : batman_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
   {"indexUid" : movies2_index.uid.clone(), "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
 ]}))
 .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-[uuid]",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies-[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies_2-[uuid]",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetDistribution": {
        "color": {
          "blue": 3,
          "green": 2,
          "red": 3
        },
        "title": {
          "Badman": 1,
          "Batman": 1,
          "Batman Returns": 1
        }
      },
      "facetStats": {},
      "requestUid": "[uuid]"
    }
    "###);
}
