use meili_snap::{json_string, snapshot};

use super::{DOCUMENTS, NESTED_DOCUMENTS};
use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn search_empty_list() {
    let server = Server::new().await;

    let (response, code) = server.multi_search(json!({"queries": []})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "results": []
    }
    "###);
}

#[actix_rt::test]
async fn search_json_object() {
    let server = Server::new().await;

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
async fn search_json_array() {
    let server = Server::new().await;

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
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "test", "q": "captain"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response["results"], { "[].processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    [
      {
        "indexUid": "test",
        "hits": [
          {
            "title": "Gläss",
            "id": "450465",
            "_vectors": {
              "manual": [
                -100,
                340,
                90
              ]
            }
          }
        ],
        "query": "glass",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1
      },
      {
        "indexUid": "test",
        "hits": [
          {
            "title": "Captain Marvel",
            "id": "299537",
            "_vectors": {
              "manual": [
                1,
                2,
                54
              ]
            }
          }
        ],
        "query": "captain",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1
      }
    ]
    "###);
}

#[actix_rt::test]
async fn simple_search_missing_index_uid() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, @r###"
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
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid": "hé", "q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, @r###"
    {
      "message": "Invalid value at `.queries[0].indexUid`: `hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_two_indexes() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response["results"], { "[].processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    [
      {
        "indexUid": "test",
        "hits": [
          {
            "title": "Gläss",
            "id": "450465",
            "_vectors": {
              "manual": [
                -100,
                340,
                90
              ]
            }
          }
        ],
        "query": "glass",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1
      },
      {
        "indexUid": "nested",
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
            "_vectors": {
              "manual": [
                1,
                2,
                3
              ]
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
            "_vectors": {
              "manual": [
                1,
                2,
                54
              ]
            }
          }
        ],
        "query": "pésti",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 2
      }
    ]
    "###);
}

#[actix_rt::test]
async fn search_one_index_doesnt_exist() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
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
    let server = Server::new().await;

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
async fn search_one_query_error() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass", "facets": ["title"]},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Invalid facet distribution, this index does not have configured filterable attributes.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn search_multiple_query_errors() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass", "facets": ["title"]},
        {"indexUid": "nested", "q": "pésti", "facets": ["doggos"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Invalid facet distribution, this index does not have configured filterable attributes.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}
