use meili_snap::{json_string, snapshot};

use crate::common::{Server, DOCUMENTS};
use crate::json;

#[actix_rt::test]
async fn search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Make multiple requests and check that metadata is consistent
    index.search(json!({"q": "glass", "showPerformanceDetails": true}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]", ".performanceDetails" => "[details]"}), @r###"
            {
              "hits": [
                {
                  "id": "450465",
                  "title": "Gläss",
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
              "requestUid": "[uuid]",
              "performanceDetails": "[details]"
            }
            "###);
        }).await;
}

#[actix_rt::test]
async fn multi_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Make federated search request with performance details
    let (response, code) = server
        .multi_search(json!({
            "queries": [
                {"indexUid": index.uid, "q": "glass", "showPerformanceDetails": true}
            ]
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["results"][0], { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]", ".performanceDetails" => "[details]"}), @r###"
    {
      "indexUid": "[uuid]",
      "hits": [
        {
          "id": "450465",
          "title": "Gläss",
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
      "requestUid": "[uuid]",
      "performanceDetails": "[details]"
    }
    "###);
}

#[actix_rt::test]
async fn invalid_federated_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Make invalid federated search request with performance details
    let (response, code) = server
        .multi_search(json!({
            "federation": {},
            "queries": [
                {"indexUid": index.uid, "q": "glass", "showPerformanceDetails": true}
            ]
        }))
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]"}), @r###"
    {
      "message": "Inside `.queries[0]`: Using `.showPerformanceDetails` is not allowed in federated queries.\n - Hint: remove `showPerformanceDetails` from query #0 or remove `federation` from the request",
      "code": "invalid_multi_search_query_show_performance_details",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_show_performance_details"
    }
    "###);

    // Make invalid federated search request with performance details
    let (response, code) = server
        .multi_search(json!({
            "federation": { "showPerformanceDetails": "true"},
            "queries": [
                {"indexUid": index.uid, "q": "glass"}
            ]
        }))
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]"}), @r###"
    {
      "message": "Invalid value type at `.federation.showPerformanceDetails`: expected a boolean, but found a string: `\"true\"`",
      "code": "invalid_multi_search_query_show_performance_details",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_show_performance_details"
    }
    "###);
}

#[actix_rt::test]
async fn federated_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();
    // Make federated search request with performance details
    let (response, code) = server
        .multi_search(json!({
            "federation": { "showPerformanceDetails": true },
            "queries": [
                {"indexUid": index.uid, "q": "glass"}
            ]
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]", ".performanceDetails" => "[details]" }), @r###"
    {
      "hits": [
        {
          "id": "450465",
          "title": "Gläss",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "[uuid]",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1,
      "requestUid": "[uuid]",
      "performanceDetails": "[details]"
    }
    "###);
}
