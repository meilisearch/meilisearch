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
      "message": "Inside `.queries[0]`: Using `.showPerformanceDetails` is not allowed in federated queries.\n - Hint: remove `showPerformanceDetails` from the query or remove `federation` from the request",
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
      "code": "invalid_search_show_performance_details",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_show_performance_details"
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

#[actix_rt::test]
async fn federated_search_multiple_indexes_query_steps() {
    let server = Server::new_shared();
    let index_a = server.unique_index();
    let index_b = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index_a.add_documents(documents.clone(), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, _code) = index_b.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Interleave queries across indexes so that global and per-index positions differ
    let (response, code) = server
        .multi_search(json!({
            "federation": { "showPerformanceDetails": true },
            "queries": [
                {"indexUid": index_a.uid, "q": "glass"},
                {"indexUid": index_b.uid, "q": "glass"},
                {"indexUid": index_a.uid, "q": "dragon"}
            ]
        }))
        .await;

    snapshot!(code, @"200 OK");
    // Each query must get its own step, named after its position in the federated request,
    // so that the durations of queries targeting different indexes are not merged together.
    // Keys are sorted because indexes are processed in an order that depends on their (random) uid.
    let mut query_steps: Vec<&str> = response["performanceDetails"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .filter(|step| step.contains("query["))
        .collect();
    query_steps.sort_unstable();
    snapshot!(json_string!(query_steps), @r###"
    [
      "process > execute local > query[0]",
      "process > execute local > query[0] > evaluate query",
      "process > execute local > query[0] > keyword ranking",
      "process > execute local > query[0] > tokenize query",
      "process > execute local > query[1]",
      "process > execute local > query[1] > evaluate query",
      "process > execute local > query[1] > keyword ranking",
      "process > execute local > query[1] > tokenize query",
      "process > execute local > query[2]",
      "process > execute local > query[2] > evaluate query",
      "process > execute local > query[2] > keyword ranking",
      "process > execute local > query[2] > tokenize query"
    ]
    "###);
}
