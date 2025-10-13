use meili_snap::{json_string, snapshot};

use crate::common::{shared_index_with_documents, Server, DOCUMENTS};
use crate::json;

#[actix_rt::test]
async fn search_without_metadata_header() {
    let index = shared_index_with_documents().await;

    // Test that metadata is not included by default
    index
        .search(json!({"q": "glass"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
            {
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
            }
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn search_with_metadata_header() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    // Test with Meili-Include-Metadata header
    let (response, code) = index
        .search_with_headers(json!({"q": "glass"}), vec![("Meili-Include-Metadata", "true")])
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]", ".metadata.queryUid" => "[uuid]" }), @r###"
    {
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
      "requestUid": "[uuid]",
      "metadata": {
        "queryUid": "[uuid]",
        "indexUid": "[uuid]",
        "primaryKey": "id"
      }
    }
    "###);
}

#[actix_rt::test]
async fn search_with_metadata_header_and_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Test with Meili-Include-Metadata header
    let (response, code) = index
        .search_with_headers(json!({"q": "glass"}), vec![("Meili-Include-Metadata", "true")])
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]", ".metadata.queryUid" => "[uuid]" }), @r###"
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
      "metadata": {
        "queryUid": "[uuid]",
        "indexUid": "[uuid]",
        "primaryKey": "id"
      }
    }
    "###);
}

#[actix_rt::test]
async fn multi_search_without_metadata_header() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    // Test multi-search without metadata header
    let (response, code) = server
        .multi_search(json!({
            "queries": [
                {"indexUid": index.uid, "q": "glass"},
                {"indexUid": index.uid, "q": "dragon"}
            ]
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".results[0].processingTimeMs" => "[duration]", ".results[0].requestUid" => "[uuid]", ".results[1].processingTimeMs" => "[duration]", ".results[1].requestUid" => "[uuid]" }), @r###"
    {
      "results": [
        {
          "indexUid": "[uuid]",
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
          "indexUid": "[uuid]",
          "hits": [
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "id": "166428",
              "color": [
                "green",
                "red"
              ]
            }
          ],
          "query": "dragon",
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 0,
          "estimatedTotalHits": 1,
          "requestUid": "[uuid]"
        }
      ]
    }
    "###);
}

#[actix_rt::test]
async fn multi_search_with_metadata_header() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Test multi-search with metadata header
    let (response, code) = server
        .multi_search_with_headers(
            json!({
                "queries": [
                    {"indexUid": index.uid, "q": "glass"},
                    {"indexUid": index.uid, "q": "dragon"}
                ]
            }),
            vec![("Meili-Include-Metadata", "true")],
        )
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".results[0].processingTimeMs" => "[duration]", ".results[0].requestUid" => "[uuid]", ".results[0].metadata.queryUid" => "[uuid]", ".results[1].processingTimeMs" => "[duration]", ".results[1].requestUid" => "[uuid]", ".results[1].metadata.queryUid" => "[uuid]" }), @r###"
    {
      "results": [
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
          "metadata": {
            "queryUid": "[uuid]",
            "indexUid": "[uuid]",
            "primaryKey": "id"
          }
        },
        {
          "indexUid": "[uuid]",
          "hits": [
            {
              "id": "166428",
              "title": "How to Train Your Dragon: The Hidden World",
              "color": [
                "green",
                "red"
              ]
            }
          ],
          "query": "dragon",
          "processingTimeMs": "[duration]",
          "limit": 20,
          "offset": 0,
          "estimatedTotalHits": 1,
          "requestUid": "[uuid]",
          "metadata": {
            "queryUid": "[uuid]",
            "indexUid": "[uuid]",
            "primaryKey": "id"
          }
        }
      ]
    }
    "###);
}

#[actix_rt::test]
async fn search_metadata_header_false_value() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    // Test with header set to false
    let (response, code) = index
        .search_with_headers(json!({"q": "glass"}), vec![("Meili-Include-Metadata", "false")])
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
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
    }
    "###);
}

#[actix_rt::test]
async fn search_metadata_uuid_format() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .search_with_headers(json!({"q": "glass"}), vec![("Meili-Include-Metadata", "true")])
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]", ".metadata.queryUid" => "[uuid]" }), @r###"
    {
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
      "requestUid": "[uuid]",
      "metadata": {
        "queryUid": "[uuid]",
        "indexUid": "[uuid]",
        "primaryKey": "id"
      }
    }
    "###);
}

#[actix_rt::test]
async fn search_metadata_consistency_across_requests() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _code) = index.add_documents(documents, Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Make multiple requests and check that metadata is consistent
    for _i in 0..3 {
        let (response, code) = index
            .search_with_headers(json!({"q": "glass"}), vec![("Meili-Include-Metadata", "true")])
            .await;

        snapshot!(code, @"200 OK");
        snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]", ".metadata.queryUid" => "[uuid]" }), @r###"
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
          "metadata": {
            "queryUid": "[uuid]",
            "indexUid": "[uuid]",
            "primaryKey": "id"
          }
        }
        "###);
    }
}
