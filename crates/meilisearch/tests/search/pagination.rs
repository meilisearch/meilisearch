use super::shared_index_with_documents;
use crate::common::Server;
use crate::json;
use meili_snap::{json_string, snapshot};

#[actix_rt::test]
async fn default_search_should_return_estimated_total_hit() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert!(response.get("estimatedTotalHits").is_some());
            assert!(response.get("limit").is_some());
            assert!(response.get("offset").is_some());

            // these fields shouldn't be present
            assert!(response.get("totalHits").is_none());
            assert!(response.get("page").is_none());
            assert!(response.get("totalPages").is_none());
        })
        .await;
}

#[actix_rt::test]
async fn simple_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"page": 1}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            assert!(response.get("totalHits").is_some());
            assert_eq!(response["page"], 1);
            assert_eq!(response["totalPages"], 1);

            // these fields shouldn't be present
            assert!(response.get("estimatedTotalHits").is_none());
            assert!(response.get("limit").is_none());
            assert!(response.get("offset").is_none());
        })
        .await;
}

#[actix_rt::test]
async fn page_zero_should_not_return_any_result() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"page": 0}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 0);
            assert!(response.get("totalHits").is_some());
            assert_eq!(response["page"], 0);
            assert_eq!(response["totalPages"], 1);
        })
        .await;
}

#[actix_rt::test]
async fn hits_per_page_1() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"hitsPerPage": 1}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            assert_eq!(response["totalHits"], 5);
            assert_eq!(response["page"], 1);
            assert_eq!(response["totalPages"], 5);
        })
        .await;
}

#[actix_rt::test]
async fn hits_per_page_0_should_not_return_any_result() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"hitsPerPage": 0}), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 0);
            assert_eq!(response["totalHits"], 5);
            assert_eq!(response["page"], 1);
            assert_eq!(response["totalPages"], 0);
        })
        .await;
}

#[actix_rt::test]
async fn ensure_placeholder_search_hit_count_valid() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
        {
            "title": "Shazam!",
            "id": "287947",
            "distinct": 1,
        },
        {
            "title": "Captain Marvel",
            "id": "299537",
            "distinct": 4,
        },
        {
            "title": "Escape Room",
            "id": "522681",
            "distinct": 2,
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "id": "166428",
            "distinct": 3,
        },
        {
            "title": "Glass",
            "id": "450465",
            "distinct": 3,
        }
    ]);
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, _code) = index
        .update_settings(
            json!({ "rankingRules": ["distinct:asc"], "distinctAttribute": "distinct"}),
        )
        .await;
    server.wait_task(response.uid()).await.succeeded();

    for page in 0..=4 {
        index
            .search(json!({"page": page, "hitsPerPage": 1}), |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["totalHits"], 4);
                assert_eq!(response["totalPages"], 4);
            })
            .await;
    }
}

#[actix_rt::test]
async fn test_issue_5274() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
        {
            "id": 1,
            "title": "Document 1",
            "content": "This is the first."
        },
        {
            "id": 2,
            "title": "Document 2",
            "content": "This is the second doc."
        }
    ]);
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    // Find out the lowest ranking score among the documents
    let (rep, _status) = index
        .search_post(json!({"q": "doc", "page": 1, "hitsPerPage": 2, "showRankingScore": true}))
        .await;
    let hits = rep["hits"].as_array().expect("Missing hits array");
    let second_hit = hits.get(1).expect("Missing second hit");
    let ranking_score = second_hit
        .get("_rankingScore")
        .expect("Missing _rankingScore field")
        .as_f64()
        .expect("Expected _rankingScore to be a f64");

    // Search with a ranking score threshold just above and expect to be a single hit
    let (rep, _status) = index
        .search_post(json!({"q": "doc", "page": 1, "hitsPerPage": 1, "rankingScoreThreshold": ranking_score + 0.0001}))
        .await;

    snapshot!(json_string!(rep, {
        ".processingTimeMs" => "[ignored]",
        ".requestUid" => "[uuid]"
    }), @r###"
    {
      "hits": [
        {
          "id": 2,
          "title": "Document 2",
          "content": "This is the second doc."
        }
      ],
      "query": "doc",
      "processingTimeMs": "[ignored]",
      "hitsPerPage": 1,
      "page": 1,
      "totalPages": 1,
      "totalHits": 1,
      "requestUid": "[uuid]"
    }
    "###);
}
