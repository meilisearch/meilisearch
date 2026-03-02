//! Tests for the document join (hydration) feature.
//!
//! Each test enables the `foreignKeys` experimental feature, performs a search with
//! `attributesToHighlight` (so that both the document and `_formatted` contain the foreign key
//! field), asserts that hydration occurs (foreign key value is the full document). Then the
//! feature is disabled and the same search is run again to assert no hydration (foreign key
//! remains the raw document id).

use meili_snap::{json_string, snapshot};

use crate::common::index::Index;
use crate::common::{Server, Value};
use crate::json;

/// Documents for the "authors" (foreign) index.
fn authors_documents() -> Value {
    json!([
        { "id": "a1", "name": "Alice" },
        { "id": "a2", "name": "Bob" }
    ])
}

/// Documents for the "books" (main) index: each has a foreign key `author_id` → authors.
fn books_documents() -> Value {
    json!([
        { "id": "b1", "title": "Rust in action", "author_id": "a1" },
        { "id": "b2", "title": "Captain Marvel story", "author_id": "a2" }
    ])
}

/// Set up authors and books indexes with foreign key from books.author_id → authors.
/// Requires `foreignKeys` experimental feature to be enabled.
/// Returns (authors_index, books_index) so callers can use the index uids or the Index for search.
async fn setup_indexes_with_foreign_key(server: &Server) -> (Index<'_>, Index<'_>) {
    let authors_index = server.unique_index();
    let books_index = server.unique_index();

    let (task, code) = authors_index.create(Some("id")).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = authors_index.add_documents(authors_documents(), None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = books_index.create(Some("id")).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = books_index
        .update_settings(json!({
            "foreignKeys": [
                { "foreignIndexUid": authors_index.uid, "fieldName": "author_id" }
            ]
        }))
        .await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = books_index.add_documents(books_documents(), None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    (authors_index, books_index)
}

#[actix_rt::test]
async fn search_hydration_with_attributes_to_highlight() {
    let server = Server::new().await;
    server.set_features(json!({ "foreignKeys": true })).await;

    let (_authors_index, books_index) = setup_indexes_with_foreign_key(&server).await;

    let search_params = json!({
        "q": "Rust",
        "attributesToRetrieve": ["title", "author_id"],
        "attributesToHighlight": ["title"]
    });

    // With feature enabled: author_id should be hydrated (full object)
    let (response, code) = books_index.search_post(search_params.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]" }), @r###"
    [
      {
        "title": "Rust in action",
        "author_id": {
          "id": "a1",
          "name": "Alice"
        },
        "_formatted": {
          "title": "<em>Rust</em> in action",
          "author_id": {
            "id": "a1",
            "name": "Alice"
          }
        }
      }
    ]
    "###);

    // Disable feature: no hydration, author_id stays as raw id
    server.set_features(json!({ "foreignKeys": false })).await;

    let (response, code) = books_index.search_post(search_params).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]" }), @r###"
    [
      {
        "title": "Rust in action",
        "author_id": "a1",
        "_formatted": {
          "title": "<em>Rust</em> in action",
          "author_id": "a1"
        }
      }
    ]
    "###);
}

#[actix_rt::test]
async fn multi_search_hydration_with_attributes_to_highlight() {
    let server = Server::new().await;
    server.set_features(json!({ "foreignKeys": true })).await;

    let (_authors_index, books_index) = setup_indexes_with_foreign_key(&server).await;

    let multi_params = json!({
        "queries": [
            {
                "indexUid": books_index.uid,
                "q": "Rust",
                "attributesToRetrieve": ["title", "author_id"],
                "attributesToHighlight": ["title"]
            }
        ]
    });

    // With feature enabled: hydration in multi-search results
    let (response, code) = server.multi_search(multi_params.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["results"], { ".**.processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]", ".**.indexUid" => "[index]" }), @r###"
    [
      {
        "indexUid": "[index]",
        "hits": [
          {
            "title": "Rust in action",
            "author_id": {
              "id": "a1",
              "name": "Alice"
            },
            "_formatted": {
              "title": "<em>Rust</em> in action",
              "author_id": {
                "id": "a1",
                "name": "Alice"
              }
            }
          }
        ],
        "query": "Rust",
        "processingTimeMs": "[duration]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1,
        "requestUid": "[uuid]"
      }
    ]
    "###);

    // Disable feature: no hydration
    server.set_features(json!({ "foreignKeys": false })).await;

    let (response, code) = server.multi_search(multi_params).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["results"], { ".**.processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]", ".**.indexUid" => "[index]" }), @r###"
    [
      {
        "indexUid": "[index]",
        "hits": [
          {
            "title": "Rust in action",
            "author_id": "a1",
            "_formatted": {
              "title": "<em>Rust</em> in action",
              "author_id": "a1"
            }
          }
        ],
        "query": "Rust",
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
async fn federated_search_hydration_with_attributes_to_highlight() {
    let server = Server::new().await;
    server.set_features(json!({ "foreignKeys": true })).await;

    let (_authors_index, books_index) = setup_indexes_with_foreign_key(&server).await;

    let federated_params = json!({
        "federation": {},
        "queries": [
            {
                "indexUid": books_index.uid,
                "q": "Captain",
                "attributesToRetrieve": ["title", "author_id"],
                "attributesToHighlight": ["title"]
            }
        ]
    });

    // With feature enabled: hydration in federated (multi-search with federation) results
    let (response, code) = server.multi_search(federated_params.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]", ".**._federation" => "[federation]" }), @r###"
    [
      {
        "title": "Captain Marvel story",
        "author_id": {
          "id": "a2",
          "name": "Bob"
        },
        "_federation": "[federation]",
        "_formatted": {
          "title": "<em>Captain</em> Marvel story",
          "author_id": {
            "id": "a2",
            "name": "Bob"
          }
        }
      }
    ]
    "###);

    // Disable feature: no hydration
    server.set_features(json!({ "foreignKeys": false })).await;

    let (response, code) = server.multi_search(federated_params).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]", ".**._federation" => "[federation]" }), @r###"
    [
      {
        "title": "Captain Marvel story",
        "author_id": "a2",
        "_federation": "[federation]",
        "_formatted": {
          "title": "<em>Captain</em> Marvel story",
          "author_id": "a2"
        }
      }
    ]
    "###);
}
