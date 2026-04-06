//! Tests for the document join (hydration) feature.
//!
//! Each test enables the `foreignKeys` experimental feature, performs a search with
//! `attributesToHighlight` (so that both the document and `_formatted` contain the foreign key
//! field), asserts that hydration occurs (foreign key value is the full document). Then the
//! feature is disabled and the same search is run again to assert no hydration (foreign key
//! remains the raw document id).
//!
//! The `_foreign(...)` filter tests combine a local condition on the main index with a nested
//! filter evaluated on the foreign index (via configured `foreignKeys`).

use meili_snap::{json_string, snapshot};

use crate::common::index::Index;
use crate::common::{Server, Value};
use crate::json;

/// Documents for the "authors" (foreign) index.
fn authors_documents() -> Value {
    json!([
        { "id": "a1", "name": "Alice" },
        { "id": "a2", "name": "Bob" },
        { "id": "a3", "name": "Charlie" },
        { "id": "a4", "name": "Diana" },
        { "id": "a5", "name": "Ethan" },
        { "id": "a6", "name": "Fiona" },
        { "id": "a7", "name": "George" },
        { "id": "a8", "name": "Hannah" },
        { "id": "a9", "name": "Isaac" },
        { "id": "a10", "name": "Julia" },
        { "id": "a11", "name": "Kevin" },
        { "id": "a12", "name": "Liam" },
        { "id": "a13", "name": "Mia" },
    ])
}

/// Documents for the "books" (main) index: each has a foreign key `author` → authors.
fn books_documents() -> Value {
    json!([
        { "id": "b1", "title": "Rust in action", "author": "a1", "related_authors": ["a2", "a3"] },
        { "id": "b2", "title": "Captain Marvel story", "author": "a2", "related_authors": ["a3", "a4"] },
        { "id": "b3", "title": "The Great Gatsby", "author": "a3", "related_authors": ["a4", "a5"] },
        { "id": "b4", "title": "To Kill a Mockingbird", "author": "a4", "related_authors": ["a5", "a6"] },
        { "id": "b5", "title": "1984", "author": "a5", "related_authors": ["a6", "a7"] },
        { "id": "b6", "title": "The Catcher in the Rye", "author": "a6", "related_authors": ["a7", "a8"] },
        { "id": "b7", "title": "The Lord of the Rings", "author": "a7", "related_authors": ["a8", "a9"] },
        { "id": "b8", "title": "The Hobbit", "author": "a8", "related_authors": ["a9", "a10"] },
        { "id": "b9", "title": "The Little Prince", "author": "a9", "related_authors": ["a10", "a11"] },
        { "id": "b10", "title": "The Alchemist", "author": "a10", "related_authors": ["a11", "a12"] },
    ])
}

/// Same authors as [`authors_documents`], with `birthday` and `popularity` for `_foreign` filters.
/// Only `a2` matches `birthday STARTS WITH "1958-"` with `popularity >= 3.5`.
fn authors_documents_with_author_profile() -> Value {
    json!([
        { "id": "a1", "name": "Alice", "birthday": "1990-01-01", "popularity": 2.0 },
        { "id": "a2", "name": "Bob", "birthday": "1958-06-15", "popularity": 4.5 },
        { "id": "a3", "name": "Charlie", "birthday": "1940-05-05", "popularity": 4.8 },
        { "id": "a4", "name": "Diana", "birthday": "1985-03-20", "popularity": 3.2 },
        { "id": "a5", "name": "Ethan", "birthday": "1972-11-30", "popularity": 3.9 },
        { "id": "a6", "name": "Fiona", "birthday": "1988-07-14", "popularity": 2.8 },
        { "id": "a7", "name": "George", "birthday": "1961-04-02", "popularity": 3.0 },
        { "id": "a8", "name": "Hannah", "birthday": "1995-09-09", "popularity": 4.1 },
        { "id": "a9", "name": "Isaac", "birthday": "1977-12-25", "popularity": 3.6 },
        { "id": "a10", "name": "Julia", "birthday": "1982-06-01", "popularity": 4.0 },
        { "id": "a11", "name": "Kevin", "birthday": "1999-02-18", "popularity": 2.5 },
        { "id": "a12", "name": "Liam", "birthday": "2001-10-10", "popularity": 3.1 },
        { "id": "a13", "name": "Mia", "birthday": "2003-08-08", "popularity": 2.2 },
    ])
}

/// Same books as [`books_documents`], with a `genres` array for local filters.
/// Several books include `action` so the `_foreign` clause must still narrow results to `b2`.
fn books_documents_with_genres() -> Value {
    json!([
        { "id": "b1", "title": "Rust in action", "author": "a1", "related_authors": ["a2", "a3"], "genres": ["tech", "programming"] },
        { "id": "b2", "title": "Captain Marvel story", "author": "a2", "related_authors": ["a3", "a4"], "genres": ["action", "fiction"] },
        { "id": "b3", "title": "The Great Gatsby", "author": "a3", "related_authors": ["a4", "a5"], "genres": ["action", "classic"] },
        { "id": "b4", "title": "To Kill a Mockingbird", "author": "a4", "related_authors": ["a5", "a6"], "genres": ["classic", "drama"] },
        { "id": "b5", "title": "1984", "author": "a5", "related_authors": ["a6", "a7"], "genres": ["dystopia", "fiction"] },
        { "id": "b6", "title": "The Catcher in the Rye", "author": "a6", "related_authors": ["a7", "a8"], "genres": ["action", "fiction"] },
        { "id": "b7", "title": "The Lord of the Rings", "author": "a7", "related_authors": ["a8", "a9"], "genres": ["fantasy", "adventure"] },
        { "id": "b8", "title": "The Hobbit", "author": "a8", "related_authors": ["a9", "a10"], "genres": ["fantasy"] },
        { "id": "b9", "title": "The Little Prince", "author": "a9", "related_authors": ["a10", "a11"], "genres": ["fiction"] },
        { "id": "b10", "title": "The Alchemist", "author": "a10", "related_authors": ["a11", "a12"], "genres": ["fiction"] },
    ])
}

/// Set up authors and books indexes with foreign key from books.author → authors.
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
                { "foreignIndexUid": authors_index.uid, "fieldName": "author" },
                { "foreignIndexUid": authors_index.uid, "fieldName": "related_authors" }
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

/// Same as [`setup_indexes_with_foreign_key`], but documents include profile/genre fields and both
/// indexes expose the attributes needed for `_foreign` and local filters.
async fn setup_indexes_with_foreign_key_and_filterable_profile(
    server: &Server,
) -> (Index<'_>, Index<'_>) {
    let authors_index = server.unique_index();
    let books_index = server.unique_index();

    let (task, code) = authors_index.create(Some("id")).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) =
        authors_index.add_documents(authors_documents_with_author_profile(), None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = authors_index
        .update_settings(json!({ "filterableAttributes": ["id", "birthday", "popularity"] }))
        .await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = books_index.create(Some("id")).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = books_index
        .update_settings(json!({
            "foreignKeys": [
                { "foreignIndexUid": authors_index.uid, "fieldName": "author" },
                { "foreignIndexUid": authors_index.uid, "fieldName": "related_authors" }
            ],
            "filterableAttributes": ["id", "genres", "author", "related_authors"]
        }))
        .await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = books_index.add_documents(books_documents_with_genres(), None).await;
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
        "attributesToRetrieve": ["title", "author", "related_authors"],
        "attributesToHighlight": ["title"]
    });

    // With feature enabled: author should be hydrated (full object)
    let (response, code) = books_index.search_post(search_params.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]" }), @r###"
    [
      {
        "title": "Rust in action",
        "author": {
          "id": "a1",
          "name": "Alice"
        },
        "related_authors": [
          {
            "id": "a2",
            "name": "Bob"
          },
          {
            "id": "a3",
            "name": "Charlie"
          }
        ],
        "_formatted": {
          "title": "<em>Rust</em> in action",
          "author": {
            "id": "a1",
            "name": "Alice"
          },
          "related_authors": [
            {
              "id": "a2",
              "name": "Bob"
            },
            {
              "id": "a3",
              "name": "Charlie"
            }
          ]
        }
      }
    ]
    "###);

    // Disable feature: no hydration, author stays as raw id
    server.set_features(json!({ "foreignKeys": false })).await;

    let (response, code) = books_index.search_post(search_params).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]" }), @r###"
    [
      {
        "title": "Rust in action",
        "author": "a1",
        "related_authors": [
          "a2",
          "a3"
        ],
        "_formatted": {
          "title": "<em>Rust</em> in action",
          "author": "a1",
          "related_authors": [
            "a2",
            "a3"
          ]
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
                "attributesToRetrieve": ["title", "author", "related_authors"],
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
            "author": {
              "id": "a1",
              "name": "Alice"
            },
            "related_authors": [
              {
                "id": "a2",
                "name": "Bob"
              },
              {
                "id": "a3",
                "name": "Charlie"
              }
            ],
            "_formatted": {
              "title": "<em>Rust</em> in action",
              "author": {
                "id": "a1",
                "name": "Alice"
              },
              "related_authors": [
                {
                  "id": "a2",
                  "name": "Bob"
                },
                {
                  "id": "a3",
                  "name": "Charlie"
                }
              ]
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
            "author": "a1",
            "related_authors": [
              "a2",
              "a3"
            ],
            "_formatted": {
              "title": "<em>Rust</em> in action",
              "author": "a1",
              "related_authors": [
                "a2",
                "a3"
              ]
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
                "attributesToRetrieve": ["title", "author", "related_authors"],
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
        "author": {
          "id": "a2",
          "name": "Bob"
        },
        "related_authors": [
          {
            "id": "a3",
            "name": "Charlie"
          },
          {
            "id": "a4",
            "name": "Diana"
          }
        ],
        "_federation": "[federation]",
        "_formatted": {
          "title": "<em>Captain</em> Marvel story",
          "author": {
            "id": "a2",
            "name": "Bob"
          },
          "related_authors": [
            {
              "id": "a3",
              "name": "Charlie"
            },
            {
              "id": "a4",
              "name": "Diana"
            }
          ]
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
        "author": "a2",
        "related_authors": [
          "a3",
          "a4"
        ],
        "_federation": "[federation]",
        "_formatted": {
          "title": "<em>Captain</em> Marvel story",
          "author": "a2",
          "related_authors": [
            "a3",
            "a4"
          ]
        }
      }
    ]
    "###);
}

#[actix_rt::test]
async fn search_with_foreign_filter_on_author_profile() {
    let server = Server::new().await;
    server.set_features(json!({ "foreignKeys": true })).await;

    let (_authors_index, books_index) =
        setup_indexes_with_foreign_key_and_filterable_profile(&server).await;

    let search_params = json!({
        "q": "",
        "filter": "genres = action AND _foreign(author, birthday STARTS WITH \"1958-\" AND popularity >= 3.5)",
        "attributesToRetrieve": ["title", "author", "related_authors", "genres"]
    });

    let (response, code) = books_index.search_post(search_params.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]" }), @r###"
    [
      {
        "title": "Captain Marvel story",
        "author": {
          "id": "a2",
          "name": "Bob",
          "birthday": "1958-06-15",
          "popularity": 4.5
        },
        "related_authors": [
          {
            "id": "a3",
            "name": "Charlie",
            "birthday": "1940-05-05",
            "popularity": 4.8
          },
          {
            "id": "a4",
            "name": "Diana",
            "birthday": "1985-03-20",
            "popularity": 3.2
          }
        ],
        "genres": [
          "action",
          "fiction"
        ]
      }
    ]
    "###);

    server.set_features(json!({ "foreignKeys": false })).await;

    let (response, code) = books_index.search_post(search_params).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".**.requestUid" => "[uuid]" }), @r###"
    {
      "message": "using a foreign filter requires enabling the `foreign_keys` experimental feature. See https://github.com/orgs/meilisearch/discussions/873\n30:36 _foreign(author, birthday STARTS WITH \"1958-\" AND popularity >= 3.5)",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}

#[actix_rt::test]
async fn federated_search_with_foreign_filter_on_author_profile() {
    let server = Server::new().await;
    server.set_features(json!({ "foreignKeys": true })).await;
    let (_authors_index, books_index) =
        setup_indexes_with_foreign_key_and_filterable_profile(&server).await;
    let federated_params = json!({
        "federation": {},
        "queries": [
            {
                "indexUid": books_index.uid,
                "q": "",
                "filter": "genres = action AND _foreign(author, birthday STARTS WITH \"1958-\" AND popularity >= 3.5)",
                "attributesToRetrieve": ["title", "author", "related_authors", "genres"]
            },
            {
                "indexUid": books_index.uid,
                "q": "",
                "filter": "genres = classic AND (_foreign(author, birthday STARTS WITH \"198\") OR _foreign(related_authors, birthday STARTS WITH \"198\"))",
                "attributesToRetrieve": ["title", "author", "related_authors", "genres"]
            }
        ]
    });
    let (response, code) = server.multi_search(federated_params.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"], { ".**._rankingScore" => "[score]", ".**._federation" => "[federation]" }), @r###"
    [
      {
        "title": "Captain Marvel story",
        "author": {
          "id": "a2",
          "name": "Bob",
          "birthday": "1958-06-15",
          "popularity": 4.5
        },
        "related_authors": [
          {
            "id": "a3",
            "name": "Charlie",
            "birthday": "1940-05-05",
            "popularity": 4.8
          },
          {
            "id": "a4",
            "name": "Diana",
            "birthday": "1985-03-20",
            "popularity": 3.2
          }
        ],
        "genres": [
          "action",
          "fiction"
        ],
        "_federation": "[federation]"
      },
      {
        "title": "The Great Gatsby",
        "author": {
          "id": "a3",
          "name": "Charlie",
          "birthday": "1940-05-05",
          "popularity": 4.8
        },
        "related_authors": [
          {
            "id": "a4",
            "name": "Diana",
            "birthday": "1985-03-20",
            "popularity": 3.2
          },
          {
            "id": "a5",
            "name": "Ethan",
            "birthday": "1972-11-30",
            "popularity": 3.9
          }
        ],
        "genres": [
          "action",
          "classic"
        ],
        "_federation": "[federation]"
      },
      {
        "title": "To Kill a Mockingbird",
        "author": {
          "id": "a4",
          "name": "Diana",
          "birthday": "1985-03-20",
          "popularity": 3.2
        },
        "related_authors": [
          {
            "id": "a5",
            "name": "Ethan",
            "birthday": "1972-11-30",
            "popularity": 3.9
          },
          {
            "id": "a6",
            "name": "Fiona",
            "birthday": "1988-07-14",
            "popularity": 2.8
          }
        ],
        "genres": [
          "classic",
          "drama"
        ],
        "_federation": "[federation]"
      }
    ]
    "###);
    server.set_features(json!({ "foreignKeys": false })).await;
    let (response, code) = server.multi_search(federated_params).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".**.requestUid" => "[uuid]" }), @r###"
    {
      "message": "using a foreign filter requires enabling the `foreign_keys` experimental feature. See https://github.com/orgs/meilisearch/discussions/873\n30:36 _foreign(author, birthday STARTS WITH \"1958-\" AND popularity >= 3.5)",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}

#[actix_rt::test]
async fn multi_search_with_foreign_filter_on_author_profile() {
    let server = Server::new().await;
    server.set_features(json!({ "foreignKeys": true })).await;
    let (_authors_index, books_index) =
        setup_indexes_with_foreign_key_and_filterable_profile(&server).await;
    let multi_params = json!({
        "queries": [
            {
                "indexUid": books_index.uid,
                "q": "",
                "filter": "genres = action AND _foreign(author, birthday STARTS WITH \"1958-\" AND popularity >= 3.5)",
                "attributesToRetrieve": ["title", "author", "related_authors", "genres"]
            }
        ]
    });
    let (response, code) = server.multi_search(multi_params.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["results"], { ".**.processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]", ".**.indexUid" => "[index]" }), @r###"
  [
    {
      "indexUid": "[index]",
      "hits": [
        {
          "title": "Captain Marvel story",
          "author": {
            "id": "a2",
            "name": "Bob",
            "birthday": "1958-06-15",
            "popularity": 4.5
          },
          "related_authors": [
            {
              "id": "a3",
              "name": "Charlie",
              "birthday": "1940-05-05",
              "popularity": 4.8
            },
            {
              "id": "a4",
              "name": "Diana",
              "birthday": "1985-03-20",
              "popularity": 3.2
            }
          ],
          "genres": [
            "action",
            "fiction"
          ]
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1,
      "requestUid": "[uuid]"
    }
  ]
  "###);
    server.set_features(json!({ "foreignKeys": false })).await;
    let (response, code) = server.multi_search(multi_params).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".**.requestUid" => "[uuid]" }), @r###"
    {
      "message": "Inside `.queries[0]`: using a foreign filter requires enabling the `foreign_keys` experimental feature. See https://github.com/orgs/meilisearch/discussions/873\n30:36 _foreign(author, birthday STARTS WITH \"1958-\" AND popularity >= 3.5)",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}
