use meilisearch::Opt;
use tempfile::TempDir;

use crate::common::{default_settings, Server};
use crate::json;

/// Feature name to test against.
/// This will have to be changed by a different one when that feature is stabilized.
/// All tests that need to set a feature can make use of this constant.
const FEATURE_NAME: &str = "metrics";

#[actix_rt::test]
async fn experimental_features() {
    let server = Server::new().await;

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": false,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false,
      "network": false,
      "getTaskDocumentsRoute": false,
      "compositeEmbedders": false,
      "chatCompletions": false,
      "multimodal": false,
      "vectorStoreSetting": false
    }
    "###);

    let (response, code) = server.set_features(json!({FEATURE_NAME: true})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false,
      "network": false,
      "getTaskDocumentsRoute": false,
      "compositeEmbedders": false,
      "chatCompletions": false,
      "multimodal": false,
      "vectorStoreSetting": false
    }
    "###);

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false,
      "network": false,
      "getTaskDocumentsRoute": false,
      "compositeEmbedders": false,
      "chatCompletions": false,
      "multimodal": false,
      "vectorStoreSetting": false
    }
    "###);

    // sending null does not change the value
    let (response, code) = server.set_features(json!({FEATURE_NAME: null})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false,
      "network": false,
      "getTaskDocumentsRoute": false,
      "compositeEmbedders": false,
      "chatCompletions": false,
      "multimodal": false,
      "vectorStoreSetting": false
    }
    "###);

    // not sending the field does not change the value
    let (response, code) = server.set_features(json!({})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false,
      "network": false,
      "getTaskDocumentsRoute": false,
      "compositeEmbedders": false,
      "chatCompletions": false,
      "multimodal": false,
      "vectorStoreSetting": false
    }
    "###);
}

#[actix_rt::test]
async fn experimental_feature_metrics() {
    // instance flag for metrics enables metrics at startup
    let dir = TempDir::new().unwrap();
    let enable_metrics = Opt { experimental_enable_metrics: true, ..default_settings(dir.path()) };
    let server = Server::new_with_options(enable_metrics).await.unwrap();

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false,
      "network": false,
      "getTaskDocumentsRoute": false,
      "compositeEmbedders": false,
      "chatCompletions": false,
      "multimodal": false,
      "vectorStoreSetting": false
    }
    "###);

    let (response, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"200 OK");

    // metrics are not returned in json format
    // so the test server will return null
    meili_snap::snapshot!(response, @"null");

    // disabling metrics results in invalid request
    let (response, code) = server.set_features(json!({"metrics": false})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response["metrics"], @"false");

    let (response, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Getting metrics requires enabling the `metrics` experimental feature. See https://github.com/meilisearch/product/discussions/625",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // enabling metrics via HTTP results in valid request
    let (response, code) = server.set_features(json!({"metrics": true})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response["metrics"], @"true");

    let (response, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response, @"null");
}

#[actix_rt::test]
async fn errors() {
    let server = Server::new().await;

    // Sending a feature not in the list is an error
    let (response, code) = server.set_features(json!({"NotAFeature": true})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Unknown field `NotAFeature`: expected one of `metrics`, `logsRoute`, `editDocumentsByFunction`, `containsFilter`, `network`, `getTaskDocumentsRoute`, `compositeEmbedders`, `chatCompletions`, `multimodal`, `vectorStoreSetting`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // The type must be a bool, not a number
    let (response, code) = server.set_features(json!({FEATURE_NAME: 42})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.metrics`: expected a boolean, but found a positive integer: `42`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // The type must be a bool, not a string
    let (response, code) = server.set_features(json!({FEATURE_NAME: "true"})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.metrics`: expected a boolean, but found a string: `\"true\"`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn search_with_personalization_without_enabling_the_feature() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Create the index and add some documents
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _code) = index
        .add_documents(
            json!([
                {"id": 1, "title": "The Dark Knight", "genre": "Action"},
                {"id": 2, "title": "Inception", "genre": "Sci-Fi"},
                {"id": 3, "title": "The Matrix", "genre": "Sci-Fi"}
            ]),
            None,
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Try to search with personalization - should return feature_not_enabled error
    let (response, code) = index
        .search_post(json!({
            "q": "movie",
            "personalize": {
                "userContext": "I love science fiction movies"
            }
        }))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "reranking search results requires enabling the `personalization` experimental feature. See https://github.com/orgs/meilisearch/discussions/866",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}

#[actix_rt::test]
async fn multi_search_with_personalization_without_enabling_the_feature() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Create the index and add some documents
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _code) = index
        .add_documents(
            json!([
                {"id": 1, "title": "The Dark Knight", "genre": "Action"},
                {"id": 2, "title": "Inception", "genre": "Sci-Fi"},
                {"id": 3, "title": "The Matrix", "genre": "Sci-Fi"}
            ]),
            None,
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Try to multi-search with personalization - should return feature_not_enabled error
    let (response, code) = server
        .multi_search(json!({
            "queries": [
                {
                    "indexUid": index.uid,
                    "q": "movie",
                    "personalize": {
                        "userContext": "I love science fiction movies"
                    }
                }
            ]
        }))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: reranking search results requires enabling the `personalization` experimental feature. See https://github.com/orgs/meilisearch/discussions/866",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // Try to federated search with personalization - should return feature_not_enabled error
    let (response, code) = server
        .multi_search(json!({
          "federation": {},
            "queries": [
                {
                    "indexUid": index.uid,
                    "q": "movie",
                    "personalize": {
                        "userContext": "I love science fiction movies"
                    }
                }
            ]
        }))
        .await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Using `.personalize` is not allowed in federated queries.\n - Hint: remove `personalize` from query #0 or remove `federation` from the request",
      "code": "invalid_multi_search_query_personalization",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_personalization"
    }
    "###);
}
