use meilisearch::Opt;

use crate::common::{default_settings, Server};
use crate::json;

/// Feature name to test against.
/// This will have to be changed by a different one when that feature is stabilized.
/// All tests that need to set a feature can make use of this constant.
const FEATURE_NAME: &str = "vectorStore";

#[actix_rt::test]
async fn experimental_features() {
    let server = Server::new().await;

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": false,
      "metrics": false
    }
    "###);

    let (response, code) = server.set_features(json!({FEATURE_NAME: true})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "metrics": false
    }
    "###);

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "metrics": false
    }
    "###);

    // sending null does not change the value
    let (response, code) = server.set_features(json!({FEATURE_NAME: null})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "metrics": false
    }
    "###);

    // not sending the field does not change the value
    let (response, code) = server.set_features(json!({})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "metrics": false
    }
    "###);
}

#[actix_rt::test]
async fn errors() {
    let server = Server::new().await;

    // Sending a feature not in the list is an error
    let (response, code) = server.set_features(json!({"NotAFeature": true})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Unknown field `NotAFeature`: expected one of `scoreDetails`, `vectorStore`, `metrics`",
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
      "message": "Invalid value type at `.vectorStore`: expected a boolean, but found a positive integer: `42`",
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
      "message": "Invalid value type at `.vectorStore`: expected a boolean, but found a string: `\"true\"`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

/// Test features which are instance as well as runtime togglable.
/// Only metrics feature is instance and metrics togglable.
#[actix_rt::test]
async fn instance_and_runtime_experimental_features() {
    // Scenario: Keeping metrics enabled at instance level, should enable metrics.
    let temp_dir = tempfile::tempdir().unwrap();
    let enable_metrics_instance =
        Opt { experimental_enable_metrics: true, ..default_settings(temp_dir.path()) };

    let server = Server::new_with_options(enable_metrics_instance).await.unwrap();
    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": false,
      "metrics": true
    }
    "###);

    let (_, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"200 OK");

    // Scenario: Keeping metrics enabled at instance level and disabling at runtime level, should disable metrics.
    let (response, code) = server.set_features(json!({"metrics": false})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": false,
      "metrics": false
    }
    "###);

    let (response, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Getting metrics requires enabling the `metrics` experimental feature. See https://github.com/meilisearch/meilisearch/discussions/3518",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // Scenario: Keeping metrics disabled at instance level and enabling at runtime level, should enable metrics.
    let server = Server::new().await;
    let (response, code) = server.set_features(json!({"metrics": true})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": false,
      "metrics": true
    }
    "###);

    let (_, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"200 OK");
}
