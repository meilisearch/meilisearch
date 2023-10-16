use crate::common::Server;
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
      "exportPuffinReports": false
    }
    "###);

    let (response, code) = server.set_features(json!({FEATURE_NAME: true})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "exportPuffinReports": false
    }
    "###);

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "exportPuffinReports": false
    }
    "###);

    // sending null does not change the value
    let (response, code) = server.set_features(json!({FEATURE_NAME: null})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "exportPuffinReports": false
    }
    "###);

    // not sending the field does not change the value
    let (response, code) = server.set_features(json!({})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "scoreDetails": false,
      "vectorStore": true,
      "exportPuffinReports": false
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
      "message": "Unknown field `NotAFeature`: expected one of `scoreDetails`, `vectorStore`, `exportPuffinReports`",
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
