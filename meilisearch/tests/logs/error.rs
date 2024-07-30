use meili_snap::*;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn logs_stream_bad_target() {
    let server = Server::new_shared();

    // Wrong type
    let (response, code) = server.service.post("/logs/stream", json!({ "target": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.target`: expected a string, but found a boolean: `true`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // Wrong type
    let (response, code) = server.service.post("/logs/stream", json!({ "target": [] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.target`: expected a string, but found an array: `[]`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // Our help message
    let (response, code) = server.service.post("/logs/stream", json!({ "target": "" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.target`: Empty string is not a valid target. If you want to get no logs use `OFF`. Usage: `info`, `meilisearch=info`, or you can write multiple filters in one target: `index_scheduler=info,milli=trace`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // An error from the target parser
    let (response, code) = server.service.post("/logs/stream", json!({ "target": "==" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.target`: invalid filter directive: too many '=' in filter directive, expected 0 or 1",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn logs_stream_bad_mode() {
    let server = Server::new_shared();

    // Wrong type
    let (response, code) = server.service.post("/logs/stream", json!({ "mode": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.mode`: expected a string, but found a boolean: `true`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // Wrong type
    let (response, code) = server.service.post("/logs/stream", json!({ "mode": [] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.mode`: expected a string, but found an array: `[]`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // Wrong value
    let (response, code) = server.service.post("/logs/stream", json!({ "mode": "tamo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Unknown value `tamo` at `.mode`: expected one of `human`, `json`, `profile`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn logs_stream_bad_profile_memory() {
    let server = Server::new_shared();

    // Wrong type
    let (response, code) =
        server.service.post("/logs/stream", json!({ "profileMemory": "tamo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.profileMemory`: expected a boolean, but found a string: `\"tamo\"`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // Wrong type
    let (response, code) =
        server.service.post("/logs/stream", json!({ "profileMemory": ["hello", "kefir"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.profileMemory`: expected a boolean, but found an array: `[\"hello\",\"kefir\"]`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // Used with default parameters
    let (response, code) =
        server.service.post("/logs/stream", json!({ "profileMemory": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value: `profile_memory` can only be used while profiling code and is not compatible with the Human mode.",
      "code": "invalid_settings_typo_tolerance",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_typo_tolerance"
    }
    "###);

    // Used with an unsupported mode
    let (response, code) =
        server.service.post("/logs/stream", json!({ "mode": "fmt", "profileMemory": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Unknown value `fmt` at `.mode`: expected one of `human`, `json`, `profile`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn logs_stream_without_enabling_the_route() {
    let server = Server::new_shared();

    let (response, code) = server.service.post("/logs/stream", json!({})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Modifying logs through the `/logs/*` routes requires enabling the `logs route` experimental feature. See https://github.com/orgs/meilisearch/discussions/721",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    let (response, code) = server.service.delete("/logs/stream").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Modifying logs through the `/logs/*` routes requires enabling the `logs route` experimental feature. See https://github.com/orgs/meilisearch/discussions/721",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    let (response, code) = server.service.post("/logs/stderr", json!({})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Modifying logs through the `/logs/*` routes requires enabling the `logs route` experimental feature. See https://github.com/orgs/meilisearch/discussions/721",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}
