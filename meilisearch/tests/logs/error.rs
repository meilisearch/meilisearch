use meili_snap::*;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn logs_bad_target() {
    let server = Server::new().await;

    // Wrong type
    let (response, code) = server.service.post("/logs", json!({ "target": true })).await;
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
    let (response, code) = server.service.post("/logs", json!({ "target": [] })).await;
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
    let (response, code) = server.service.post("/logs", json!({ "target": "" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.target`: Empty string is not a valid target. If you want to get no logs use `OFF`. Usage: `info`, `info:meilisearch`, or you can write multiple filters in one target: `index_scheduler=info,milli=trace`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // An error from the target parser
    let (response, code) = server.service.post("/logs", json!({ "target": "==" })).await;
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
async fn logs_bad_mode() {
    let server = Server::new().await;

    // Wrong type
    let (response, code) = server.service.post("/logs", json!({ "mode": true })).await;
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
    let (response, code) = server.service.post("/logs", json!({ "mode": [] })).await;
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
    let (response, code) = server.service.post("/logs", json!({ "mode": "tamo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Unknown value `tamo` at `.mode`: expected one of `fmt`, `profile`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn logs_without_enabling_the_route() {
    let server = Server::new().await;

    let (response, code) = server.service.post("/logs", json!({})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "getting logs through the `/logs` route requires enabling the `logs route` experimental feature. See https://github.com/meilisearch/product/discussions/625",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    let (response, code) = server.service.delete("/logs").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "getting logs through the `/logs` route requires enabling the `logs route` experimental feature. See https://github.com/meilisearch/product/discussions/625",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}
