use meili_snap::{json_string, snapshot};

use crate::common::Server;
use crate::json;

#[actix_web::test]
async fn list_empty() {
    let server = Server::new().await;

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r#"
    {
      "results": []
    }
    "#);
}

#[actix_web::test]
async fn create_and_get() {
    let server = Server::new().await;

    let (value, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "rule-1",
            "actions": [
                {
                    "selector": { "id": "42" },
                    "action": { "type": "pin", "position": 1 }
                }
            ]
        }))
        .await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value), name: "create_rule_1");

    let (value, code) = server.get_dynamic_search_rule("rule-1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "get_rule_1");
}

#[actix_web::test]
async fn create_full_rule() {
    let server = Server::new().await;

    let (value, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "black-friday",
            "description": "Black Friday 2025 rules",
            "priority": 10,
            "active": true,
            "conditions": [
                { "scope": "query", "isEmpty": true },
                { "scope": "time", "start": "2025-11-28T00:00:00Z", "end": "2025-11-28T23:59:59Z" }
            ],
            "actions": [
                {
                    "selector": { "indexUid": "products", "id": "123" },
                    "action": { "type": "pin", "position": 1 }
                },
                {
                    "selector": { "filter": { "brand": "premium" } },
                    "action": { "type": "boost", "score": 2.0 }
                },
                {
                    "selector": {},
                    "action": { "type": "bury", "score": 0.3 }
                },
                {
                    "selector": { "id": "456" },
                    "action": { "type": "hide" }
                }
            ]
        }))
        .await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value));

    let (get_value, code) = server.get_dynamic_search_rule("black-friday").await;
    snapshot!(code, @"200 OK");
    assert_eq!(value, get_value);
}

#[actix_web::test]
async fn full_lifecycle() {
    let server = Server::new().await;

    // Create two rules
    let (_, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "rule-a",
            "actions": [{ "selector": {}, "action": { "type": "hide" } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (_, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "rule-b",
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "list_rules");

    // Delete rule-a
    let (_, code) = server.delete_dynamic_search_rule("rule-a").await;
    snapshot!(code, @"204 No Content");

    // List shows 1
    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "list_rules_after_delete_rule_a");

    // Get deleted returns 404
    let (_, code) = server.get_dynamic_search_rule("rule-a").await;
    snapshot!(code, @"404 Not Found");

    // Get remaining still works
    let (_, code) = server.get_dynamic_search_rule("rule-b").await;
    snapshot!(code, @"200 OK");

    // Delete rule-b
    let (_, code) = server.delete_dynamic_search_rule("rule-b").await;
    snapshot!(code, @"204 No Content");

    // List empty
    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r#"
    {
      "results": []
    }
    "#);
}

#[actix_web::test]
async fn patch_rule() {
    let server = Server::new().await;

    let (_, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "updatable",
            "actions": [{ "selector": { "id": "42" }, "action": { "type": "pin", "position": 1 } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = server
        .patch_dynamic_search_rule("updatable", json!({ "description": "Updated", "priority": 10 }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "priority");

    let (value, code) =
        server.patch_dynamic_search_rule("updatable", json!({ "active": true })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "active");

    let (value, code) = server
        .patch_dynamic_search_rule(
            "updatable",
            json!({ "conditions": [{ "scope": "query", "isEmpty": true }] }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "conditions");

    let (value, code) = server
        .patch_dynamic_search_rule(
            "updatable",
            json!({ "actions": [{ "selector": { "id": "99" }, "action": { "type": "hide" } }] }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "actions");
}

#[actix_web::test]
async fn get_not_found() {
    let server = Server::new().await;

    let (value, code) = server.get_dynamic_search_rule("no-such-rule").await;
    snapshot!(code, @"404 Not Found");
    snapshot!(json_string!(value), @r#"
    {
      "message": "Dynamic search rule `no-such-rule` not found.",
      "code": "dynamic_search_rule_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
    }
    "#);
}

#[actix_web::test]
async fn patch_not_found() {
    let server = Server::new().await;

    let (value, code) = server.patch_dynamic_search_rule("ghost", json!({ "active": true })).await;
    snapshot!(code, @"404 Not Found");
    snapshot!(json_string!(value), @r#"
    {
      "message": "Dynamic search rule `ghost` not found.",
      "code": "dynamic_search_rule_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
    }
    "#);
}

#[actix_web::test]
async fn delete_not_found() {
    let server = Server::new().await;

    let (value, code) = server.delete_dynamic_search_rule("phantom").await;
    snapshot!(code, @"404 Not Found");
    snapshot!(json_string!(value), @r#"
    {
      "message": "Dynamic search rule `phantom` not found.",
      "code": "dynamic_search_rule_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
    }
    "#);
}

#[actix_web::test]
async fn create_duplicate() {
    let server = Server::new().await;

    let (_, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "dup",
            "actions": [{ "selector": {}, "action": { "type": "hide" } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "dup",
            "actions": [{ "selector": {}, "action": { "type": "hide" } }]
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value), @r#"
    {
      "message": "Dynamic search rule `dup` already exists.",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "#);
}

#[actix_web::test]
async fn create_unknown_field() {
    let server = Server::new().await;

    let (value, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "rule-x",
            "actions": [{ "selector": {}, "action": { "type": "hide" } }],
            "unknownField": true
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn patch_unknown_field() {
    let server = Server::new().await;

    let (_, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "rule-y",
            "actions": [{ "selector": {}, "action": { "type": "hide" } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) =
        server.patch_dynamic_search_rule("rule-y", json!({ "bogusField": 42 })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn create_missing_actions() {
    let server = Server::new().await;

    let (value, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "no-actions"
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn create_missing_uid() {
    let server = Server::new().await;

    let (value, code) = server
        .create_dynamic_search_rule(json!({
            "actions": [{ "selector": {}, "action": { "type": "hide" } }]
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn create_empty_body() {
    let server = Server::new().await;

    let (_, code) = server.create_dynamic_search_rule(json!({})).await;
    snapshot!(code, @"400 Bad Request");
}

#[actix_web::test]
async fn patch_preserves_fields() {
    let server = Server::new().await;

    let (_, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "preserve",
            "description": "original",
            "priority": 5,
            "active": true,
            "conditions": [{ "scope": "query", "isEmpty": true }],
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) =
        server.patch_dynamic_search_rule("preserve", json!({ "description": "updated" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn patch_replaces_arrays() {
    let server = Server::new().await;

    let (_, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "arrays",
            "conditions": [{ "scope": "query", "isEmpty": true }],
            "actions": [
                { "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } },
                { "selector": { "id": "2" }, "action": { "type": "hide" } }
            ]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) =
        server.patch_dynamic_search_rule("arrays", json!({ "conditions": [] })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "clear_conditions");

    let (value, code) = server
        .patch_dynamic_search_rule(
            "arrays",
            json!({ "actions": [{ "selector": {}, "action": { "type": "hide" } }] }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "replace_actions");
}

#[actix_web::test]
async fn patch_empty_body() {
    let server = Server::new().await;

    let (original, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "no-change",
            "active": true,
            "actions": [{ "selector": {}, "action": { "type": "hide" } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = server.patch_dynamic_search_rule("no-change", json!({})).await;
    snapshot!(code, @"200 OK");
    assert_eq!(value, original);
}

#[actix_web::test]
async fn defaults_on_create() {
    let server = Server::new().await;

    let (value, code) = server
        .create_dynamic_search_rule(json!({
            "uid": "minimal",
            "actions": [{ "selector": {}, "action": { "type": "hide" } }]
        }))
        .await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value));
}
