use meili_snap::{json_string, snapshot};

use crate::common::Server;
use crate::json;

#[actix_web::test]
async fn routes_are_disabled_by_default() {
    let server = Server::new().await;

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value), @r#"
    {
      "message": "Using the `/dynamic-search-rules` routes requires enabling the `dynamic search rules` experimental feature. See https://github.com/orgs/meilisearch/discussions/884",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "#);
}

async fn dynamic_search_rules_server() -> Server {
    let server = Server::new().await;
    let (value, code) = server.set_features(json!({ "dynamicSearchRules": true })).await;
    assert_eq!(code, 200, "{value}");
    assert_eq!(value["dynamicSearchRules"], json!(true));
    server
}

async fn create_simple_dynamic_search_rule(server: &Server, uid: &str, active: bool, doc_id: &str) {
    let (value, code) = server
        .create_dynamic_search_rule(
            uid,
            json!({
                "active": active,
                "actions": [
                    {
                        "selector": { "id": doc_id },
                        "action": { "type": "pin", "position": 0 }
                    }
                ]
            }),
        )
        .await;
    assert_eq!(code, 201, "{value}");
}

#[actix_web::test]
async fn list_empty() {
    let server = dynamic_search_rules_server().await;

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r#"
    {
      "results": [],
      "offset": 0,
      "limit": 20,
      "total": 0
    }
    "#);
}

#[actix_web::test]
async fn list_supports_pagination() {
    let server = dynamic_search_rules_server().await;

    create_simple_dynamic_search_rule(&server, "rule-a", false, "0").await;
    create_simple_dynamic_search_rule(&server, "rule-b", true, "1").await;
    create_simple_dynamic_search_rule(&server, "rule-c", false, "2").await;

    let (value, code) = server
        .list_dynamic_search_rules_with(json!({
            "offset": 1,
            "limit": 1
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r#"
    {
      "results": [
        {
          "uid": "rule-b",
          "active": true,
          "conditions": [],
          "actions": [
            {
              "selector": {
                "id": "1"
              },
              "action": {
                "type": "pin",
                "position": 0
              }
            }
          ]
        }
      ],
      "offset": 1,
      "limit": 1,
      "total": 3
    }
    "#);
}

#[actix_web::test]
async fn list_filters_by_attribute_patterns() {
    let server = dynamic_search_rules_server().await;

    create_simple_dynamic_search_rule(&server, "promo-active", true, "1").await;
    create_simple_dynamic_search_rule(&server, "promo-inactive", false, "2").await;
    create_simple_dynamic_search_rule(&server, "standard-active", true, "3").await;

    let (value, code) = server
        .list_dynamic_search_rules_with(json!({
            "filter": {
                "attributePatterns": ["promo*"]
            }
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r#"
    {
      "results": [
        {
          "uid": "promo-active",
          "active": true,
          "conditions": [],
          "actions": [
            {
              "selector": {
                "id": "1"
              },
              "action": {
                "type": "pin",
                "position": 0
              }
            }
          ]
        },
        {
          "uid": "promo-inactive",
          "active": false,
          "conditions": [],
          "actions": [
            {
              "selector": {
                "id": "2"
              },
              "action": {
                "type": "pin",
                "position": 0
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "#);
}

#[actix_web::test]
async fn list_filters_by_active_and_combines_filters() {
    let server = dynamic_search_rules_server().await;

    create_simple_dynamic_search_rule(&server, "promo-active", true, "1").await;
    create_simple_dynamic_search_rule(&server, "promo-inactive", false, "2").await;
    create_simple_dynamic_search_rule(&server, "standard-active", true, "3").await;

    let (value, code) = server
        .list_dynamic_search_rules_with(json!({
            "filter": {
                "active": true
            }
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r#"
    {
      "results": [
        {
          "uid": "promo-active",
          "active": true,
          "conditions": [],
          "actions": [
            {
              "selector": {
                "id": "1"
              },
              "action": {
                "type": "pin",
                "position": 0
              }
            }
          ]
        },
        {
          "uid": "standard-active",
          "active": true,
          "conditions": [],
          "actions": [
            {
              "selector": {
                "id": "3"
              },
              "action": {
                "type": "pin",
                "position": 0
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "#);

    let (value, code) = server
        .list_dynamic_search_rules_with(json!({
            "filter": {
                "attributePatterns": ["promo*"],
                "active": true
            }
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r#"
    {
      "results": [
        {
          "uid": "promo-active",
          "active": true,
          "conditions": [],
          "actions": [
            {
              "selector": {
                "id": "1"
              },
              "action": {
                "type": "pin",
                "position": 0
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "#);
}

#[actix_web::test]
async fn create_and_get() {
    let server = dynamic_search_rules_server().await;

    let (value, code) = server
        .create_dynamic_search_rule(
            "rule-1",
            json!({
                "actions": [
                    {
                        "selector": { "id": "42" },
                        "action": { "type": "pin", "position": 1 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value), name: "create_rule_1");

    let (value, code) = server.get_dynamic_search_rule("rule-1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "get_rule_1");
}

#[actix_web::test]
async fn create_full_rule() {
    let server = dynamic_search_rules_server().await;

    let (value, code) = server
        .create_dynamic_search_rule("black-friday", json!({
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
                    "selector": { "indexUid": "products", "id": "456" },
                    "action": { "type": "pin", "position": 0 }
                },
                {
                    "selector": { "id": "789" },
                    "action": { "type": "pin", "position": 3 }
                },
                {
                    "selector": { "id": "999" },
                    "action": { "type": "pin", "position": 8 }
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
async fn create_rejects_query_condition_with_both_is_empty_and_contains() {
    let server = dynamic_search_rules_server().await;

    let (value, code) = server
        .create_dynamic_search_rule(
            "invalid-query-condition",
            json!({
                "conditions": [
                    { "scope": "query", "isEmpty": false, "contains": "batman" }
                ],
                "actions": [
                    {
                        "selector": { "id": "42" },
                        "action": { "type": "pin", "position": 0 }
                    }
                ]
            }),
        )
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value), @r#"
    {
      "message": "Invalid value at `.conditions[0]`: either `isEmpty` or `contains` can be used, not all at once",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "#);
}

#[actix_web::test]
async fn full_lifecycle() {
    let server = dynamic_search_rules_server().await;

    // Create two rules
    let (_, code) = server
        .create_dynamic_search_rule("rule-a", json!({
            "actions": [{ "selector": { "id": "0" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (_, code) = server
        .create_dynamic_search_rule("rule-b", json!({
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
      "results": [],
      "offset": 0,
      "limit": 20,
      "total": 0
    }
    "#);
}

#[actix_web::test]
async fn patch_rule() {
    let server = dynamic_search_rules_server().await;

    let (_, code) = server
        .create_dynamic_search_rule("updatable", json!({
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
            json!({ "actions": [{ "selector": { "id": "99" }, "action": { "type": "pin", "position": 7 } }] }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "actions");
}

#[actix_web::test]
async fn get_not_found() {
    let server = dynamic_search_rules_server().await;

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
    let server = dynamic_search_rules_server().await;

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
    let server = dynamic_search_rules_server().await;

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
    let server = dynamic_search_rules_server().await;

    let (_, code) = server
        .create_dynamic_search_rule("dup", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = server
        .create_dynamic_search_rule("dup", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
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
    let server = dynamic_search_rules_server().await;

    let (value, code) = server
        .create_dynamic_search_rule("rule-x", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }],
            "unknownField": true
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn patch_unknown_field() {
    let server = dynamic_search_rules_server().await;

    let (_, code) = server
        .create_dynamic_search_rule("rule-y", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
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
    let server = dynamic_search_rules_server().await;

    let (value, code) = server.create_dynamic_search_rule("no-actions", json!({})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn create_empty_body() {
    let server = dynamic_search_rules_server().await;

    let (_, code) = server.create_dynamic_search_rule("empty", json!({})).await;
    snapshot!(code, @"400 Bad Request");
}

#[actix_web::test]
async fn patch_preserves_fields() {
    let server = dynamic_search_rules_server().await;

    let (_, code) = server
        .create_dynamic_search_rule("preserve", json!({
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
    let server = dynamic_search_rules_server().await;

    let (_, code) = server
        .create_dynamic_search_rule(
            "arrays",
            json!({
                "conditions": [{ "scope": "query", "isEmpty": true }],
                "actions": [
                    { "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } },
                    { "selector": { "id": "2" }, "action": { "type": "pin", "position": 2 } }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) =
        server.patch_dynamic_search_rule("arrays", json!({ "conditions": [] })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "clear_conditions");

    let (value, code) = server
        .patch_dynamic_search_rule(
            "arrays",
            json!({ "actions": [{ "selector": { "id": "3" }, "action": { "type": "pin", "position": 4 } }] }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "replace_actions");
}

#[actix_web::test]
async fn patch_empty_body() {
    let server = dynamic_search_rules_server().await;

    let (original, code) = server
        .create_dynamic_search_rule("no-change", json!({
            "active": true,
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = server.patch_dynamic_search_rule("no-change", json!({})).await;
    snapshot!(code, @"200 OK");
    assert_eq!(value, original);
}

#[actix_web::test]
async fn defaults_on_create() {
    let server = dynamic_search_rules_server().await;

    let (value, code) = server
        .create_dynamic_search_rule("minimal", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn disabling_the_feature_stops_applying_rules_to_search() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "local", "title": "Batman Returns" },
                { "id": "remote", "title": "Batman" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (_, code) = server
        .create_dynamic_search_rule(
            "pin-remote",
            json!({
                "active": true,
                "actions": [
                    {
                        "selector": { "id": "remote" },
                        "action": { "type": "pin", "position": 0 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = index.search_post(json!({ "q": "batman returns" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), name: "results_when_dsr_enabled");

    let (value, code) = server.set_features(json!({ "dynamicSearchRules": false })).await;
    snapshot!(code, @"200 OK");
    snapshot!(value["dynamicSearchRules"], @"false");

    let (value, code) = index.search_post(json!({ "q": "batman returns" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), name: "results_when_dsr_disabled");
}

#[actix_web::test]
async fn search_applies_pins_when_query_contains_value() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "local", "title": "Batman Returns" },
                { "id": "remote", "title": "Batman" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (_, code) = server
        .create_dynamic_search_rule(
            "pin-when-query-contains-returns",
            json!({
                "active": true,
                "conditions": [
                    { "scope": "query", "contains": "returns" }
                ],
                "actions": [
                    {
                        "selector": { "id": "remote" },
                        "action": { "type": "pin", "position": 0 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = index.search_post(json!({ "q": "Batman Returns" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r#"
    [
      {
        "id": "remote",
        "title": "Batman"
      },
      {
        "id": "local",
        "title": "Batman Returns"
      }
    ]
    "#);
}

#[actix_web::test]
async fn search_filters_out_pinned_documents_excluded_by_filters() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) = index.update_settings(json!({ "filterableAttributes": ["kind"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-1", "kind": "keep" },
                { "id": "filtered-pin", "kind": "drop" },
                { "id": "organic-2", "kind": "keep" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (_, code) = server
        .create_dynamic_search_rule(
            "pin-filtered",
            json!({
                "active": true,
                "actions": [
                    {
                        "selector": { "id": "filtered-pin" },
                        "action": { "type": "pin", "position": 0 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = index.search_post(json!({ "filter": "kind = keep", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]));
}

#[actix_web::test]
async fn search_keeps_pins_that_miss_query_but_not_filters() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index.update_settings(json!({ "filterableAttributes": ["kind"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-match", "title": "Batman Returns", "kind": "keep" },
                { "id": "pinned-query-miss", "title": "The Matrix", "kind": "keep" },
                { "id": "filtered-pin", "title": "Batman Returns", "kind": "drop" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (_, code) = server
        .create_dynamic_search_rule(
            "pin-query-miss-but-filtered",
            json!({
                "active": true,
                "conditions": [
                    { "scope": "query", "contains": "returns" }
                ],
                "actions": [
                    {
                        "selector": { "id": "pinned-query-miss" },
                        "action": { "type": "pin", "position": 0 }
                    },
                    {
                        "selector": { "id": "filtered-pin" },
                        "action": { "type": "pin", "position": 1 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) =
        index.search_post(json!({ "q": "Batman Returns", "filter": "kind = keep" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r#"
    [
      {
        "id": "pinned-query-miss",
        "title": "The Matrix",
        "kind": "keep"
      },
      {
        "id": "organic-match",
        "title": "Batman Returns",
        "kind": "keep"
      }
    ]
    "#);
}

#[actix_web::test]
async fn search_counts_pins_that_miss_query() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index.update_settings(json!({ "filterableAttributes": ["kind"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-match", "title": "Batman Returns", "kind": "keep" },
                { "id": "pinned-query-miss", "title": "The Matrix", "kind": "keep" },
                { "id": "filtered-pin", "title": "Batman Returns", "kind": "drop" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (_, code) = server
        .create_dynamic_search_rule(
            "pin-query-miss-but-filtered",
            json!({
                "active": true,
                "conditions": [
                    { "scope": "query", "contains": "returns" }
                ],
                "actions": [
                    {
                        "selector": { "id": "pinned-query-miss" },
                        "action": { "type": "pin", "position": 0 }
                    },
                    {
                        "selector": { "id": "filtered-pin" },
                        "action": { "type": "pin", "position": 1 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) =
        index.search_post(json!({ "q": "Batman Returns", "filter": "kind = keep" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r#"
    {
      "hits": [
        {
          "id": "pinned-query-miss",
          "title": "The Matrix",
          "kind": "keep"
        },
        {
          "id": "organic-match",
          "title": "Batman Returns",
          "kind": "keep"
        }
      ],
      "query": "Batman Returns",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]"
    }
    "#);

    let (value, code) = index
        .search_post(json!({
            "q": "Batman Returns",
            "filter": "kind = keep",
            "page": 2,
            "hitsPerPage": 1
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r#"
    {
      "hits": [
        {
          "id": "organic-match",
          "title": "Batman Returns",
          "kind": "keep"
        }
      ],
      "query": "Batman Returns",
      "processingTimeMs": "[duration]",
      "hitsPerPage": 1,
      "page": 2,
      "totalPages": 2,
      "totalHits": 2,
      "requestUid": "[uuid]"
    }
    "#);
}

#[actix_web::test]
async fn search_pumps_pins_when_organic_results_run_out() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-1" },
                { "id": "late-pin-1" },
                { "id": "organic-2" },
                { "id": "late-pin-2" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (_, code) = server
        .create_dynamic_search_rule(
            "pump-pins",
            json!({
                "active": true,
                "actions": [
                    {
                        "selector": { "id": "late-pin-1" },
                        "action": { "type": "pin", "position": 10 }
                    },
                    {
                        "selector": { "id": "late-pin-2" },
                        "action": { "type": "pin", "position": 20 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"201 Created");

    let (value, code) = index.search_post(json!({ "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), name: "limit_10");

    let (value, code) = index.search_post(json!({ "offset": 2, "limit": 2 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), name: "offset_2_limit_2");
}
