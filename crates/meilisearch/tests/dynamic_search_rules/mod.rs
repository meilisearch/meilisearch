use meili_snap::{json_string, snapshot};

use crate::common::Server;
use crate::json;

#[actix_web::test]
async fn routes_are_disabled_by_default() {
    let server = Server::new().await;

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value), @r###"
    {
      "message": "Calling the `POST /dynamic-search-rules` route requires enabling the `dynamic search rules` experimental feature. See https://github.com/orgs/meilisearch/discussions/884",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}

async fn dynamic_search_rules_server() -> Server {
    let server = Server::new().await;
    let (value, code) = server.set_features(json!({ "dynamicSearchRules": true })).await;
    assert_eq!(code, 200, "{value}");
    assert_eq!(value["dynamicSearchRules"], json!(true));
    server
}

async fn create_simple_dynamic_search_rule(server: &Server, uid: &str, active: bool, doc_id: &str) {
    let (task, code) = server
        .create_dynamic_search_rule(
            uid,
            json!({
                "description": uid,
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();
    assert_eq!(code, 202, "{task}");
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
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "rule-b",
          "description": "rule-b",
          "active": true,
          "conditions": {},
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
    "###);
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
                "query": "promo"
            }
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "promo-active",
          "description": "promo-active",
          "active": true,
          "conditions": {},
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
          "description": "promo-inactive",
          "active": false,
          "conditions": {},
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
    "###);
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
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "promo-active",
          "description": "promo-active",
          "active": true,
          "conditions": {},
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
          "description": "standard-active",
          "active": true,
          "conditions": {},
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
    "###);

    let (value, code) = server
        .list_dynamic_search_rules_with(json!({
            "filter": {
                "query": "promo",
                "active": true
            }
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "promo-active",
          "description": "promo-active",
          "active": true,
          "conditions": {},
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
    "###);
}

#[actix_web::test]
async fn create_and_get() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("rule-1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "get_rule_1");
}

#[actix_web::test]
async fn create_full_rule() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule(
            "black-friday",
            json!({
                "description": "Black Friday 2025 rules",
                "precedence": 10,
                "active": true,
                "conditions": {
                    "query": {
                        "isEmpty": true
                    },
                    "time": {
            "start": "2025-11-28T00:00:00Z", "end": "2025-11-28T23:59:59Z"
                    }
                },
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
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (get_value, code) = server.get_dynamic_search_rule("black-friday").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(get_value), @r###"
    {
      "uid": "black-friday",
      "description": "Black Friday 2025 rules",
      "precedence": 10,
      "active": true,
      "conditions": {
        "time": {
          "start": "2025-11-28T00:00:00Z",
          "end": "2025-11-28T23:59:59Z"
        },
        "query": {
          "isEmpty": true
        }
      },
      "actions": [
        {
          "selector": {
            "indexUid": "products",
            "id": "123"
          },
          "action": {
            "type": "pin",
            "position": 1
          }
        },
        {
          "selector": {
            "indexUid": "products",
            "id": "456"
          },
          "action": {
            "type": "pin",
            "position": 0
          }
        },
        {
          "selector": {
            "id": "789"
          },
          "action": {
            "type": "pin",
            "position": 3
          }
        },
        {
          "selector": {
            "id": "999"
          },
          "action": {
            "type": "pin",
            "position": 8
          }
        }
      ]
    }
    "###);
}

#[actix_web::test]
async fn create_rejects_query_condition_with_both_is_empty_and_contains() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule(
            "invalid-query-condition",
            json!({
                "conditions":{
                    "query": {
                        "isEmpty": true,
                        "words": "batman"
                    }
                }
                ,
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
    snapshot!(json_string!(task), @r###"
    {
      "message": "Invalid value at `.conditions.query`: either `isEmpty` or `words` can be used, not both at once",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_web::test]
async fn full_lifecycle() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule("rule-a", json!({
            "actions": [{ "selector": { "id": "0" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule("rule-b", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "list_rules");

    let (task, code) = server.delete_dynamic_search_rule("rule-a").await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "list_rules_after_delete_rule_a");

    let (_, code) = server.get_dynamic_search_rule("rule-a").await;
    snapshot!(code, @"404 Not Found");

    let (_, code) = server.get_dynamic_search_rule("rule-b").await;
    snapshot!(code, @"200 OK");

    let (task, code) = server.delete_dynamic_search_rule("rule-b").await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

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

    let (task, code) = server
        .create_dynamic_search_rule("updatable", json!({
            "actions": [{ "selector": { "id": "42" }, "action": { "type": "pin", "position": 1 } }]
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .patch_dynamic_search_rule(
            "updatable",
            json!({ "description": "Updated", "precedence": 10 }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "precedence": 10,
          "active": true,
          "conditions": {},
          "actions": [
            {
              "selector": {
                "id": "42"
              },
              "action": {
                "type": "pin",
                "position": 1
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
    let (task, code) =
        server.patch_dynamic_search_rule("updatable", json!({ "active": false })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "precedence": 10,
          "active": false,
          "conditions": {},
          "actions": [
            {
              "selector": {
                "id": "42"
              },
              "action": {
                "type": "pin",
                "position": 1
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);

    let (task, code) = server
        .patch_dynamic_search_rule(
            "updatable",
            json!({ "conditions": {
                "query": {
                    "isEmpty": true
                }
            } }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "precedence": 10,
          "active": false,
          "conditions": {
            "query": {
              "isEmpty": true
            }
          },
          "actions": [
            {
              "selector": {
                "id": "42"
              },
              "action": {
                "type": "pin",
                "position": 1
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "precedence": 10,
          "active": false,
          "conditions": {
            "query": {
              "isEmpty": true
            }
          },
          "actions": [
            {
              "selector": {
                "id": "42"
              },
              "action": {
                "type": "pin",
                "position": 1
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
    let (task, code) = server
        .patch_dynamic_search_rule(
            "updatable",
            json!({ "actions": [{ "selector": { "id": "99" }, "action": { "type": "pin", "position": 7 } }] }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "precedence": 10,
          "active": false,
          "conditions": {
            "query": {
              "isEmpty": true
            }
          },
          "actions": [
            {
              "selector": {
                "id": "99"
              },
              "action": {
                "type": "pin",
                "position": 7
              }
            }
          ]
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
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
async fn patch_creates_rule_when_missing() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .patch_dynamic_search_rule(
            "foobar",
            json!({
                "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("foobar").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "uid": "foobar",
      "active": true,
      "conditions": {},
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
    "###);
}

#[actix_web::test]
async fn delete_not_found() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server.delete_dynamic_search_rule("phantom").await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();
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

    let (task, code) = server
        .create_dynamic_search_rule("rule-y", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) =
        server.patch_dynamic_search_rule("rule-y", json!({ "bogusField": 42 })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn create_missing_actions() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server.create_dynamic_search_rule("no-actions", json!({})).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("no-actions").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "uid": "no-actions",
      "active": true,
      "conditions": {},
      "actions": []
    }
    "###);
}

#[actix_web::test]
async fn create_empty_body() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server.create_dynamic_search_rule("empty", json!({})).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();
}

#[actix_web::test]
async fn patch_preserves_fields() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule("preserve", json!({
            "description": "original",
            "precedence": 5,
            "active": true,
            "conditions":
            {
                "query": {
                    "isEmpty": true
                }
            },
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("preserve").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "uid": "preserve",
      "description": "original",
      "precedence": 5,
      "active": true,
      "conditions": {
        "query": {
          "isEmpty": true
        }
      },
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
    "###);

    let (task, code) =
        server.patch_dynamic_search_rule("preserve", json!({ "description": "updated" })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("preserve").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "uid": "preserve",
      "description": "updated",
      "precedence": 5,
      "active": true,
      "conditions": {
        "query": {
          "isEmpty": true
        }
      },
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
    "###);

    snapshot!(json_string!(value));
}

#[actix_web::test]
async fn patch_replaces_arrays() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule(
            "arrays",
            json!({
                "conditions": {},
                "actions": [
                    { "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } },
                    { "selector": { "id": "2" }, "action": { "type": "pin", "position": 2 } }
                ]
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .patch_dynamic_search_rule(
            "arrays",
            json!({ "actions": [{ "selector": { "id": "3" }, "action": { "type": "pin", "position": 4 } }] }),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();
    snapshot!(code, @"202 Accepted");
    let (value, code) = server.get_dynamic_search_rule("arrays").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), name: "replace_actions");
}

#[actix_web::test]
async fn patch_empty_body() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule("no-change", json!({
            "active": true,
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server.patch_dynamic_search_rule("no-change", json!({})).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("no-change").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value), @r###"
    {
      "uid": "no-change",
      "active": true,
      "conditions": {},
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
    "###);
}

#[actix_web::test]
async fn defaults_on_create() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule("minimal", json!({
            "actions": [{ "selector": { "id": "1" }, "action": { "type": "pin", "position": 0 } }]
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();
    let (value, code) = server.get_dynamic_search_rule("minimal").await;
    snapshot!(code, @"200 OK");
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

    let (task, code) = server
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

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

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-when-query-contains-returns",
            json!({
                "active": true,
                "conditions": {
                    "query": {
                        "words": "returns"
                    }
                },
                "actions": [
                    {
                        "selector": { "id": "remote" },
                        "action": { "type": "pin", "position": 0 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

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

    let (task, code) = server
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

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

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-query-miss-but-filtered",
            json!({
                "active": true,
                "conditions": {
                    "query": {
                        "words": "returns"
                    }
                },
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

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
async fn search_keeps_hybrid_pins_that_miss_query_but_not_filters() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index
        .update_settings(json!({
            "filterableAttributes": ["kind"],
            "embedders": {
                "default": {
                    "source": "userProvided",
                    "dimensions": 2
                }
            }
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                {
                    "id": "organic-match",
                    "title": "Batman Returns",
                    "kind": "keep",
                    "_vectors": { "default": [1.0, 1.0] }
                },
                {
                    "id": "pinned-query-miss",
                    "title": "The Matrix",
                    "kind": "keep",
                    "_vectors": { "default": [-1.0, -1.0] }
                },
                {
                    "id": "filtered-pin",
                    "title": "Batman Returns",
                    "kind": "drop",
                    "_vectors": { "default": [1.0, 1.0] }
                }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-query-miss-but-filtered-hybrid",
            json!({
                "active": true,
                "conditions": {
                    "query": {
                        "words": "returns"
                    }
                },
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index
        .search_post(json!({
            "q": "Batman Returns",
            "filter": "kind = keep",
            "vector": [1.0, 1.0],
            "hybrid": {
                "embedder": "default",
                "semanticRatio": 0.5
            }
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }),
        @r#"
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
      "requestUid": "[uuid]",
      "semanticHitCount": 1
    }
    "#
    );
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

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-query-miss-but-filtered",
            json!({
                "active": true,
                "conditions": {
                    "query": {
                        "words": "returns"
                    }
                },
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

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
#[ignore = "distinct/pinning semantics to revisit"]
async fn search_distinct_deduplicates_pinned_documents() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index.update_settings(json!({ "filterableAttributes": ["series"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-duplicate", "title": "Batman Returns", "series": "batman" },
                { "id": "pinned-duplicate", "title": "The Matrix", "series": "batman" },
                { "id": "organic-unique", "title": "Batman Forever", "series": "forever" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-duplicate-series",
            json!({
                "active": true,
                "actions": [
                    {
                        "selector": { "id": "pinned-duplicate" },
                        "action": { "type": "pin", "position": 0 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index.search_post(json!({ "q": "Batman", "distinct": "series" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r#"
    {
      "hits": [
        {
          "id": "pinned-duplicate",
          "title": "The Matrix",
          "series": "batman"
        },
        {
          "id": "organic-unique",
          "title": "Batman Forever",
          "series": "forever"
        }
      ],
      "query": "Batman",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]"
    }
    "#);
}

#[actix_web::test]
async fn search_facet_distribution_counts_pins_that_miss_query() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) =
        index.update_settings(json!({ "filterableAttributes": ["kind", "color"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-match", "title": "Batman Returns", "kind": "keep", "color": "red" },
                { "id": "pinned-query-miss", "title": "The Matrix", "kind": "keep", "color": "blue" },
                { "id": "filtered-pin", "title": "Batman Returns", "kind": "drop", "color": "green" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-query-miss-and-filtered",
            json!({
                "active": true,
                "conditions": {
                    "query": {
                        "words": "returns"
                    }
                },
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index
        .search_post(json!({
            "q": "Batman Returns",
            "filter": "kind = keep",
            "facets": ["color"]
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r#"
    {
      "hits": [
        {
          "id": "pinned-query-miss",
          "title": "The Matrix",
          "kind": "keep",
          "color": "blue"
        },
        {
          "id": "organic-match",
          "title": "Batman Returns",
          "kind": "keep",
          "color": "red"
        }
      ],
      "query": "Batman Returns",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "facetDistribution": {
        "color": {
          "blue": 1,
          "red": 1
        }
      },
      "facetStats": {},
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

    let (task, code) = server
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
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index.search_post(json!({ "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), name: "limit_10");

    let (value, code) = index.search_post(json!({ "offset": 2, "limit": 2 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), name: "offset_2_limit_2");
}

#[actix_web::test]
async fn duplicated_word_constraints() {
    let server = dynamic_search_rules_server().await;

    let index = server.index("movies");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "pinned" },
                { "id": "mario" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "double-words-constraints",
            json!({
                "active": true,
                "conditions": {
                  "query": {
                    "words": "Mario Luigi"
                  }
                },
                "actions": [
                    {
                        "selector": { "id": "pinned" },
                        "action": { "type": "pin", "position": 1 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index.search_post(json!({ "q": "mario luigi" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "mario"
      },
      {
        "id": "pinned"
      }
    ]
    "###);

    let (value, code) = index.search_post(json!({ "q": "mario" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "mario"
      }
    ]
    "###);

    let (value, code) = index.search_post(json!({ "q": "mario mario" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "mario"
      }
    ]
    "###);
}

#[actix_web::test]
async fn list_many_rules() {
    let server = dynamic_search_rules_server().await;

    let index = server.index("movies");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "pinned" },
                { "id": "mario" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let mut last_task = None;
    for i in 0..1_001 {
        let (task, code) = server
            .create_dynamic_search_rule(
                format!("dsr-number-{i}"),
                json!({
                    "description": "Some DSR rule",
                    "active": false,
                }),
            )
            .await;
        snapshot!(code, @"202 Accepted");

        last_task = Some(task);
    }

    if let Some(last_task) = last_task {
        server.wait_task(last_task.uid()).await.succeeded();
    }

    let (value, code) = server
        .list_dynamic_search_rules_with(json!({
          "filter": {
            "query": "DSR"
          },
          "offset": 1000
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(value, @r###"
    {
      "results": [
        {
          "uid": "dsr-number-1000",
          "description": "Some DSR rule",
          "active": false,
          "conditions": {},
          "actions": []
        }
      ],
      "offset": 1000,
      "limit": 20,
      "total": 1001
    }
    "###);
}

#[actix_web::test]
async fn search_applies_precedenceless_rules() {
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

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-with-precedence",
            json!({
                "active": true,
                "actions": [
                    {
                        "selector": { "id": "remote" },
                        "action": { "type": "pin", "position": 0 }
                    }
                ],
                "precedence": 10
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-without-precedence",
            json!({
                "active": true,
                "actions": [
                    {
                        "selector": { "id": "local" },
                        // pick another position due to another bug causing
                        // precedence to sometimes get ignored
                        "action": { "type": "pin", "position": 1 }
                    }
                ]
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) =
        index.search_post(json!({ "q": "Missing", "showRankingScoreDetails": true })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "remote",
        "title": "Batman",
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 0
          }
        }
      },
      {
        "id": "local",
        "title": "Batman Returns",
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 1
          }
        }
      }
    ]
    "###);
}
