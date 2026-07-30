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
                "actions": {
                  "pin": [
                    {
                      "id": doc_id,
                      "position": 0
                    }
                  ]
                }
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
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "rule-b",
          "description": "rule-b",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "1",
                "position": 0
              }
            ]
          }
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
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "promo-inactive",
          "description": "promo-inactive",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "2",
                "position": 0
              }
            ]
          }
        },
        {
          "uid": "promo-active",
          "description": "promo-active",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "1",
                "position": 0
              }
            ]
          }
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
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "standard-active",
          "description": "standard-active",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "3",
                "position": 0
              }
            ]
          }
        },
        {
          "uid": "promo-active",
          "description": "promo-active",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "1",
                "position": 0
              }
            ]
          }
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
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "promo-active",
          "description": "promo-active",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "1",
                "position": 0
              }
            ]
          }
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
                "actions": {
                  "pin": [
                    {
                      "id": "42",
                      "position": 1
                    }
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("rule-1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "rule-1",
      "lastUpdatedAt": "[updated]",
      "active": true,
      "conditions": {},
      "actions": {
        "pin": [
          {
            "id": "42",
            "position": 1
          }
        ]
      }
    }
    "###);
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
                "actions": {
                  "pin": [
                    { "indexUid": "products", "id": "123", "position": 1 },
                    { "indexUid": "products", "id": "456", "position": 0 },
                    { "id": "789", "position": 3 },
                    { "id": "999", "position": 8 },
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (get_value, code) = server.get_dynamic_search_rule("black-friday").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(get_value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "black-friday",
      "description": "Black Friday 2025 rules",
      "lastUpdatedAt": "[updated]",
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
      "actions": {
        "pin": [
          {
            "indexUid": "products",
            "id": "123",
            "position": 1
          },
          {
            "indexUid": "products",
            "id": "456",
            "position": 0
          },
          {
            "id": "789",
            "position": 3
          },
          {
            "id": "999",
            "position": 8
          }
        ]
      }
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
                "actions": {
                  "pin": [
                    {
                      "id": "42",
                      "position": 0,
                    }
                  ]
                }
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
        .create_dynamic_search_rule(
            "rule-a",
            json!({
                "actions": {
                  "pin": [
                    { "id": "0", "position": 0 }
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "rule-b",
            json!({
                "actions": {
                  "pin": [
                    { "id": "1", "position": 0 }
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "rule-b",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "1",
                "position": 0
              }
            ]
          }
        },
        {
          "uid": "rule-a",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "0",
                "position": 0
              }
            ]
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);

    let (task, code) = server.delete_dynamic_search_rule("rule-a").await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "rule-b",
          "lastUpdatedAt": "[updated]",
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "1",
                "position": 0
              }
            ]
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);

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
        .create_dynamic_search_rule(
            "updatable",
            json!({
                "actions": {
                  "pin": [
                    {"id":"42", "position":1}
                  ]
                }
            }),
        )
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
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "lastUpdatedAt": "[updated]",
          "precedence": 10,
          "active": true,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "42",
                "position": 1
              }
            ]
          }
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
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "lastUpdatedAt": "[updated]",
          "precedence": 10,
          "active": false,
          "conditions": {},
          "actions": {
            "pin": [
              {
                "id": "42",
                "position": 1
              }
            ]
          }
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
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "lastUpdatedAt": "[updated]",
          "precedence": 10,
          "active": false,
          "conditions": {
            "query": {
              "isEmpty": true
            }
          },
          "actions": {
            "pin": [
              {
                "id": "42",
                "position": 1
              }
            ]
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "lastUpdatedAt": "[updated]",
          "precedence": 10,
          "active": false,
          "conditions": {
            "query": {
              "isEmpty": true
            }
          },
          "actions": {
            "pin": [
              {
                "id": "42",
                "position": 1
              }
            ]
          }
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
            json!({ "actions": {"pin":[
              {"id":"99","position":7}
            ]}}),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.list_dynamic_search_rules().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "updatable",
          "description": "Updated",
          "lastUpdatedAt": "[updated]",
          "precedence": 10,
          "active": false,
          "conditions": {
            "query": {
              "isEmpty": true
            }
          },
          "actions": {
            "pin": [
              {
                "id": "99",
                "position": 7
              }
            ]
          }
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
                "actions": {"pin":[{"id":"1","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("foobar").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "foobar",
      "lastUpdatedAt": "[updated]",
      "active": true,
      "conditions": {},
      "actions": {
        "pin": [
          {
            "id": "1",
            "position": 0
          }
        ]
      }
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
        .create_dynamic_search_rule(
            "rule-x",
            json!({
                "actions": {"pin":[{"id":"1","position":0}]},
                "unknownField": true
            }),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value), @r###"
    {
      "message": "Unknown field `unknownField`: expected one of `description`, `precedence`, `active`, `conditions`, `actions`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_web::test]
async fn patch_unknown_field() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule(
            "rule-y",
            json!({
                "actions": {"pin":[{"id":"1","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) =
        server.patch_dynamic_search_rule("rule-y", json!({ "bogusField": 42 })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value), @r###"
    {
      "message": "Unknown field `bogusField`: expected one of `description`, `precedence`, `active`, `conditions`, `actions`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_web::test]
async fn create_missing_actions() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server.create_dynamic_search_rule("no-actions", json!({})).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("no-actions").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "no-actions",
      "lastUpdatedAt": "[updated]",
      "active": true,
      "conditions": {},
      "actions": {}
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
        .create_dynamic_search_rule(
            "preserve",
            json!({
                "description": "original",
                "precedence": 5,
                "active": true,
                "conditions":
                {
                    "query": {
                        "isEmpty": true
                    }
                },
                "actions": {"pin":[{"id":"1","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("preserve").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "preserve",
      "description": "original",
      "lastUpdatedAt": "[updated]",
      "precedence": 5,
      "active": true,
      "conditions": {
        "query": {
          "isEmpty": true
        }
      },
      "actions": {
        "pin": [
          {
            "id": "1",
            "position": 0
          }
        ]
      }
    }
    "###);

    let (task, code) =
        server.patch_dynamic_search_rule("preserve", json!({ "description": "updated" })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("preserve").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "preserve",
      "description": "updated",
      "lastUpdatedAt": "[updated]",
      "precedence": 5,
      "active": true,
      "conditions": {
        "query": {
          "isEmpty": true
        }
      },
      "actions": {
        "pin": [
          {
            "id": "1",
            "position": 0
          }
        ]
      }
    }
    "###);
}

#[actix_web::test]
async fn patch_replaces_arrays() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule(
            "arrays",
            json!({
                "conditions": {},
                "actions": {
                  "pin": [
                    {
                      "id":"1",
                      "position":0
                    },
                    {
                      "id":"2",
                      "position":2
                    }
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .patch_dynamic_search_rule(
            "arrays",
            json!({ "actions": {"pin":[{"id":"3","position":4}]}
            }),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();
    snapshot!(code, @"202 Accepted");
    let (value, code) = server.get_dynamic_search_rule("arrays").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "arrays",
      "lastUpdatedAt": "[updated]",
      "active": true,
      "conditions": {},
      "actions": {
        "pin": [
          {
            "id": "3",
            "position": 4
          }
        ]
      }
    }
    "###);
}

#[actix_web::test]
async fn patch_empty_body() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule(
            "no-change",
            json!({
                "active": true,
                "actions": {"pin":[{"id":"1","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server.patch_dynamic_search_rule("no-change", json!({})).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server.get_dynamic_search_rule("no-change").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "no-change",
      "lastUpdatedAt": "[updated]",
      "active": true,
      "conditions": {},
      "actions": {
        "pin": [
          {
            "id": "1",
            "position": 0
          }
        ]
      }
    }
    "###);
}

#[actix_web::test]
async fn defaults_on_create() {
    let server = dynamic_search_rules_server().await;

    let (task, code) = server
        .create_dynamic_search_rule(
            "minimal",
            json!({
                "actions": {"pin":[{"id":"1","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();
    let (value, code) = server.get_dynamic_search_rule("minimal").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".lastUpdatedAt" => "[updated]"}), @r###"
    {
      "uid": "minimal",
      "lastUpdatedAt": "[updated]",
      "active": true,
      "conditions": {},
      "actions": {
        "pin": [
          {
            "id": "1",
            "position": 0
          }
        ]
      }
    }
    "###);
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
                "actions": {"pin":[{"id":"remote","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index.search_post(json!({ "q": "batman returns" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
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
    "###);

    let (value, code) = server.set_features(json!({ "dynamicSearchRules": false })).await;
    snapshot!(code, @"200 OK");
    snapshot!(value["dynamicSearchRules"], @"false");

    let (value, code) = index.search_post(json!({ "q": "batman returns" })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "local",
        "title": "Batman Returns"
      },
      {
        "id": "remote",
        "title": "Batman"
      }
    ]
    "###);
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
                "actions": {"pin":[{"id":"remote","position":0}]}
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
async fn search_removes_hidden_documents() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "keep-doc-1" },
                { "id": "hidden-doc-1" },
                { "id": "keep-doc-2" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // regular search shows all three docs
    let (value, code) = index.search_post(json!({ "q": "doc", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "keep-doc-1"
      },
      {
        "id": "hidden-doc-1"
      },
      {
        "id": "keep-doc-2"
      }
    ]
    "###);

    // add a rule to hide a doc when looking for "doc"
    let (task, code) = server
        .create_dynamic_search_rule(
            "hide-hidden",
            json!({
                "active": true,
                "conditions": {
                  "query": {
                    "words": "doc"
                  }
                },
                "actions": {
                  "scale": [
                    {"ids": ["hidden-doc-1"], "weight": 0.0}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // searching for "doc" hides the hidden doc
    let (value, code) = index.search_post(json!({ "q": "doc", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "keep-doc-1"
      },
      {
        "id": "keep-doc-2"
      }
    ]
    "###);

    // placeholder search still show the doc
    let (value, code) = index.search_post(json!({ "q": "", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "keep-doc-1"
      },
      {
        "id": "hidden-doc-1"
      },
      {
        "id": "keep-doc-2"
      }
    ]
    "###);
}

#[actix_web::test]
async fn search_boost_deboost_hide() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) = index.update_settings(json!({ "filterableAttributes": ["kind"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "boosted-doc", "kind": "boost" },
                { "id": "deboosted-doc-with-optional", "kind": "deboost", },
                { "id": "hidden-doc-with-optional", "kind": "hide", },
                { "id": "unchanged-doc-with-optional", "kind": "unchanged" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // "natural" search without a DSR
    let (value, code) = index.search_post(json!({ "q": "doc optional", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "deboosted-doc-with-optional",
        "kind": "deboost"
      },
      {
        "id": "hidden-doc-with-optional",
        "kind": "hide"
      },
      {
        "id": "unchanged-doc-with-optional",
        "kind": "unchanged"
      },
      {
        "id": "boosted-doc",
        "kind": "boost"
      }
    ]
    "###);

    // add scaling rule depending on kind
    let (task, code) = server
        .create_dynamic_search_rule(
            "scale-kind",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    {"filter": "kind = boost", "weight": 2.5},
                    {"filter": "kind = deboost", "weight": 0.5},
                    {"filter": "kind = hide", "weight": 0.0}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // same search with the DSR
    let (value, code) = index.search_post(json!({ "q": "doc optional", "showRankingScore": true, "showRankingScoreDetails": true, "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "boosted-doc",
        "kind": "boost",
        "_rankingScore": 0.4621212121212121,
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "scale-kind",
                "weight": 2.5
              }
            ],
            "totalWeight": 2.5
          },
          "words": {
            "order": 1,
            "matchingWords": 1,
            "maxMatchingWords": 2,
            "score": 0.5
          },
          "typo": {
            "order": 2,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 3,
            "score": 1.0
          },
          "attributeRank": {
            "order": 4,
            "score": 1.0
          },
          "wordPosition": {
            "order": 5,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 6,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      },
      {
        "id": "unchanged-doc-with-optional",
        "kind": "unchanged",
        "_rankingScore": 0.951058201058201,
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 2,
            "maxMatchingWords": 2,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 2,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 0.75
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.8571428571428571
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 2,
            "maxMatchingWords": 2,
            "score": 0.3333333333333333
          }
        }
      },
      {
        "id": "deboosted-doc-with-optional",
        "kind": "deboost",
        "_rankingScore": 0.951058201058201,
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "scale-kind",
                "weight": 0.5
              }
            ],
            "totalWeight": 0.5
          },
          "words": {
            "order": 1,
            "matchingWords": 2,
            "maxMatchingWords": 2,
            "score": 1.0
          },
          "typo": {
            "order": 2,
            "typoCount": 0,
            "maxTypoCount": 2,
            "score": 1.0
          },
          "proximity": {
            "order": 3,
            "score": 0.75
          },
          "attributeRank": {
            "order": 4,
            "score": 1.0
          },
          "wordPosition": {
            "order": 5,
            "score": 0.8571428571428571
          },
          "exactness": {
            "order": 6,
            "matchType": "noExactMatch",
            "matchingWords": 2,
            "maxMatchingWords": 2,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);
}

#[actix_web::test]
async fn search_scale_combination() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) =
        index.update_settings(json!({ "filterableAttributes": ["category", "color"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "red-shorts", "category": "shorts", "color": "red" },
                { "id": "blue-shorts", "category": "shorts", "color": "blue" },
                { "id": "green-pants", "category": "pants", "color": "green" },
                { "id": "blue-pants", "category": "pants", "color": "blue" },
                { "id": "orange-pants", "category": "pants", "color": "orange" },
                { "id": "red-pants", "category": "pants", "color": "red" },
                { "id": "green-shirt", "category": "shirt", "color": "green" },
                { "id": "blue-shirt", "category": "shirt", "color": "blue" },
                { "id": "orange-shirt", "category": "shirt", "color": "orange" },
                { "id": "red-shirt", "category": "shirt", "color": "red" },
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // "natural" search without a DSR
    let (value, code) = index.search_post(json!({ "q": "", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "red-shorts",
        "category": "shorts",
        "color": "red"
      },
      {
        "id": "blue-shorts",
        "category": "shorts",
        "color": "blue"
      },
      {
        "id": "green-pants",
        "category": "pants",
        "color": "green"
      },
      {
        "id": "blue-pants",
        "category": "pants",
        "color": "blue"
      },
      {
        "id": "orange-pants",
        "category": "pants",
        "color": "orange"
      },
      {
        "id": "red-pants",
        "category": "pants",
        "color": "red"
      },
      {
        "id": "green-shirt",
        "category": "shirt",
        "color": "green"
      },
      {
        "id": "blue-shirt",
        "category": "shirt",
        "color": "blue"
      },
      {
        "id": "orange-shirt",
        "category": "shirt",
        "color": "orange"
      },
      {
        "id": "red-shirt",
        "category": "shirt",
        "color": "red"
      }
    ]
    "###);

    // scale rules depending on user preference
    let (task, code) = server
        .create_dynamic_search_rule(
            "color-pref",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    // this user likes red best
                    {"filter": "color = red", "weight": 1.5},
                    // this user likes orange least
                    {"filter": "color = orange", "weight": 0.8}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "cat-pref",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    // this user likes shirt best
                    {"filter": "category = shirt", "weight": 1.2},
                    // this user never wants to see shorts
                    {"filter": "category = shorts", "weight": 0.0}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // search with the DSRs
    let (value, code) =
        index.search_post(json!({ "q": "", "showRankingScoreDetails": true,"limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "red-shirt",
        "category": "shirt",
        "color": "red",
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "color-pref",
                "weight": 1.5
              },
              {
                "ruleUid": "cat-pref",
                "weight": 1.2
              }
            ],
            "totalWeight": 1.7999999999999998
          }
        }
      },
      {
        "id": "red-pants",
        "category": "pants",
        "color": "red",
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "color-pref",
                "weight": 1.5
              }
            ],
            "totalWeight": 1.5
          }
        }
      },
      {
        "id": "green-shirt",
        "category": "shirt",
        "color": "green",
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "cat-pref",
                "weight": 1.2
              }
            ],
            "totalWeight": 1.2
          }
        }
      },
      {
        "id": "blue-shirt",
        "category": "shirt",
        "color": "blue",
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "cat-pref",
                "weight": 1.2
              }
            ],
            "totalWeight": 1.2
          }
        }
      },
      {
        "id": "green-pants",
        "category": "pants",
        "color": "green",
        "_rankingScoreDetails": {}
      },
      {
        "id": "blue-pants",
        "category": "pants",
        "color": "blue",
        "_rankingScoreDetails": {}
      },
      {
        "id": "orange-shirt",
        "category": "shirt",
        "color": "orange",
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "color-pref",
                "weight": 0.8
              },
              {
                "ruleUid": "cat-pref",
                "weight": 1.2
              }
            ],
            "totalWeight": 0.96
          }
        }
      },
      {
        "id": "orange-pants",
        "category": "pants",
        "color": "orange",
        "_rankingScoreDetails": {
          "scale": {
            "order": 0,
            "actions": [
              {
                "ruleUid": "color-pref",
                "weight": 0.8
              }
            ],
            "totalWeight": 0.8
          }
        }
      }
    ]
    "###);
}

#[actix_web::test]
async fn search_pin_wins_over_boost() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "boosted-and-pinned-doc" },
                { "id": "organic-premier-doc-1" },
                { "id": "organic-premier-doc-2" },
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // add a rule with pin and boost
    let (task, code) = server
        .create_dynamic_search_rule(
            "boost-and-pin",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    {"ids": ["boosted-and-pinned-doc"], "weight": 10.0} // major boost should guarantee it is first
                  ],
                  "pin": [
                    {"id": "boosted-and-pinned-doc", "position": 1} // will be number 2
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // search that should put organic docs first
    let (value, code) = index
        .search_post(
            json!({ "q": "doc premier organic", "limit": 10, "showRankingScoreDetails": true }),
        )
        .await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "organic-premier-doc-1",
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 3,
            "maxMatchingWords": 3,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 3,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 0.7142857142857143
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.8709677419354839
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 3,
            "maxMatchingWords": 3,
            "score": 0.3333333333333333
          }
        }
      },
      {
        "id": "boosted-and-pinned-doc",
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 1,
            "precedence": null,
            "ruleUid": "boost-and-pin"
          }
        }
      },
      {
        "id": "organic-premier-doc-2",
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 3,
            "maxMatchingWords": 3,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 3,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 0.7142857142857143
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.8709677419354839
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 3,
            "maxMatchingWords": 3,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);
}

#[actix_web::test]
async fn search_ids_and_filter() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) = index.update_settings(json!({ "filterableAttributes": ["kind"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "actually-keep", "kind": "hide" },
                { "id": "really-hidden", "kind": "hide", },
                { "id": "not-hidden", "kind": "keep", }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // add a rule with ids AND filter
    let (task, code) = server
        .create_dynamic_search_rule(
            "hide-hidden",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    {"ids": ["really-hidden"], "filter": "kind = hide", "weight": 0.0}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // placeholder search keeps document that is in filter but not in ids
    let (value, code) = index.search_post(json!({ "q": "", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "actually-keep",
        "kind": "hide"
      },
      {
        "id": "not-hidden",
        "kind": "keep"
      }
    ]
    "###);
}

#[actix_web::test]
async fn search_pin_wins_over_hide() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("products");

    let (task, code) = index.update_settings(json!({ "filterableAttributes": ["kind"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "keep-doc-1", "kind": "keep" },
                { "id": "hidden-and-pinned-doc-1", "kind": "hide", },
                { "id": "hidden-doc-1", "kind": "hide", },
                { "id": "keep-doc-2", "kind": "keep" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // add a rule to hide a doc
    let (task, code) = server
        .create_dynamic_search_rule(
            "hide-hidden",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    {"filter": "kind = hide", "weight": 0.0}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // another rule to pin a hidden doc
    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-hidden",
            json!({
                "conditions": {
                  "query": {
                    "words": "doc"
                  }
                },
                "active": true,
                "actions": {
                  "pin": [
                    {"id": "hidden-and-pinned-doc-1", "position": 0}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // placeholder search hides all hidden docs
    let (value, code) = index.search_post(json!({ "q": "", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "keep-doc-1",
        "kind": "keep"
      },
      {
        "id": "keep-doc-2",
        "kind": "keep"
      }
    ]
    "###);

    // search on doc which has both rules active keeps the hidden doc because it is pinned
    let (value, code) =
        index.search_post(json!({ "q": "doc", "limit": 10, "showRankingScoreDetails":true })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "hidden-and-pinned-doc-1",
        "kind": "hide",
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 0,
            "precedence": null,
            "ruleUid": "pin-hidden"
          }
        }
      },
      {
        "id": "keep-doc-1",
        "kind": "keep",
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      },
      {
        "id": "keep-doc-2",
        "kind": "keep",
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);
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
                "actions": {"pin":[{"id":"filtered-pin","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index.search_post(json!({ "filter": "kind = keep", "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "organic-1",
        "kind": "keep"
      },
      {
        "id": "organic-2",
        "kind": "keep"
      }
    ]
    "###);
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
                "actions": {
                  "pin": [
                    {"id":"pinned-query-miss", "position":0},
                    {"id":"filter-pin", "position":1},
                  ]
                }
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
                "actions": {
                  "pin": [
                    {"id":"pinned-query-miss","position":0},
                    {"id":"filtered-pin","position":1}
                  ]
                }
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
                "actions": {
                  "pin":[
                    {"id":"pinned-query-miss", "position":0},
                    {"id":"filtered-pin", "position":1}
                  ]
                }
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
                "actions": {"pin": [{"id":"pinned-duplicate","position":0}]}
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
                "actions": {"pin":[
                  {"id":"pinned-query-miss","position":0},
                  {"id":"filtered-pin","position":1}
                ]}
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
                "actions": {"pin":[
                  {"id":"late-pin-1", "position":10},
                  {"id":"late-pin-2", "position":20}
                ]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = index.search_post(json!({ "limit": 10 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "organic-1"
      },
      {
        "id": "organic-2"
      },
      {
        "id": "late-pin-1"
      },
      {
        "id": "late-pin-2"
      }
    ]
    "###);

    let (value, code) = index.search_post(json!({ "offset": 2, "limit": 2 })).await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "late-pin-1"
      },
      {
        "id": "late-pin-2"
      }
    ]
    "###);
}

#[actix_web::test]
async fn filter_conditions() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) =
        index.update_settings(json!({ "searchableAttributes": ["title"], "filterableAttributes": ["series", "genres"] })).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "pin-on-matrix", "title": "Batman Returns", "series": "batman", "genres": ["Action", "Superhero"] },
                { "id": "organic-on-matrix", "title": "The Matrix", "series": "batman", "genres": ["Action", "SciFi"] },
                { "id": "organic-in-batman", "title": "Batman Forever", "series": "forever", "genres":["Action", "Superhero"] },
                { "id": "pin-on-multi", "title": "Batman the animation", "series": "batman", "genres":["Action", "Superhero", "Animation"] }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-on-matrix-action",
            json!({
                "conditions": {
                  "query": {
                    "words": "Matrix"
                  },
                  "filter": {
                    "values": {
                      "genres": "action",
                    }
                  }
                },
                "active": true,
                "actions": {"pin":[{"id":"pin-on-matrix","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // only query matches, no filter
    let (value, code) =
        index.search_post(json!({ "q": "Matrix", "showRankingScoreDetails": true })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "organic-on-matrix",
        "title": "The Matrix",
        "series": "batman",
        "genres": [
          "Action",
          "SciFi"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);

    // pin on query + filter
    let (value, code) = index
        .search_post(
            json!({ "q": "Matrix", "filter": "genres = action", "showRankingScoreDetails": true }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "pin-on-matrix",
        "title": "Batman Returns",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 0,
            "precedence": null,
            "ruleUid": "pin-on-matrix-action"
          }
        }
      },
      {
        "id": "organic-on-matrix",
        "title": "The Matrix",
        "series": "batman",
        "genres": [
          "Action",
          "SciFi"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);

    // pin on query + filter still works if overconstrained
    let (value, code) = index
        .search_post(json!({ "q": "Matrix", "filter": "series = batman AND genres = action", "showRankingScoreDetails": true }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "pin-on-matrix",
        "title": "Batman Returns",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 0,
            "precedence": null,
            "ruleUid": "pin-on-matrix-action"
          }
        }
      },
      {
        "id": "organic-on-matrix",
        "title": "The Matrix",
        "series": "batman",
        "genres": [
          "Action",
          "SciFi"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);

    // pin on query + filter still works if overconstrained 2
    let (value, code) = index
        .search_post(json!({ "q": "Matrix", "filter": "series = batman OR genres = action", "showRankingScoreDetails": true }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "pin-on-matrix",
        "title": "Batman Returns",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 0,
            "precedence": null,
            "ruleUid": "pin-on-matrix-action"
          }
        }
      },
      {
        "id": "organic-on-matrix",
        "title": "The Matrix",
        "series": "batman",
        "genres": [
          "Action",
          "SciFi"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);

    // pin on filter alone doesn't work
    let (value, code) = index
        .search_post(
            json!({ "q": "", "filter": "series = batman", "showRankingScoreDetails": true }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "pin-on-matrix",
        "title": "Batman Returns",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {}
      },
      {
        "id": "organic-on-matrix",
        "title": "The Matrix",
        "series": "batman",
        "genres": [
          "Action",
          "SciFi"
        ],
        "_rankingScoreDetails": {}
      },
      {
        "id": "pin-on-multi",
        "title": "Batman the animation",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero",
          "Animation"
        ],
        "_rankingScoreDetails": {}
      }
    ]
    "###);

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-on-genres-series",
            json!({
                "conditions": {
                  "filter": {
                    "values": {
                      "genres": "Action",
                      "series": "batman",
                    }
                  }
                },
                "active": true,
                "actions": {"pin":[{"id":"pin-on-multi","position":1}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    // only first rule triggers
    let (value, code) = index
        .search_post(
            json!({ "q": "Matrix", "filter": "genres = action", "showRankingScoreDetails": true }),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "pin-on-matrix",
        "title": "Batman Returns",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 0,
            "precedence": null,
            "ruleUid": "pin-on-matrix-action"
          }
        }
      },
      {
        "id": "organic-on-matrix",
        "title": "The Matrix",
        "series": "batman",
        "genres": [
          "Action",
          "SciFi"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 0.9090909090909092
          },
          "exactness": {
            "order": 5,
            "matchType": "noExactMatch",
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 0.3333333333333333
          }
        }
      }
    ]
    "###);

    // only second rule
    let (value, code) = index
        .search_post(json!({ "q": "Batman", "filter": "genres = Action AND series = Batman", "showRankingScoreDetails": true }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "pin-on-matrix",
        "title": "Batman Returns",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 1.0
          },
          "exactness": {
            "order": 5,
            "matchType": "matchesStart",
            "score": 0.6666666666666666
          }
        }
      },
      {
        "id": "pin-on-multi",
        "title": "Batman the animation",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero",
          "Animation"
        ],
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 1,
            "precedence": null,
            "ruleUid": "pin-on-genres-series"
          }
        }
      }
    ]
    "###);

    // no rule
    let (value, code) = index
        .search_post(json!({ "q": "Batman", "filter": "genres = action OR series = batman", "showRankingScoreDetails": true }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value["hits"]), @r###"
    [
      {
        "id": "pin-on-matrix",
        "title": "Batman Returns",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 1.0
          },
          "exactness": {
            "order": 5,
            "matchType": "matchesStart",
            "score": 0.6666666666666666
          }
        }
      },
      {
        "id": "organic-in-batman",
        "title": "Batman Forever",
        "series": "forever",
        "genres": [
          "Action",
          "Superhero"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 1.0
          },
          "exactness": {
            "order": 5,
            "matchType": "matchesStart",
            "score": 0.6666666666666666
          }
        }
      },
      {
        "id": "pin-on-multi",
        "title": "Batman the animation",
        "series": "batman",
        "genres": [
          "Action",
          "Superhero",
          "Animation"
        ],
        "_rankingScoreDetails": {
          "words": {
            "order": 0,
            "matchingWords": 1,
            "maxMatchingWords": 1,
            "score": 1.0
          },
          "typo": {
            "order": 1,
            "typoCount": 0,
            "maxTypoCount": 1,
            "score": 1.0
          },
          "proximity": {
            "order": 2,
            "score": 1.0
          },
          "attributeRank": {
            "order": 3,
            "score": 1.0
          },
          "wordPosition": {
            "order": 4,
            "score": 1.0
          },
          "exactness": {
            "order": 5,
            "matchType": "matchesStart",
            "score": 0.6666666666666666
          }
        }
      }
    ]
    "###);
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
                "actions": {"pin":[{"id":"pinned","position":1}]}
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
          "offset": 990
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, {".results[].lastUpdatedAt" => "[updated]"}), @r###"
    {
      "results": [
        {
          "uid": "dsr-number-10",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-9",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-8",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-7",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-6",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-5",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-4",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-3",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-2",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-1",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        },
        {
          "uid": "dsr-number-0",
          "description": "Some DSR rule",
          "lastUpdatedAt": "[updated]",
          "active": false,
          "conditions": {},
          "actions": {}
        }
      ],
      "offset": 990,
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
                "actions": {"pin":[{"id":"remote","position":0}]},
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
                "actions": {
                  "pin": [
                    {
                      "id": "local",
                      // pick another position due to another bug causing
                      // precedence to sometimes get ignored
                      "position": 1
                    }
                  ]
                }
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
            "position": 0,
            "precedence": 10,
            "ruleUid": "pin-with-precedence"
          }
        }
      },
      {
        "id": "local",
        "title": "Batman Returns",
        "_rankingScoreDetails": {
          "pin": {
            "order": 0,
            "position": 1,
            "precedence": null,
            "ruleUid": "pin-without-precedence"
          }
        }
      }
    ]
    "###);
}

#[actix_web::test]
async fn multi_search_deduplicates_pins() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-match", "title": "Batman Returns" },
                { "id": "pinned-query-miss", "title": "The Matrix" },
                { "id": "filtered-pin", "title": "Batman Returns" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-invoked-twice-in-multi-search",
            json!({
                "active": true,
                "conditions": {
                    "query": {
                        "words": "returns"
                    }
                },
                "actions": {
                  "pin": [
                    {"id":"pinned-query-miss","position":0},
                    {"id":"filtered-pin","position":1}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server
        .multi_search(json!({
          "federation": {},
          "queries": [
            // 2 identical queries, both trigger the DSR
           { "q": "Batman Returns", "indexUid": "movies", "showRankingScoreDetails": true },
           { "q": "Batman Returns", "indexUid": "movies", "showRankingScoreDetails": true }
          ]
        }))
        .await;
    snapshot!(code, @"200 OK");
    // docs are pinned only once
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
    {
      "hits": [
        {
          "id": "pinned-query-miss",
          "title": "The Matrix",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "pin": {
              "order": 0,
              "position": 0,
              "precedence": null,
              "ruleUid": "pin-invoked-twice-in-multi-search"
            }
          }
        },
        {
          "id": "filtered-pin",
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "pin": {
              "order": 0,
              "position": 1,
              "precedence": null,
              "ruleUid": "pin-invoked-twice-in-multi-search"
            }
          }
        },
        {
          "id": "organic-match",
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "words": {
              "order": 0,
              "matchingWords": 2,
              "maxMatchingWords": 2,
              "score": 1.0
            },
            "typo": {
              "order": 1,
              "typoCount": 0,
              "maxTypoCount": 2,
              "score": 1.0
            },
            "proximity": {
              "order": 2,
              "score": 1.0
            },
            "attributeRank": {
              "order": 3,
              "score": 1.0
            },
            "wordPosition": {
              "order": 4,
              "score": 1.0
            },
            "exactness": {
              "order": 5,
              "matchType": "exactMatch",
              "score": 1.0
            }
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_web::test]
async fn multi_search_lower_precedence_pin_wins() {
    let server = dynamic_search_rules_server().await;
    let index = server.index("movies");

    let (task, code) = index
        .add_documents(
            json!([
                { "id": "organic-match", "title": "Batman Returns" },
                { "id": "pinned-twice", "title": "The Matrix" },
                { "id": "filtered-pin", "title": "Batman Returns" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-for-query-0",
            json!({
                "precedence": 42,
                "active": true,
                "conditions": {
                    "query": {
                        "words": "returns"
                    }
                },
                "actions": {"pin":[{"id":"pinned-twice","position":0}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "pin-for-query-1",
            json!({
                "precedence": 0,
                "active": true,
                "conditions": {
                    "query": {
                        "words": "batman"
                    }
                },
                "actions": {"pin":[{"id":"pinned-twice","position":1}]}
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server
        .multi_search(json!({
          "federation": {},
          "queries": [
            // each query triggers one DSR, each DSR wants to pin the same document in different locations
           { "q": "Returns", "indexUid": "movies", "showRankingScoreDetails": true },
           { "q": "Batman", "indexUid": "movies", "showRankingScoreDetails": true }
          ]
        }))
        .await;
    snapshot!(code, @"200 OK");
    // doc is only pinned once at the location (1) decided by the rule with earliest precedence
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
    {
      "hits": [
        {
          "id": "organic-match",
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScoreDetails": {
            "words": {
              "order": 0,
              "matchingWords": 1,
              "maxMatchingWords": 1,
              "score": 1.0
            },
            "typo": {
              "order": 1,
              "typoCount": 0,
              "maxTypoCount": 1,
              "score": 1.0
            },
            "proximity": {
              "order": 2,
              "score": 1.0
            },
            "attributeRank": {
              "order": 3,
              "score": 1.0
            },
            "wordPosition": {
              "order": 4,
              "score": 1.0
            },
            "exactness": {
              "order": 5,
              "matchType": "matchesStart",
              "score": 0.6666666666666666
            }
          }
        },
        {
          "id": "pinned-twice",
          "title": "The Matrix",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "pin": {
              "order": 0,
              "position": 1,
              "precedence": 0,
              "ruleUid": "pin-for-query-1"
            }
          }
        },
        {
          "id": "filtered-pin",
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScoreDetails": {
            "words": {
              "order": 0,
              "matchingWords": 1,
              "maxMatchingWords": 1,
              "score": 1.0
            },
            "typo": {
              "order": 1,
              "typoCount": 0,
              "maxTypoCount": 1,
              "score": 1.0
            },
            "proximity": {
              "order": 2,
              "score": 1.0
            },
            "attributeRank": {
              "order": 3,
              "score": 1.0
            },
            "wordPosition": {
              "order": 4,
              "score": 1.0
            },
            "exactness": {
              "order": 5,
              "matchType": "matchesStart",
              "score": 0.6666666666666666
            }
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_web::test]
async fn multi_search_boost() {
    let server = dynamic_search_rules_server().await;
    let movies_index = server.index("movies");
    let comics_index = server.index("comics");

    let (task, code) = movies_index
        .add_documents(
            json!([
                { "id": "dark-knight-1", "title": "The Dark Knight Returns Part 1" },
                { "id": "dark-knight-2", "title": "The Dark Knight Returns Part 2" },
                { "id": "batman-returns", "title": "Batman Returns" },
                { "id": "superman-returns", "title": "Superman Returns" }
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = comics_index
        .add_documents(
            json!([
                { "id": "batman-returns", "title": "Batman Returns (Comics)" },
                { "id": "superman-returns", "title": "Superman Returns - The Prequels" },
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server
        .multi_search(json!({
          "federation": {},
          "queries": [
           { "q": "Returns Superman Prequel", "indexUid": "movies", "showRankingScore": true },
           { "q": "Returns Superman Prequel", "indexUid": "comics", "showRankingScore": true }
          ]
        }))
        .await;
    snapshot!(code, @"200 OK");
    // doc is only pinned once at the location (1) decided by the rule with earliest precedence
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
    {
      "hits": [
        {
          "id": "superman-returns",
          "title": "Superman Returns - The Prequels",
          "_federation": {
            "indexUid": "comics",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9628456221198156
          },
          "_rankingScore": 0.9628456221198156
        },
        {
          "id": "superman-returns",
          "title": "Superman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.6353615520282186
          },
          "_rankingScore": 0.6353615520282186
        },
        {
          "id": "batman-returns",
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.30808080808080807
          },
          "_rankingScore": 0.30808080808080807
        },
        {
          "id": "batman-returns",
          "title": "Batman Returns (Comics)",
          "_federation": {
            "indexUid": "comics",
            "queriesPosition": 1,
            "weightedRankingScore": 0.30808080808080807
          },
          "_rankingScore": 0.30808080808080807
        },
        {
          "id": "dark-knight-1",
          "title": "The Dark Knight Returns Part 1",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.29292929292929293
          },
          "_rankingScore": 0.29292929292929293
        },
        {
          "id": "dark-knight-2",
          "title": "The Dark Knight Returns Part 2",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.29292929292929293
          },
          "_rankingScore": 0.29292929292929293
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 6,
      "requestUid": "[uuid]"
    }
    "###);

    // add DSRs
    let (task, code) = server
        .create_dynamic_search_rule(
            "batman-festival",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    {"ids": ["dark-knight-1", "dark-knight-2", "batman-returns"], "weight": 2.5}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = server
        .create_dynamic_search_rule(
            "batman-returns-sales",
            json!({
                "active": true,
                "actions": {
                  "scale": [
                    {"ids": ["batman-returns"], "weight": 1.5}
                  ]
                }
            }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(task.uid()).await.succeeded();

    let (value, code) = server
        .multi_search(json!({
          "federation": {},
          "queries": [
           { "q": "Returns Superman Prequel", "indexUid": "movies", "showRankingScore": true, "showRankingScoreDetails": true },
           { "q": "Returns Superman Prequel", "indexUid": "comics", "showRankingScore": true, "showRankingScoreDetails": true }
          ]
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
    {
      "hits": [
        {
          "id": "batman-returns",
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.30808080808080807
          },
          "_rankingScore": 0.30808080808080807,
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                },
                {
                  "ruleUid": "batman-returns-sales",
                  "weight": 1.5
                }
              ],
              "totalWeight": 3.75
            },
            "words": {
              "order": 1,
              "matchingWords": 1,
              "maxMatchingWords": 3,
              "score": 0.3333333333333333
            },
            "typo": {
              "order": 2,
              "typoCount": 0,
              "maxTypoCount": 1,
              "score": 1.0
            },
            "proximity": {
              "order": 3,
              "score": 1.0
            },
            "attributeRank": {
              "order": 4,
              "score": 1.0
            },
            "wordPosition": {
              "order": 5,
              "score": 0.9090909090909092
            },
            "exactness": {
              "order": 6,
              "matchType": "noExactMatch",
              "matchingWords": 1,
              "maxMatchingWords": 1,
              "score": 0.3333333333333333
            }
          }
        },
        {
          "id": "batman-returns",
          "title": "Batman Returns (Comics)",
          "_federation": {
            "indexUid": "comics",
            "queriesPosition": 1,
            "weightedRankingScore": 0.30808080808080807
          },
          "_rankingScore": 0.30808080808080807,
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                },
                {
                  "ruleUid": "batman-returns-sales",
                  "weight": 1.5
                }
              ],
              "totalWeight": 3.75
            },
            "words": {
              "order": 1,
              "matchingWords": 1,
              "maxMatchingWords": 3,
              "score": 0.3333333333333333
            },
            "typo": {
              "order": 2,
              "typoCount": 0,
              "maxTypoCount": 1,
              "score": 1.0
            },
            "proximity": {
              "order": 3,
              "score": 1.0
            },
            "attributeRank": {
              "order": 4,
              "score": 1.0
            },
            "wordPosition": {
              "order": 5,
              "score": 0.9090909090909092
            },
            "exactness": {
              "order": 6,
              "matchType": "noExactMatch",
              "matchingWords": 1,
              "maxMatchingWords": 1,
              "score": 0.3333333333333333
            }
          }
        },
        {
          "id": "superman-returns",
          "title": "Superman Returns - The Prequels",
          "_federation": {
            "indexUid": "comics",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9628456221198156
          },
          "_rankingScore": 0.9628456221198156,
          "_rankingScoreDetails": {
            "words": {
              "order": 0,
              "matchingWords": 3,
              "maxMatchingWords": 3,
              "score": 1.0
            },
            "typo": {
              "order": 1,
              "typoCount": 0,
              "maxTypoCount": 3,
              "score": 1.0
            },
            "proximity": {
              "order": 2,
              "score": 0.5714285714285714
            },
            "attributeRank": {
              "order": 3,
              "score": 1.0
            },
            "wordPosition": {
              "order": 4,
              "score": 0.9032258064516128
            },
            "exactness": {
              "order": 5,
              "matchType": "noExactMatch",
              "matchingWords": 2,
              "maxMatchingWords": 3,
              "score": 0.25
            }
          }
        },
        {
          "id": "dark-knight-1",
          "title": "The Dark Knight Returns Part 1",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.29292929292929293
          },
          "_rankingScore": 0.29292929292929293,
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                }
              ],
              "totalWeight": 2.5
            },
            "words": {
              "order": 1,
              "matchingWords": 1,
              "maxMatchingWords": 3,
              "score": 0.3333333333333333
            },
            "typo": {
              "order": 2,
              "typoCount": 0,
              "maxTypoCount": 1,
              "score": 1.0
            },
            "proximity": {
              "order": 3,
              "score": 1.0
            },
            "attributeRank": {
              "order": 4,
              "score": 1.0
            },
            "wordPosition": {
              "order": 5,
              "score": 0.8181818181818182
            },
            "exactness": {
              "order": 6,
              "matchType": "noExactMatch",
              "matchingWords": 1,
              "maxMatchingWords": 1,
              "score": 0.3333333333333333
            }
          }
        },
        {
          "id": "dark-knight-2",
          "title": "The Dark Knight Returns Part 2",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.29292929292929293
          },
          "_rankingScore": 0.29292929292929293,
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                }
              ],
              "totalWeight": 2.5
            },
            "words": {
              "order": 1,
              "matchingWords": 1,
              "maxMatchingWords": 3,
              "score": 0.3333333333333333
            },
            "typo": {
              "order": 2,
              "typoCount": 0,
              "maxTypoCount": 1,
              "score": 1.0
            },
            "proximity": {
              "order": 3,
              "score": 1.0
            },
            "attributeRank": {
              "order": 4,
              "score": 1.0
            },
            "wordPosition": {
              "order": 5,
              "score": 0.8181818181818182
            },
            "exactness": {
              "order": 6,
              "matchType": "noExactMatch",
              "matchingWords": 1,
              "maxMatchingWords": 1,
              "score": 0.3333333333333333
            }
          }
        },
        {
          "id": "superman-returns",
          "title": "Superman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 0.6353615520282186
          },
          "_rankingScore": 0.6353615520282186,
          "_rankingScoreDetails": {
            "words": {
              "order": 0,
              "matchingWords": 2,
              "maxMatchingWords": 3,
              "score": 0.6666666666666666
            },
            "typo": {
              "order": 1,
              "typoCount": 0,
              "maxTypoCount": 2,
              "score": 1.0
            },
            "proximity": {
              "order": 2,
              "score": 0.75
            },
            "attributeRank": {
              "order": 3,
              "score": 1.0
            },
            "wordPosition": {
              "order": 4,
              "score": 0.9047619047619048
            },
            "exactness": {
              "order": 5,
              "matchType": "noExactMatch",
              "matchingWords": 2,
              "maxMatchingWords": 2,
              "score": 0.3333333333333333
            }
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 6,
      "requestUid": "[uuid]"
    }
    "###);

    // placeholder still applies the scaling
    let (value, code) = server
        .multi_search(json!({
          "federation": {},
          "queries": [
           { "q": "", "indexUid": "movies", "showRankingScoreDetails": true },
           { "q": "", "indexUid": "comics", "showRankingScoreDetails": true }
          ]
        }))
        .await;
    snapshot!(code, @"200 OK");

    snapshot!(json_string!(value, { ".requestUid" => "[uuid]", ".processingTimeMs" => "[duration]" }), @r###"
    {
      "hits": [
        {
          "id": "batman-returns",
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                },
                {
                  "ruleUid": "batman-returns-sales",
                  "weight": 1.5
                }
              ],
              "totalWeight": 3.75
            }
          }
        },
        {
          "id": "batman-returns",
          "title": "Batman Returns (Comics)",
          "_federation": {
            "indexUid": "comics",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                },
                {
                  "ruleUid": "batman-returns-sales",
                  "weight": 1.5
                }
              ],
              "totalWeight": 3.75
            }
          }
        },
        {
          "id": "dark-knight-1",
          "title": "The Dark Knight Returns Part 1",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                }
              ],
              "totalWeight": 2.5
            }
          }
        },
        {
          "id": "dark-knight-2",
          "title": "The Dark Knight Returns Part 2",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {
            "scale": {
              "order": 0,
              "actions": [
                {
                  "ruleUid": "batman-festival",
                  "weight": 2.5
                }
              ],
              "totalWeight": 2.5
            }
          }
        },
        {
          "id": "superman-returns",
          "title": "Superman Returns",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {}
        },
        {
          "id": "superman-returns",
          "title": "Superman Returns - The Prequels",
          "_federation": {
            "indexUid": "comics",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScoreDetails": {}
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 6,
      "requestUid": "[uuid]"
    }
    "###);
}
