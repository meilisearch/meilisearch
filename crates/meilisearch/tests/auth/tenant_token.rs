use std::collections::HashMap;

use ::time::format_description::well_known::Rfc3339;
use maplit::hashmap;
use once_cell::sync::Lazy;
use time::{Duration, OffsetDateTime};

use super::authorization::{ALL_ACTIONS, AUTHORIZATIONS};
use crate::common::{Server, Value, DOCUMENTS};
use crate::json;

fn generate_tenant_token(
    parent_uid: impl AsRef<str>,
    parent_key: impl AsRef<str>,
    mut body: HashMap<&str, Value>,
) -> String {
    use jsonwebtoken::{encode, EncodingKey, Header};

    let parent_uid = parent_uid.as_ref();
    body.insert("apiKeyUid", json!(parent_uid));
    encode(&Header::default(), &body, &EncodingKey::from_secret(parent_key.as_ref().as_bytes()))
        .unwrap()
}

static INVALID_RESPONSE: Lazy<Value> = Lazy::new(|| {
    json!({
        "message": null,
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    })
});

static ACCEPTED_KEYS: Lazy<Vec<Value>> = Lazy::new(|| {
    vec![
        json!({
            "indexes": ["*"],
            "actions": ["*"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["*"],
            "actions": ["search"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["sales"],
            "actions": ["*"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["sales"],
            "actions": ["search"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["sal*", "prod*"],
            "actions": ["search"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
    ]
});

static REFUSED_KEYS: Lazy<Vec<Value>> = Lazy::new(|| {
    vec![
        // no search action
        json!({
            "indexes": ["*"],
            "actions": ALL_ACTIONS.iter().cloned().filter(|a| *a != "search" && *a != "*").collect::<Vec<_>>(),
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["sales"],
            "actions": ALL_ACTIONS.iter().cloned().filter(|a| *a != "search" && *a != "*").collect::<Vec<_>>(),
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        // bad index
        json!({
            "indexes": ["products"],
            "actions": ["*"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["prod*", "p*"],
            "actions": ["*"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["products"],
            "actions": ["search"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
    ]
});

macro_rules! compute_authorized_search {
    ($tenant_tokens:expr, $filter:expr, $expected_count:expr) => {
        let mut server = Server::new_auth().await;
        server.use_admin_key("MASTER_KEY").await;
        let index = server.index("sales");
        let documents = DOCUMENTS.clone();
        let (task1,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(task1.uid()).await.succeeded();
        let (task2,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["color"]}))
            .await;
        index.wait_task(task2.uid()).await.succeeded();
        drop(index);

        for key_content in ACCEPTED_KEYS.iter() {
            server.use_api_key("MASTER_KEY");
            let (response, code) = server.add_api_key(key_content.clone()).await;
            assert_eq!(code, 201);
            let key = response["key"].as_str().unwrap();
            let uid = response["uid"].as_str().unwrap();

            for tenant_token in $tenant_tokens.iter() {
                let web_token = generate_tenant_token(&uid, &key, tenant_token.clone());
                server.use_api_key(&web_token);
                let index = server.index("sales");
                index
                    .search(json!({ "filter": $filter }), |response, code| {
                        assert_eq!(
                            code, 200,
                            "{} using tenant_token: {:?} generated with parent_key: {:?}",
                            response, tenant_token, key_content
                        );
                        assert_eq!(
                            response["hits"].as_array().unwrap().len(),
                            $expected_count,
                            "{} using tenant_token: {:?} generated with parent_key: {:?}",
                            response,
                            tenant_token,
                            key_content
                        );
                    })
                    .await;
            }
        }
    };
}

macro_rules! compute_forbidden_search {
    ($tenant_tokens:expr, $parent_keys:expr) => {
        let mut server = Server::new_auth().await;
        server.use_admin_key("MASTER_KEY").await;
        let index = server.index("sales");
        let documents = DOCUMENTS.clone();
        let (task, _status_code) = index.add_documents(documents, None).await;
        index.wait_task(task.uid()).await.succeeded();
        drop(index);

        for key_content in $parent_keys.iter() {
            server.use_api_key("MASTER_KEY");
            let (response, code) = server.add_api_key(key_content.clone()).await;
            assert_eq!(code, 201, "{:?}", response);
            let key = response["key"].as_str().unwrap();
            let uid = response["uid"].as_str().unwrap();

            for tenant_token in $tenant_tokens.iter() {
                let web_token = generate_tenant_token(&uid, &key, tenant_token.clone());
                server.use_api_key(&web_token);
                let index = server.index("sales");
                index
                    .search(json!({}), |mut response, code| {
                        // We don't assert anything on the message since it may change between cases
                        response["message"] = serde_json::json!(null);
                        assert_eq!(
                            response,
                            INVALID_RESPONSE.clone(),
                            "{} using tenant_token: {:?} generated with parent_key: {:?}",
                            response,
                            tenant_token,
                            key_content
                        );
                        assert_eq!(
                            code, 403,
                            "{} using tenant_token: {:?} generated with parent_key: {:?}",
                            response, tenant_token, key_content
                        );
                    })
                    .await;
            }
        }
    };
}

#[actix_rt::test]
async fn search_authorized_simple_token() {
    let tenant_tokens = vec![
        hashmap! {
            "searchRules" => json!({"*": {}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["*"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sales"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"*": {}}),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!({"*": null}),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!(["*"]),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!({"sales": {}}),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!({"sales": null}),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!(["sales"]),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!(["sa*"]),
            "exp" => json!(null)
        },
    ];

    compute_authorized_search!(tenant_tokens, {}, 5);
}

#[actix_rt::test]
async fn search_authorized_filter_token() {
    let tenant_tokens = vec![
        hashmap! {
            "searchRules" => json!({"*": {"filter": "color = blue"}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {"filter": "color = blue"}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"*": {"filter": ["color = blue"]}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {"filter": ["color = blue"]}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        // filter on sales should override filters on *
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": "color = blue"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": "color = blue"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": ["color = blue"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": ["color = blue"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
    ];

    compute_authorized_search!(tenant_tokens, {}, 3);
}

#[actix_rt::test]
async fn filter_search_authorized_filter_token() {
    let tenant_tokens = vec![
        hashmap! {
            "searchRules" => json!({"*": {"filter": "color = blue"}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {"filter": "color = blue"}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"*": {"filter": ["color = blue"]}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {"filter": ["color = blue"]}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        // filter on sales should override filters on *
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": "color = blue"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": "color = blue"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": ["color = blue"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": ["color = blue"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sal*": {"filter": ["color = blue"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
    ];

    compute_authorized_search!(tenant_tokens, "color = yellow", 1);
}

/// Tests that those Tenant Token are incompatible with the REFUSED_KEYS defined above.
#[actix_rt::test]
async fn error_search_token_forbidden_parent_key() {
    let tenant_tokens = vec![
        hashmap! {
            "searchRules" => json!({"*": {}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"*": null}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["*"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": null}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sales"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sali*", "s*", "sales*"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
    ];

    compute_forbidden_search!(tenant_tokens, REFUSED_KEYS);
}

#[actix_rt::test]
async fn error_search_forbidden_token() {
    let tenant_tokens = vec![
        // bad index
        hashmap! {
            "searchRules" => json!({"products": {}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["products"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"products": {}}),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!({"products": null}),
            "exp" => json!(null)
        },
        hashmap! {
            "searchRules" => json!(["products"]),
            "exp" => json!(null)
        },
        // expired token
        hashmap! {
            "searchRules" => json!({"*": {}}),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"*": null}),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["*"]),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {}}),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": null}),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sales"]),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
    ];

    compute_forbidden_search!(tenant_tokens, ACCEPTED_KEYS);
}

#[actix_rt::test]
async fn error_access_forbidden_routes() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["*"],
        "actions": ["*"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(code, 201);
    assert!(response["key"].is_string());

    let key = response["key"].as_str().unwrap();
    let uid = response["uid"].as_str().unwrap();

    let tenant_token = hashmap! {
        "searchRules" => json!(["*"]),
        "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
    };
    let web_token = generate_tenant_token(uid, key, tenant_token);
    server.use_api_key(&web_token);

    for ((method, route), actions) in AUTHORIZATIONS.iter() {
        if !actions.contains("search") {
            let (mut response, code) = server.dummy_request(method, route).await;
            response["message"] = serde_json::json!(null);
            assert_eq!(response, INVALID_RESPONSE.clone());
            assert_eq!(code, 403);
        }
    }
}

#[actix_rt::test]
async fn error_access_expired_parent_key() {
    use std::{thread, time};
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["*"],
        "actions": ["*"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::seconds(1)).format(&Rfc3339).unwrap(),
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(code, 201);
    assert!(response["key"].is_string());

    let key = response["key"].as_str().unwrap();
    let uid = response["uid"].as_str().unwrap();

    let tenant_token = hashmap! {
        "searchRules" => json!(["*"]),
        "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
    };
    let web_token = generate_tenant_token(uid, key, tenant_token);
    server.use_api_key(&web_token);

    // test search request while parent_key is not expired
    let (mut response, code) = server.dummy_request("POST", "/indexes/products/search").await;
    response["message"] = serde_json::json!(null);
    assert_ne!(response, INVALID_RESPONSE.clone());
    assert_ne!(code, 403);

    // wait until the key is expired.
    thread::sleep(time::Duration::new(1, 0));

    let (mut response, code) = server.dummy_request("POST", "/indexes/products/search").await;
    response["message"] = serde_json::json!(null);
    assert_eq!(response, INVALID_RESPONSE.clone());
    assert_eq!(code, 403);
}

#[actix_rt::test]
async fn error_access_modified_token() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["*"],
        "actions": ["*"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(code, 201);
    assert!(response["key"].is_string());

    let key = response["key"].as_str().unwrap();
    let uid = response["uid"].as_str().unwrap();

    let tenant_token = hashmap! {
        "searchRules" => json!(["products"]),
        "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
    };
    let web_token = generate_tenant_token(uid, key, tenant_token);
    server.use_api_key(&web_token);

    // test search request while web_token is valid
    let (response, code) = server.dummy_request("POST", "/indexes/products/search").await;
    assert_ne!(response, INVALID_RESPONSE.clone());
    assert_ne!(code, 403);

    let tenant_token = hashmap! {
        "searchRules" => json!(["*"]),
        "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
    };

    let alt = generate_tenant_token(uid, key, tenant_token);
    let altered_token = [
        web_token.split('.').next().unwrap(),
        alt.split('.').nth(1).unwrap(),
        web_token.split('.').nth(2).unwrap(),
    ]
    .join(".");

    server.use_api_key(&altered_token);
    let (mut response, code) = server.dummy_request("POST", "/indexes/products/search").await;
    response["message"] = serde_json::json!(null);
    assert_eq!(response, INVALID_RESPONSE.clone());
    assert_eq!(code, 403);
}
