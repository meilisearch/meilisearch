use std::collections::{HashMap, HashSet};

use ::time::format_description::well_known::Rfc3339;
use maplit::hashmap;
use meilisearch::Opt;
use once_cell::sync::Lazy;
use tempfile::TempDir;
use time::{Duration, OffsetDateTime};

use crate::common::{default_settings, Server, Value};
use crate::json;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum IndexScopePolicy {
    Deny,
    Allow,
}

#[allow(clippy::type_complexity)]
pub static AUTHORIZATIONS: Lazy<
    HashMap<
        (
            // method
            &'static str,
            // URL
            &'static str,
            // allow/disallow route call
            IndexScopePolicy,
        ),
        HashMap<
            // action name
            &'static str,
            // allow/disallow key creation
            IndexScopePolicy,
        >,
    >,
> = Lazy::new(|| {
    use IndexScopePolicy::*;
    let authorizations = hashmap! {
        ("POST",    "/multi-search", Allow) =>                                    hashmap!{"search" => Allow, "*" => Allow},
        ("POST",    "/indexes/products/search", Allow) =>                         hashmap!{"search" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/search", Allow) =>                         hashmap!{"search" => Allow, "*" => Allow},
        ("POST",    "/indexes/products/documents", Allow) =>                      hashmap!{"documents.add" => Allow, "documents.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/documents", Allow) =>                      hashmap!{"documents.get" => Allow, "documents.*" => Allow, "*" => Allow},
        ("POST",    "/indexes/products/documents/fetch", Allow) =>                hashmap!{"documents.get" => Allow, "documents.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/documents/0", Allow) =>                    hashmap!{"documents.get" => Allow, "documents.*" => Allow, "*" => Allow},
        ("DELETE",  "/indexes/products/documents/0", Allow) =>                    hashmap!{"documents.delete" => Allow, "documents.*" => Allow, "*" => Allow},
        ("POST",    "/indexes/products/documents/delete-batch", Allow) =>         hashmap!{"documents.delete" => Allow, "documents.*" => Allow, "*" => Allow},
        ("POST",    "/indexes/products/documents/delete", Allow) =>               hashmap!{"documents.delete" => Allow, "documents.*" => Allow, "*" => Allow},
        ("GET",     "/tasks", Allow) =>                                           hashmap!{"tasks.get" => Allow, "tasks.*" => Allow, "*" => Allow},
        ("DELETE",  "/tasks", Allow) =>                                           hashmap!{"tasks.delete" => Allow, "tasks.*" => Allow, "*" => Allow},
        ("GET",     "/tasks?indexUid=products", Allow) =>                         hashmap!{"tasks.get" => Allow, "tasks.*" => Allow, "*" => Allow},
        ("GET",     "/tasks/0", Allow) =>                                         hashmap!{"tasks.get" => Allow, "tasks.*" => Allow, "*" => Allow},
        ("POST",    "/tasks/compact", Deny) =>                                    hashmap!{"tasks.compact" => Deny, "tasks.*" => Allow, "*" => Allow},
        ("PATCH",   "/indexes/products/", Allow) =>                               hashmap!{"indexes.update" => Allow, "indexes.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/", Allow) =>                               hashmap!{"indexes.get" => Allow, "indexes.*" => Allow, "*" => Allow},
        ("DELETE",  "/indexes/products/", Allow) =>                               hashmap!{"indexes.delete" => Allow, "indexes.*" => Allow, "*" => Allow},
        ("POST",    "/indexes", Allow) =>                                         hashmap!{"indexes.create" => Allow, "indexes.*" => Allow, "*" => Allow},
        ("GET",     "/indexes", Allow) =>                                         hashmap!{"indexes.get" => Allow, "indexes.*" => Allow, "*" => Allow},
        ("POST",    "/swap-indexes", Allow) =>                                    hashmap!{"indexes.swap" => Allow, "indexes.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings", Allow) =>                       hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/displayed-attributes", Allow) =>  hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/distinct-attribute", Allow) =>    hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/filterable-attributes", Allow) => hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/ranking-rules", Allow) =>         hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/searchable-attributes", Allow) => hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/sortable-attributes", Allow) =>   hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/stop-words", Allow) =>            hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/settings/synonyms", Allow) =>              hashmap!{"settings.get" => Allow, "settings.*" => Allow, "*" => Allow},
        ("DELETE",  "/indexes/products/settings", Allow) =>                       hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PATCH",   "/indexes/products/settings", Allow) =>                       hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PATCH",   "/indexes/products/settings/typo-tolerance", Allow) =>        hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/displayed-attributes", Allow) =>  hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/distinct-attribute", Allow) =>    hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/filterable-attributes", Allow) => hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/ranking-rules", Allow) =>         hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/searchable-attributes", Allow) => hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/sortable-attributes", Allow) =>   hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/stop-words", Allow) =>            hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("PUT",     "/indexes/products/settings/synonyms", Allow) =>              hashmap!{"settings.update" => Allow, "settings.*" => Allow, "*" => Allow},
        ("GET",     "/indexes/products/stats", Allow) =>                          hashmap!{"stats.get" => Allow, "stats.*" => Allow, "*" => Allow},
        ("GET",     "/stats", Allow) =>                                           hashmap!{"stats.get" => Allow, "stats.*" => Allow, "*" => Allow},
        ("POST",    "/dumps", Deny) =>                                           hashmap!{"dumps.create" => Deny, "dumps.*" => Deny, "*" => Allow},
        ("POST",    "/snapshots", Deny) =>                                       hashmap!{"snapshots.create" => Deny, "snapshots.*" => Deny, "*" => Allow},
        ("GET",     "/version", Deny) =>                                         hashmap!{"version" => Deny, "*" => Allow},
        ("GET",     "/metrics", Deny) =>                                         hashmap!{"metrics.get" => Deny, "metrics.*" => Deny, "*" => Allow},
        ("POST",    "/logs/stream", Deny) =>                                     hashmap!{"metrics.get" => Deny, "metrics.*" => Deny, "*" => Allow},
        ("DELETE",  "/logs/stream", Deny) =>                                     hashmap!{"metrics.get" => Deny, "metrics.*" => Deny, "*" => Allow},
        ("PATCH",   "/keys/mykey/", Deny) =>                                     hashmap!{"keys.update" => Deny, "*" => Allow},
        ("GET",     "/keys/mykey/", Deny) =>                                     hashmap!{"keys.get" => Deny, "*" => Allow},
        ("DELETE",  "/keys/mykey/", Deny) =>                                     hashmap!{"keys.delete" => Deny, "*" => Allow},
        ("POST",    "/keys", Deny) =>                                            hashmap!{"keys.create" => Deny, "*" => Allow},
        ("GET",     "/keys", Deny) =>                                            hashmap!{"keys.get" => Deny, "*" => Allow},
        ("GET",     "/experimental-features", Deny) =>                           hashmap!{"experimental.get" => Deny, "*" => Allow},
        ("PATCH",   "/experimental-features", Deny) =>                           hashmap!{"experimental.update" => Deny, "*" => Allow},
        ("GET",   "/network", Deny) =>                                           hashmap!{"network.get" => Deny, "*" => Allow},
        ("PATCH",   "/network", Deny) =>                                         hashmap!{"network.update" => Deny, "*" => Allow},
    };

    authorizations
});

pub static ALL_ACTIONS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| AUTHORIZATIONS.values().flat_map(|value| value.keys()).copied().collect());

pub static ALL_INDEX_SCOPED_ACTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    AUTHORIZATIONS
        .values()
        .flat_map(|value| {
            value.iter().filter_map(|(action, key_policy)| {
                (*key_policy == IndexScopePolicy::Allow).then_some(action)
            })
        })
        .copied()
        .collect()
});

static INVALID_RESPONSE: Lazy<Value> = Lazy::new(|| {
    json!({"message": null,
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    })
});

static INVALID_ROUTE_POLICY_RESPONSE: Lazy<Value> = Lazy::new(|| {
    json!({"message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    })
});

const MASTER_KEY: &str = "MASTER_KEY";

#[actix_rt::test]
async fn error_access_expired_key() {
    use std::{thread, time};

    let mut server = Server::new_auth().await;
    server.use_api_key(MASTER_KEY);

    let content = json!({
        "indexes": ["*"],
        "actions": ALL_ACTIONS.clone(),
        "expiresAt": (OffsetDateTime::now_utc() + Duration::seconds(1)).format(&Rfc3339).unwrap(),
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    // wait until the key is expired.
    thread::sleep(time::Duration::new(1, 0));

    for (method, route, _) in AUTHORIZATIONS.keys() {
        let (mut response, code) = server.dummy_request(method, route).await;
        response["message"] = serde_json::json!(null);

        assert_eq!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_eq!(403, code, "{:?}", &response);
    }
}

#[actix_rt::test]
async fn error_access_unauthorized_index() {
    let mut server = Server::new_auth().await;
    server.use_api_key(MASTER_KEY);

    let content = json!({
        "indexes": ["sales"],
        "actions": ["*"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    for (method, route, _) in AUTHORIZATIONS
        .keys()
        // filter `products` index routes
        .filter(|(_, route, _)| route.starts_with("/indexes/products"))
    {
        let (mut response, code) = server.dummy_request(method, route).await;
        response["message"] = serde_json::json!(null);

        assert_eq!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_eq!(403, code, "{:?}", &response);
    }
}

#[actix_rt::test]
async fn error_access_unauthorized_action() {
    let mut server = Server::new_auth().await;

    for ((method, route, _), action) in AUTHORIZATIONS.iter() {
        // create a new API key letting only the needed action.
        server.use_api_key(MASTER_KEY);

        let action = action.keys().copied().collect();

        let content = json!({
            "indexes": ["*"],
            "actions": ALL_ACTIONS.difference(&action).collect::<Vec<_>>(),
            "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
        });

        let (response, code) = server.add_api_key(content).await;
        assert_eq!(201, code, "{:?}", &response);
        assert!(response["key"].is_string());

        let key = response["key"].as_str().unwrap();
        server.use_api_key(key);
        let (mut response, code) = server.dummy_request(method, route).await;
        response["message"] = serde_json::json!(null);

        assert_eq!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_eq!(403, code, "{:?}", &response);
    }
}

#[actix_rt::test]
async fn access_authorized_master_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key(MASTER_KEY);

    // master key must have access to all routes.
    for ((method, route, _), _) in AUTHORIZATIONS.iter() {
        let (response, code) = server.dummy_request(method, route).await;

        assert_ne!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_ne!(code, 403);
    }
}

#[actix_rt::test]
async fn access_authorized_restricted_index() {
    let dir = TempDir::new().unwrap();
    let enable_metrics = Opt { experimental_enable_metrics: true, ..default_settings(dir.path()) };
    let mut server = Server::new_auth_with_options(enable_metrics, dir).await;

    // check that global actions are forbidden for index-scoped api keys
    let all_actions_key = {
        // create a new API key with all actions
        server.use_api_key(MASTER_KEY);
        let content = json!({
            "indexes": ["products"],
            "actions": ["*"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
        });

        let (response, code) = server.add_api_key(content).await;
        assert_eq!(201, code, "{:?}", &response);

        assert!(response["key"].is_string());

        response["key"].as_str().unwrap().to_string()
    };

    for ((method, route, route_policy), actions) in AUTHORIZATIONS.iter() {
        for (action, key_policy) in actions {
            // create a new API key letting only the needed action.
            server.use_api_key(MASTER_KEY);

            let content = json!({
                "indexes": ["products"],
                "actions": [action],
                "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
            });

            let (response, code) = server.add_api_key(content).await;

            if matches!(key_policy, IndexScopePolicy::Allow) {
                // adding an API key is possible for this action
                assert_eq!(201, code, "{:?}", &response);
                assert!(response["key"].is_string());

                let key = response["key"].as_str().unwrap();
                server.use_api_key(key);

                let (response, code) = server.dummy_request(method, route).await;

                if matches!(route_policy, IndexScopePolicy::Allow) {
                    assert_ne!(
                        response,
                        INVALID_RESPONSE.clone(),
                        "on route: {:?} - {:?} with action: {:?}",
                        method,
                        route,
                        action
                    );
                    assert_ne!(code, 403);

                    // it is possible to call this action with the all_actions API key
                    server.use_api_key(&all_actions_key);

                    let (response, code) = server.dummy_request(method, route).await;

                    assert_ne!(
                        response,
                        INVALID_RESPONSE.clone(),
                        "on route: {:?} - {:?} with action: {:?}",
                        method,
                        route,
                        action
                    );

                    assert_ne!(code, 403);
                } else {
                    // all_actions API key also doesn't work
                    assert_eq!(
                        response,
                        INVALID_ROUTE_POLICY_RESPONSE.clone(),
                        "on route: {:?} - {:?} with action: {:?}",
                        method,
                        route,
                        action
                    );
                    assert_eq!(code, 403);

                    server.use_api_key(&all_actions_key);
                    let (response, code) = server.dummy_request(method, route).await;

                    // all_actions API key also doesn't work
                    assert_eq!(
                        response,
                        INVALID_ROUTE_POLICY_RESPONSE.clone(),
                        "on route: {:?} - {:?} with action: {:?}",
                        method,
                        route,
                        action
                    );
                    assert_eq!(code, 403);
                }
            } else {
                // cannot add key for this action
                assert_eq!(400, code, "{:?}", &response);

                // test all_actions key
                server.use_api_key(&all_actions_key);
                let (response, code) = server.dummy_request(method, route).await;
                if matches!(route_policy, IndexScopePolicy::Allow) {
                    assert_ne!(
                        response,
                        INVALID_RESPONSE.clone(),
                        "on route: {:?} - {:?} with action: {:?}",
                        method,
                        route,
                        action
                    );

                    assert_ne!(code, 403);
                } else {
                    // all_actions API key also doesn't work
                    assert_eq!(
                        response,
                        INVALID_ROUTE_POLICY_RESPONSE.clone(),
                        "on route: {:?} - {:?} with action: {:?}",
                        method,
                        route,
                        action
                    );
                    assert_eq!(code, 403);
                }
            }
        }
    }
}

#[actix_rt::test]
async fn access_authorized_no_index_restriction() {
    let mut server = Server::new_auth().await;

    for ((method, route, _), actions) in AUTHORIZATIONS.iter() {
        for action in actions.keys() {
            // create a new API key letting only the needed action.
            server.use_api_key(MASTER_KEY);

            let content = json!({
                "indexes": ["*"],
                "actions": [action],
                "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
            });

            let (response, code) = server.add_api_key(content).await;
            assert_eq!(201, code, "{:?}", &response);
            assert!(response["key"].is_string());

            let key = response["key"].as_str().unwrap();
            server.use_api_key(key);

            let (response, code) = server.dummy_request(method, route).await;

            assert_ne!(
                response,
                INVALID_RESPONSE.clone(),
                "on route: {:?} - {:?} with action: {:?}",
                method,
                route,
                action
            );
            assert_ne!(code, 403, "on route: {:?} - {:?} with action: {:?}", method, route, action);
        }
    }
}

#[actix_rt::test]
async fn access_authorized_stats_restricted_index() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    server.wait_task(task_id).await;

    // create key with access on `products` index only.
    let content = json!({
        "indexes": ["products"],
        "actions": ["stats.get"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    let (response, code) = server.stats().await;
    assert_eq!(200, code, "{:?}", &response);

    // key should have access on `products` index.
    assert!(response["indexes"].get("products").is_some());

    // key should not have access on `test` index.
    assert!(response["indexes"].get("test").is_none());
}

#[actix_rt::test]
async fn access_authorized_stats_no_index_restriction() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    server.wait_task(task_id).await;

    // create key with access on all indexes.
    let content = json!({
        "indexes": ["*"],
        "actions": ["stats.get"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    let (response, code) = server.stats().await;
    assert_eq!(200, code, "{:?}", &response);

    // key should have access on `products` index.
    assert!(response["indexes"].get("products").is_some());

    // key should have access on `test` index.
    assert!(response["indexes"].get("test").is_some());
}

#[actix_rt::test]
async fn list_authorized_indexes_restricted_index() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    server.wait_task(task_id).await;

    // create key with access on `products` index only.
    let content = json!({
        "indexes": ["products"],
        "actions": ["indexes.get"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    let (response, code) = server.list_indexes(None, None).await;
    assert_eq!(200, code, "{:?}", &response);

    let response = response["results"].as_array().unwrap();
    // key should have access on `products` index.
    assert!(response.iter().any(|index| index["uid"] == "products"));

    // key should not have access on `test` index.
    assert!(!response.iter().any(|index| index["uid"] == "test"));
}

#[actix_rt::test]
async fn list_authorized_indexes_no_index_restriction() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    server.wait_task(task_id).await;

    // create key with access on all indexes.
    let content = json!({
        "indexes": ["*"],
        "actions": ["indexes.get"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    let (response, code) = server.list_indexes(None, None).await;
    assert_eq!(200, code, "{:?}", &response);

    let response = response["results"].as_array().unwrap();
    // key should have access on `products` index.
    assert!(response.iter().any(|index| index["uid"] == "products"));

    // key should have access on `test` index.
    assert!(response.iter().any(|index| index["uid"] == "test"));
}

#[actix_rt::test]
async fn access_authorized_index_patterns() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create products_1 index
    let index_1 = server.index("products_1");
    let (response, code) = index_1.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);

    // create products index
    let index_ = server.index("products");
    let (response, code) = index_.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);

    // create key with all document access on indices with product_* pattern.
    let content = json!({
        "indexes": ["products_*"],
        "actions": ["documents.*"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });

    // Register the key
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    // refer to products_1 and products with modified api key.
    let index_1 = server.index("products_1");

    let index_ = server.index("products");

    // try to create a index via add documents route
    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    // Adding document to products_1 index. Should succeed with 202
    let (response, code) = index_1.add_documents(documents.clone(), None).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();

    // Adding document to products index. Should Fail with 403 -- invalid_api_key
    let (response, code) = index_.add_documents(documents, None).await;
    assert_eq!(403, code, "{:?}", &response);

    server.use_api_key(MASTER_KEY);

    // refer to products_1 with a modified api key.
    let index_1 = server.index("products_1");

    server.wait_task(task_id).await;

    let (response, code) = index_1.get_task(task_id).await;
    assert_eq!(200, code, "{:?}", &response);
    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn raise_error_non_authorized_index_patterns() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create products_1 index
    let product_1_index = server.index("products_1");
    let (response, code) = product_1_index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);

    // create products_2 index
    let product_2_index = server.index("products_2");
    let (response, code) = product_2_index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);

    // create test index
    let test_index = server.index("test");
    let (response, code) = test_index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);

    // create key with all document access on indices with product_* pattern.
    let content = json!({
        "indexes": ["products_*"],
        "actions": ["documents.*"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });

    // Register the key
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    // refer to products_1 and products_2 with modified api key.
    let product_1_index = server.index("products_1");
    let product_2_index = server.index("products_2");

    // refer to  test index
    let test_index = server.index("test");

    // try to create a index via add documents route
    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    // Adding document to products_1 index. Should succeed with 202
    let (response, code) = product_1_index.add_documents(documents.clone(), None).await;
    assert_eq!(202, code, "{:?}", &response);
    let task1_id = response["taskUid"].as_u64().unwrap();

    // Adding document to products_2 index. Should succeed with 202
    let (response, code) = product_2_index.add_documents(documents.clone(), None).await;
    assert_eq!(202, code, "{:?}", &response);
    let task2_id = response["taskUid"].as_u64().unwrap();

    // Adding a document to test index. Should Fail with 403 -- invalid_api_key
    let (response, code) = test_index.add_documents(documents, None).await;
    assert_eq!(403, code, "{:?}", &response);

    server.use_api_key(MASTER_KEY);

    // refer to products_1 with a modified api key.
    let product_1_index = server.index("products_1");
    // refer to products_2 with a modified api key.
    // let product_2_index = server.index("products_2");

    server.wait_task(task1_id).await;
    server.wait_task(task2_id).await;

    let (response, code) = product_1_index.get_task(task1_id).await;
    assert_eq!(200, code, "{:?}", &response);
    assert_eq!(response["status"], "succeeded");

    let (response, code) = product_1_index.get_task(task2_id).await;
    assert_eq!(200, code, "{:?}", &response);
    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn pattern_indexes() {
    // Create a server with master key
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // index.* constraints on products_* index pattern
    let content = json!({
        "indexes": ["products_*"],
        "actions": ["indexes.*"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });

    // Generate and use the api key
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    let key = response["key"].as_str().expect("Key is not string");
    server.use_api_key(key);

    // Create Index products_1 using generated api key
    let products_1 = server.index("products_1");
    let (response, code) = products_1.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);

    // Fail to create products_* using generated api key
    let products_1 = server.index("products_*");
    let (response, code) = products_1.create(Some("id")).await;
    assert_eq!(400, code, "{:?}", &response);

    // Fail to create test_1 using generated api key
    let products_1 = server.index("test_1");
    let (response, code) = products_1.create(Some("id")).await;
    assert_eq!(403, code, "{:?}", &response);
}

#[actix_rt::test]
async fn list_authorized_tasks_restricted_index() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    server.wait_task(task_id).await;

    // create key with access on `products` index only.
    let content = json!({
        "indexes": ["products"],
        "actions": ["tasks.get"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    let (response, code) = server.service.get("/tasks").await;
    assert_eq!(200, code, "{:?}", &response);
    let response = response["results"].as_array().unwrap();
    // key should have access on `products` index.
    assert!(response.iter().any(|task| task["indexUid"] == "products"));

    // key should not have access on `test` index.
    assert!(!response.iter().any(|task| task["indexUid"] == "test"));
}

#[actix_rt::test]
async fn list_authorized_tasks_no_index_restriction() {
    let mut server = Server::new_auth().await;
    server.use_admin_key(MASTER_KEY).await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    server.wait_task(task_id).await;

    // create key with access on all indexes.
    let content = json!({
        "indexes": ["*"],
        "actions": ["tasks.get"],
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    let (response, code) = server.service.get("/tasks").await;
    assert_eq!(200, code, "{:?}", &response);

    let response = response["results"].as_array().unwrap();
    // key should have access on `products` index.
    assert!(response.iter().any(|task| task["indexUid"] == "products"));

    // key should have access on `test` index.
    assert!(response.iter().any(|task| task["indexUid"] == "test"));
}

#[actix_rt::test]
async fn error_creating_index_without_action() {
    let mut server = Server::new_auth().await;
    server.use_api_key(MASTER_KEY);

    // create key with access on all indexes.
    let content = json!({
        "indexes": ["*"],
        // Give all action but the ones allowing to create an index.
        "actions": ALL_ACTIONS.iter().cloned().filter(|a| !AUTHORIZATIONS.get(&("POST","/indexes", IndexScopePolicy::Allow)).unwrap().contains_key(a)).collect::<Vec<_>>(),
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    let expected_error = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    // try to create a index via add documents route
    let index = server.index("test");
    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();

    let response = server.wait_task(task_id).await;
    assert_eq!(response["status"], "failed");
    assert_eq!(response["error"], expected_error.clone());

    // try to create a index via add settings route
    let settings = json!({ "distinctAttribute": "test"});

    let (response, code) = index.update_settings(settings).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();

    let response = server.wait_task(task_id).await;

    assert_eq!(response["status"], "failed");
    assert_eq!(response["error"], expected_error.clone());

    // try to create a index via add specialized settings route
    let (response, code) = index.update_distinct_attribute(json!("test")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();

    let response = server.wait_task(task_id).await;

    assert_eq!(response["status"], "failed");
    assert_eq!(response["error"], expected_error.clone());
}

#[actix_rt::test]
async fn lazy_create_index() {
    let mut server = Server::new_auth().await;

    // create key with access on all indexes.
    let contents = vec![
        json!({
            "indexes": ["*"],
            "actions": ["*"],
            "expiresAt": "2050-11-13T00:00:00Z"
        }),
        json!({
            "indexes": ["*"],
            "actions": ["indexes.*", "documents.*", "settings.*", "tasks.*"],
            "expiresAt": "2050-11-13T00:00:00Z"
        }),
        json!({
            "indexes": ["*"],
            "actions": ["indexes.create", "documents.add", "settings.update", "tasks.get"],
            "expiresAt": "2050-11-13T00:00:00Z"
        }),
    ];

    for content in contents {
        server.use_api_key(MASTER_KEY);
        let (response, code) = server.add_api_key(content).await;
        assert_eq!(201, code, "{:?}", &response);
        assert!(response["key"].is_string());

        // use created key.
        let key = response["key"].as_str().unwrap();
        server.use_api_key(key);

        // try to create a index via add documents route
        let index = server.index("test");
        let documents = json!([
            {
                "id": 1,
                "content": "foo",
            }
        ]);

        let (response, code) = index.add_documents(documents, None).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        server.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");

        // try to create a index via add settings route
        let index = server.index("test1");
        let settings = json!({ "distinctAttribute": "test"});

        let (response, code) = index.update_settings(settings).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        server.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");

        // try to create a index via add specialized settings route
        let index = server.index("test2");
        let (response, code) = index.update_distinct_attribute(json!("test")).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        server.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");
    }
}

#[actix_rt::test]
async fn lazy_create_index_from_pattern() {
    let mut server = Server::new_auth().await;

    // create key with access on all indexes.
    let contents = vec![
        json!({
            "indexes": ["products_*"],
            "actions": ["*"],
            "expiresAt": "2050-11-13T00:00:00Z"
        }),
        json!({
            "indexes": ["products_*"],
            "actions": ["indexes.*", "documents.*", "settings.*", "tasks.*"],
            "expiresAt": "2050-11-13T00:00:00Z"
        }),
        json!({
            "indexes": ["products_*"],
            "actions": ["indexes.create", "documents.add", "settings.update", "tasks.get"],
            "expiresAt": "2050-11-13T00:00:00Z"
        }),
    ];

    for content in contents {
        server.use_api_key(MASTER_KEY);
        let (response, code) = server.add_api_key(content).await;
        assert_eq!(201, code, "{:?}", &response);
        assert!(response["key"].is_string());

        // use created key.
        let key = response["key"].as_str().unwrap();
        server.use_api_key(key);

        // try to create a index via add documents route
        let index = server.index("products_1");
        let test = server.index("test");
        let documents = json!([
            {
                "id": 1,
                "content": "foo",
            }
        ]);

        let (response, code) = index.add_documents(documents.clone(), None).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        server.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");

        // Fail to create test index
        let (response, code) = test.add_documents(documents, None).await;
        assert_eq!(403, code, "{:?}", &response);

        // try to create a index via add settings route
        let index = server.index("products_2");
        let settings = json!({ "distinctAttribute": "test"});

        let (response, code) = index.update_settings(settings).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        server.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");

        // Fail to create test index

        let index = server.index("test");
        let settings = json!({ "distinctAttribute": "test"});

        let (response, code) = index.update_settings(settings).await;
        assert_eq!(403, code, "{:?}", &response);

        // try to create a index via add specialized settings route
        let index = server.index("products_3");
        let (response, code) = index.update_distinct_attribute(json!("test")).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        server.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");

        // Fail to create test index
        let index = server.index("test");
        let settings = json!({ "distinctAttribute": "test"});

        let (response, code) = index.update_settings(settings).await;
        assert_eq!(403, code, "{:?}", &response);
    }
}

#[actix_rt::test]
async fn error_creating_index_without_index() {
    let mut server = Server::new_auth().await;
    server.use_api_key(MASTER_KEY);

    // create key with access on all indexes.
    let content = json!({
        "indexes": ["unexpected","products_*"],
        "actions": ["*"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    // use created key.
    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    // try to create a index via add documents route
    let index = server.index("test");
    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(403, code, "{:?}", &response);

    // try to create a index via add settings route
    let index = server.index("test1");
    let settings = json!({ "distinctAttribute": "test"});
    let (response, code) = index.update_settings(settings).await;
    assert_eq!(403, code, "{:?}", &response);

    // try to create a index via add specialized settings route
    let index = server.index("test2");
    let (response, code) = index.update_distinct_attribute(json!("test")).await;
    assert_eq!(403, code, "{:?}", &response);

    // try to create a index via create index route
    let index = server.index("test3");
    let (response, code) = index.create(None).await;
    assert_eq!(403, code, "{:?}", &response);

    // try to create a index via add documents route
    let index = server.index("products");
    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(403, code, "{:?}", &response);

    // try to create a index via add settings route
    let index = server.index("products");
    let settings = json!({ "distinctAttribute": "test"});
    let (response, code) = index.update_settings(settings).await;
    assert_eq!(403, code, "{:?}", &response);

    // try to create a index via add specialized settings route
    let index = server.index("products");
    let (response, code) = index.update_distinct_attribute(json!("test")).await;
    assert_eq!(403, code, "{:?}", &response);

    // try to create a index via create index route
    let index = server.index("products");
    let (response, code) = index.create(None).await;
    assert_eq!(403, code, "{:?}", &response);
}
