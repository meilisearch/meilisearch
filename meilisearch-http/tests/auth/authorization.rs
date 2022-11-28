use std::collections::{HashMap, HashSet};

use ::time::format_description::well_known::Rfc3339;
use maplit::{hashmap, hashset};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use time::{Duration, OffsetDateTime};

use crate::common::Server;

pub static AUTHORIZATIONS: Lazy<HashMap<(&'static str, &'static str), HashSet<&'static str>>> =
    Lazy::new(|| {
        let mut authorizations = hashmap! {
            ("POST",    "/indexes/products/search") =>                         hashset!{"search", "*"},
            ("GET",     "/indexes/products/search") =>                         hashset!{"search", "*"},
            ("POST",    "/indexes/products/documents") =>                      hashset!{"documents.add", "documents.*", "*"},
            ("GET",     "/indexes/products/documents") =>                      hashset!{"documents.get", "documents.*", "*"},
            ("GET",     "/indexes/products/documents/0") =>                    hashset!{"documents.get", "documents.*", "*"},
            ("DELETE",  "/indexes/products/documents/0") =>                    hashset!{"documents.delete", "documents.*", "*"},
            ("GET",     "/tasks") =>                                           hashset!{"tasks.get", "tasks.*", "*"},
            ("DELETE",  "/tasks") =>                                           hashset!{"tasks.delete", "tasks.*", "*"},
            ("GET",     "/tasks?indexUid=products") =>                         hashset!{"tasks.get", "tasks.*", "*"},
            ("GET",     "/tasks/0") =>                                         hashset!{"tasks.get", "tasks.*", "*"},
            ("PATCH",   "/indexes/products/") =>                               hashset!{"indexes.update", "indexes.*", "*"},
            ("GET",     "/indexes/products/") =>                               hashset!{"indexes.get", "indexes.*", "*"},
            ("DELETE",  "/indexes/products/") =>                               hashset!{"indexes.delete", "indexes.*", "*"},
            ("POST",    "/indexes") =>                                         hashset!{"indexes.create", "indexes.*", "*"},
            ("GET",     "/indexes") =>                                         hashset!{"indexes.get", "indexes.*", "*"},
            ("POST",    "/swap-indexes") =>                                    hashset!{"indexes.swap", "indexes.*", "*"},
            ("GET",     "/indexes/products/settings") =>                       hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/displayed-attributes") =>  hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/distinct-attribute") =>    hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/filterable-attributes") => hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/ranking-rules") =>         hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/searchable-attributes") => hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/sortable-attributes") =>   hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/stop-words") =>            hashset!{"settings.get", "settings.*", "*"},
            ("GET",     "/indexes/products/settings/synonyms") =>              hashset!{"settings.get", "settings.*", "*"},
            ("DELETE",  "/indexes/products/settings") =>                       hashset!{"settings.update", "settings.*", "*"},
            ("PATCH",   "/indexes/products/settings") =>                       hashset!{"settings.update", "settings.*", "*"},
            ("PATCH",   "/indexes/products/settings/typo-tolerance") =>        hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/displayed-attributes") =>  hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/distinct-attribute") =>    hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/filterable-attributes") => hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/ranking-rules") =>         hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/searchable-attributes") => hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/sortable-attributes") =>   hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/stop-words") =>            hashset!{"settings.update", "settings.*", "*"},
            ("PUT",     "/indexes/products/settings/synonyms") =>              hashset!{"settings.update", "settings.*", "*"},
            ("GET",     "/indexes/products/stats") =>                          hashset!{"stats.get", "stats.*", "*"},
            ("GET",     "/stats") =>                                           hashset!{"stats.get", "stats.*", "*"},
            ("POST",    "/dumps") =>                                           hashset!{"dumps.create", "dumps.*", "*"},
            ("GET",     "/version") =>                                         hashset!{"version", "*"},
            ("PATCH",   "/keys/mykey/") =>                                     hashset!{"keys.update", "*"},
            ("GET",     "/keys/mykey/") =>                                     hashset!{"keys.get", "*"},
            ("DELETE",  "/keys/mykey/") =>                                     hashset!{"keys.delete", "*"},
            ("POST",    "/keys") =>                                            hashset!{"keys.create", "*"},
            ("GET",     "/keys") =>                                            hashset!{"keys.get", "*"},
        };

        if cfg!(feature = "metrics") {
            authorizations.insert(("GET", "/metrics"), hashset! {"metrics.get", "metrics.*", "*"});
        }

        authorizations
    });

pub static ALL_ACTIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    AUTHORIZATIONS.values().cloned().reduce(|l, r| l.union(&r).cloned().collect()).unwrap()
});

static INVALID_RESPONSE: Lazy<Value> = Lazy::new(|| {
    json!({"message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    })
});

#[actix_rt::test]
async fn error_access_expired_key() {
    use std::{thread, time};

    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["products"],
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

    for (method, route) in AUTHORIZATIONS.keys() {
        let (response, code) = server.dummy_request(method, route).await;

        assert_eq!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_eq!(403, code, "{:?}", &response);
    }
}

#[actix_rt::test]
async fn error_access_unauthorized_index() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["sales"],
        "actions": ALL_ACTIONS.clone(),
        "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    let key = response["key"].as_str().unwrap();
    server.use_api_key(key);

    for (method, route) in AUTHORIZATIONS
        .keys()
        // filter `products` index routes
        .filter(|(_, route)| route.starts_with("/indexes/products"))
    {
        let (response, code) = server.dummy_request(method, route).await;

        assert_eq!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_eq!(403, code, "{:?}", &response);
    }
}

#[actix_rt::test]
async fn error_access_unauthorized_action() {
    let mut server = Server::new_auth().await;

    for ((method, route), action) in AUTHORIZATIONS.iter() {
        // create a new API key letting only the needed action.
        server.use_api_key("MASTER_KEY");

        let content = json!({
            "indexes": ["products"],
            "actions": ALL_ACTIONS.difference(action).collect::<Vec<_>>(),
            "expiresAt": (OffsetDateTime::now_utc() + Duration::hours(1)).format(&Rfc3339).unwrap(),
        });

        let (response, code) = server.add_api_key(content).await;
        assert_eq!(201, code, "{:?}", &response);
        assert!(response["key"].is_string());

        let key = response["key"].as_str().unwrap();
        server.use_api_key(key);
        let (response, code) = server.dummy_request(method, route).await;

        assert_eq!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_eq!(403, code, "{:?}", &response);
    }
}

#[actix_rt::test]
async fn access_authorized_master_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    // master key must have access to all routes.
    for ((method, route), _) in AUTHORIZATIONS.iter() {
        let (response, code) = server.dummy_request(method, route).await;

        assert_ne!(response, INVALID_RESPONSE.clone(), "on route: {:?} - {:?}", method, route);
        assert_ne!(code, 403);
    }
}

#[actix_rt::test]
async fn access_authorized_restricted_index() {
    let mut server = Server::new_auth().await;
    for ((method, route), actions) in AUTHORIZATIONS.iter() {
        for action in actions {
            // create a new API key letting only the needed action.
            server.use_api_key("MASTER_KEY");

            let content = json!({
                "indexes": ["products"],
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
            assert_ne!(code, 403);
        }
    }
}

#[actix_rt::test]
async fn access_authorized_no_index_restriction() {
    let mut server = Server::new_auth().await;

    for ((method, route), actions) in AUTHORIZATIONS.iter() {
        for action in actions {
            // create a new API key letting only the needed action.
            server.use_api_key("MASTER_KEY");

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
            assert_ne!(code, 403);
        }
    }
}

#[actix_rt::test]
async fn access_authorized_stats_restricted_index() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    index.wait_task(task_id).await;

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
    server.use_admin_key("MASTER_KEY").await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    index.wait_task(task_id).await;

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
    server.use_admin_key("MASTER_KEY").await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    index.wait_task(task_id).await;

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
    server.use_admin_key("MASTER_KEY").await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    index.wait_task(task_id).await;

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
async fn list_authorized_tasks_restricted_index() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    index.wait_task(task_id).await;

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
    println!("{}", response);
    let response = response["results"].as_array().unwrap();
    // key should have access on `products` index.
    assert!(response.iter().any(|task| task["indexUid"] == "products"));

    // key should not have access on `test` index.
    assert!(!response.iter().any(|task| task["indexUid"] == "test"));
}

#[actix_rt::test]
async fn list_authorized_tasks_no_index_restriction() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // create index `test`
    let index = server.index("test");
    let (response, code) = index.create(Some("id")).await;
    assert_eq!(202, code, "{:?}", &response);
    // create index `products`
    let index = server.index("products");
    let (response, code) = index.create(Some("product_id")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();
    index.wait_task(task_id).await;

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
    server.use_api_key("MASTER_KEY");

    // create key with access on all indexes.
    let content = json!({
        "indexes": ["*"],
        // Give all action but the ones allowing to create an index.
        "actions": ALL_ACTIONS.iter().cloned().filter(|a| !AUTHORIZATIONS.get(&("POST","/indexes")).unwrap().contains(a)).collect::<Vec<_>>(),
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

    let response = index.wait_task(task_id).await;
    assert_eq!(response["status"], "failed");
    assert_eq!(response["error"], expected_error.clone());

    // try to create a index via add settings route
    let settings = json!({ "distinctAttribute": "test"});

    let (response, code) = index.update_settings(settings).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();

    let response = index.wait_task(task_id).await;

    assert_eq!(response["status"], "failed");
    assert_eq!(response["error"], expected_error.clone());

    // try to create a index via add specialized settings route
    let (response, code) = index.update_distinct_attribute(json!("test")).await;
    assert_eq!(202, code, "{:?}", &response);
    let task_id = response["taskUid"].as_u64().unwrap();

    let response = index.wait_task(task_id).await;

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
        server.use_api_key("MASTER_KEY");
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

        index.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");

        // try to create a index via add settings route
        let index = server.index("test1");
        let settings = json!({ "distinctAttribute": "test"});

        let (response, code) = index.update_settings(settings).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        index.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");

        // try to create a index via add specialized settings route
        let index = server.index("test2");
        let (response, code) = index.update_distinct_attribute(json!("test")).await;
        assert_eq!(202, code, "{:?}", &response);
        let task_id = response["taskUid"].as_u64().unwrap();

        index.wait_task(task_id).await;

        let (response, code) = index.get_task(task_id).await;
        assert_eq!(200, code, "{:?}", &response);
        assert_eq!(response["status"], "succeeded");
    }
}

#[actix_rt::test]
async fn error_creating_index_without_index() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    // create key with access on all indexes.
    let content = json!({
        "indexes": ["unexpected"],
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
}
