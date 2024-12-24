use std::collections::HashMap;

use ::time::format_description::well_known::Rfc3339;
use maplit::hashmap;
use once_cell::sync::Lazy;
use time::{Duration, OffsetDateTime};

use super::authorization::ALL_ACTIONS;
use crate::common::{Server, Value};
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

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "id": "287947",
            "color": ["green", "blue"]
        },
        {
            "title": "Captain Marvel",
            "id": "299537",
            "color": ["yellow", "blue"]
        },
        {
            "title": "Escape Room",
            "id": "522681",
            "color": ["yellow", "red"]
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "id": "166428",
            "color": ["green", "red"]
        },
        {
            "title": "Glass",
            "id": "450465",
            "color": ["blue", "red"]
        }
    ])
});

static NESTED_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "id": 852,
            "father": "jean",
            "mother": "michelle",
            "doggos": [
                {
                    "name": "bobby",
                    "age": 2,
                },
                {
                    "name": "buddy",
                    "age": 4,
                },
            ],
            "cattos": "pesti",
        },
        {
            "id": 654,
            "father": "pierre",
            "mother": "sabine",
            "doggos": [
                {
                    "name": "gros bill",
                    "age": 8,
                },
            ],
            "cattos": ["simba", "pestiféré"],
        },
        {
            "id": 750,
            "father": "romain",
            "mother": "michelle",
            "cattos": ["enigma"],
        },
        {
            "id": 951,
            "father": "jean-baptiste",
            "mother": "sophie",
            "doggos": [
                {
                    "name": "turbo",
                    "age": 5,
                },
                {
                    "name": "fast",
                    "age": 6,
                },
            ],
            "cattos": ["moumoute", "gomez"],
        },
    ])
});

fn invalid_response(query_index: Option<usize>) -> Value {
    let message = if let Some(query_index) = query_index {
        json!(format!("Inside `.queries[{query_index}]`: The provided API key is invalid."))
    } else {
        // if it's anything else we simply return null and will tests all the
        // error messages somewhere else
        json!(null)
    };
    json!({"message": message,
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    })
}

static ACCEPTED_KEYS_SINGLE: Lazy<Vec<Value>> = Lazy::new(|| {
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

static ACCEPTED_KEYS_BOTH: Lazy<Vec<Value>> = Lazy::new(|| {
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
            "indexes": ["sales", "products"],
            "actions": ["*"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["sales", "products"],
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

static SINGLE_REFUSED_KEYS: Lazy<Vec<Value>> = Lazy::new(|| {
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

static BOTH_REFUSED_KEYS: Lazy<Vec<Value>> = Lazy::new(|| {
    vec![
        // no search action
        json!({
            "indexes": ["*"],
            "actions": ALL_ACTIONS.iter().cloned().filter(|a| *a != "search" && *a != "*").collect::<Vec<_>>(),
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        json!({
            "indexes": ["sales", "products"],
            "actions": ALL_ACTIONS.iter().cloned().filter(|a| *a != "search" && *a != "*").collect::<Vec<_>>(),
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
        // bad index
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
            "indexes": ["sal*", "proa*"],
            "actions": ["search"],
            "expiresAt": (OffsetDateTime::now_utc() + Duration::days(1)).format(&Rfc3339).unwrap()
        }),
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

macro_rules! compute_authorized_single_search {
    ($tenant_tokens:expr, $filter:expr, $expected_count:expr) => {
        let mut server = Server::new_auth().await;
        server.use_admin_key("MASTER_KEY").await;
        let index = server.index("sales");
        let documents = DOCUMENTS.clone();
        let (add_task,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(add_task.uid()).await.succeeded();
        let (update_task,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["color"]}))
            .await;
        index.wait_task(update_task.uid()).await.succeeded();
        drop(index);

        let index = server.index("products");
        let documents = NESTED_DOCUMENTS.clone();
        let (add_task2,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(add_task2.uid()).await.succeeded();
        let (update_task2,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["doggos"]}))
            .await;
        index.wait_task(update_task2.uid()).await.succeeded();
        drop(index);


        for key_content in ACCEPTED_KEYS_SINGLE.iter().chain(ACCEPTED_KEYS_BOTH.iter()) {
            server.use_api_key("MASTER_KEY");
            let (response, code) = server.add_api_key(key_content.clone()).await;
            assert_eq!(code, 201);
            let key = response["key"].as_str().unwrap();
            let uid = response["uid"].as_str().unwrap();

            for tenant_token in $tenant_tokens.iter() {
                let web_token = generate_tenant_token(&uid, &key, tenant_token.clone());
                server.use_api_key(&web_token);
                let (response, code) = server.multi_search(json!({"queries" : [{"indexUid": "sales", "filter": $filter}]})).await;
                assert_eq!(
                    200, code,
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response, tenant_token, key_content
                );
                assert_eq!(
                    $expected_count,
                    response["results"][0]["hits"].as_array().unwrap().len(),
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response,
                    tenant_token,
                    key_content
                );

                // federated
                let (response, code) = server.multi_search(json!({"federation": {}, "queries" : [{"indexUid": "sales", "filter": $filter}]})).await;
                assert_eq!(
                    200, code,
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response, tenant_token, key_content
                );
                assert_eq!(
                    // same count as the search is federated over a single query
                    $expected_count,
                    response["hits"].as_array().unwrap().len(),
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response,
                    tenant_token,
                    key_content
                );
            }
        }
    };
}

macro_rules! compute_authorized_multiple_search {
    ($tenant_tokens:expr, $filter1:expr, $filter2:expr, $expected_count1:expr, $expected_count2:expr) => {
        let mut server = Server::new_auth().await;
        server.use_admin_key("MASTER_KEY").await;
        let index = server.index("sales");
        let documents = DOCUMENTS.clone();
        let (task,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(task.uid()).await.succeeded();
        let (task,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["color"]}))
            .await;
        index.wait_task(task.uid()).await.succeeded();
        drop(index);

        let index = server.index("products");
        let documents = NESTED_DOCUMENTS.clone();
        let (task,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(task.uid()).await.succeeded();
        let (task,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["doggos"]}))
            .await;
        index.wait_task(task.uid()).await.succeeded();
        drop(index);


        for key_content in ACCEPTED_KEYS_BOTH.iter() {
            server.use_api_key("MASTER_KEY");
            let (response, code) = server.add_api_key(key_content.clone()).await;
            assert_eq!(code, 201);
            let key = response["key"].as_str().unwrap();
            let uid = response["uid"].as_str().unwrap();

            for tenant_token in $tenant_tokens.iter() {
                let web_token = generate_tenant_token(&uid, &key, tenant_token.clone());
                server.use_api_key(&web_token);
                let (response, code) = server.multi_search(json!({"queries" : [
                    {"indexUid": "sales", "filter": $filter1},
                    {"indexUid": "products", "filter": $filter2},
                ]})).await;
                assert_eq!(
                    code, 200,
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response, tenant_token, key_content
                );
                assert_eq!(
                    response["results"][0]["hits"].as_array().unwrap().len(),
                    $expected_count1,
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response,
                    tenant_token,
                    key_content
                );
                assert_eq!(
                    response["results"][1]["hits"].as_array().unwrap().len(),
                    $expected_count2,
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response,
                    tenant_token,
                    key_content
                );

                let (response, code) = server.multi_search(json!({"federation": {}, "queries" : [
                    {"indexUid": "sales", "filter": $filter1},
                    {"indexUid": "products", "filter": $filter2},
                ]})).await;
                assert_eq!(
                    code, 200,
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response, tenant_token, key_content
                );
                assert_eq!(
                    response["hits"].as_array().unwrap().len(),
                    // sum of counts as the search is federated across to queries in different indexes
                    $expected_count1 + $expected_count2,
                    "{} using tenant_token: {:?} generated with parent_key: {:?}",
                    response,
                    tenant_token,
                    key_content
                );
            }
        }
    };
}

macro_rules! compute_forbidden_single_search {
    ($tenant_tokens:expr, $parent_keys:expr, $failed_query_indexes:expr) => {
        let mut server = Server::new_auth().await;
        server.use_admin_key("MASTER_KEY").await;
        let index = server.index("sales");
        let documents = DOCUMENTS.clone();
        let (task,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(task.uid()).await.succeeded();
        let (task,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["color"]}))
            .await;
        index.wait_task(task.uid()).await.succeeded();
        drop(index);

        let index = server.index("products");
        let documents = NESTED_DOCUMENTS.clone();
        let (task,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(task.uid()).await.succeeded();
        let (task,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["doggos"]}))
            .await;
        index.wait_task(task.uid()).await.succeeded();
        drop(index);

        assert_eq!($parent_keys.len(), $failed_query_indexes.len(), "keys != query_indexes");
        for (key_content, failed_query_indexes) in $parent_keys.iter().zip($failed_query_indexes.into_iter()) {
            server.use_api_key("MASTER_KEY");
            let (response, code) = server.add_api_key(key_content.clone()).await;
            assert_eq!(code, 201, "{:?}", response);
            let key = response["key"].as_str().unwrap();
            let uid = response["uid"].as_str().unwrap();

            assert_eq!($tenant_tokens.len(), failed_query_indexes.len(), "tenant_tokens != query_indexes");
            for (tenant_token, failed_query_index) in $tenant_tokens.iter().zip(failed_query_indexes.into_iter()) {
                let web_token = generate_tenant_token(&uid, &key, tenant_token.clone());
                server.use_api_key(&web_token);
                let (mut response, code) = server.multi_search(json!({"queries" : [{"indexUid": "sales"}]})).await;
                if failed_query_index.is_none() && !response["message"].is_null() {
                    response["message"] = serde_json::json!(null);
                }
                assert_eq!(
                    response,
                    invalid_response(failed_query_index),
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

                let (mut response, code) = server.multi_search(json!({"federation": {}, "queries" : [{"indexUid": "sales"}]})).await;
                if failed_query_index.is_none() && !response["message"].is_null() {
                    response["message"] = serde_json::json!(null);
                }
                assert_eq!(
                    response,
                    invalid_response(failed_query_index),
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
            }
        }
    };
}

macro_rules! compute_forbidden_multiple_search {
    ($tenant_tokens:expr, $parent_keys:expr, $failed_query_indexes:expr) => {
        let mut server = Server::new_auth().await;
        server.use_admin_key("MASTER_KEY").await;
        let index = server.index("sales");
        let documents = DOCUMENTS.clone();
        let (task,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(task.uid()).await.succeeded();
        let (task,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["color"]}))
            .await;
        index.wait_task(task.uid()).await.succeeded();
        drop(index);

        let index = server.index("products");
        let documents = NESTED_DOCUMENTS.clone();
        let (task,_status_code) = index.add_documents(documents, None).await;
        index.wait_task(task.uid()).await.succeeded();
        let (task,_status_code) = index
            .update_settings(json!({"filterableAttributes": ["doggos"]}))
            .await;
        index.wait_task(task.uid()).await.succeeded();
        drop(index);

        assert_eq!($parent_keys.len(), $failed_query_indexes.len(), "keys != query_indexes");
        for (key_content, failed_query_indexes) in $parent_keys.iter().zip($failed_query_indexes.into_iter()) {
            server.use_api_key("MASTER_KEY");
            let (response, code) = server.add_api_key(key_content.clone()).await;
            assert_eq!(code, 201, "{:?}", response);
            let key = response["key"].as_str().unwrap();
            let uid = response["uid"].as_str().unwrap();

            assert_eq!($tenant_tokens.len(), failed_query_indexes.len(), "tenant_token != query_indexes");
            for (tenant_token, failed_query_index) in $tenant_tokens.iter().zip(failed_query_indexes.into_iter()) {
                let web_token = generate_tenant_token(&uid, &key, tenant_token.clone());
                server.use_api_key(&web_token);
                let (mut response, code) = server.multi_search(json!({"queries" : [
                    {"indexUid": "sales"},
                    {"indexUid": "products"},
                ]})).await;
                if failed_query_index.is_none() && !response["message"].is_null() {
                    response["message"] = serde_json::json!(null);
                }
                assert_eq!(
                    response,
                    invalid_response(failed_query_index),
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

                let (mut response, code) = server.multi_search(json!({"federation": {}, "queries" : [
                    {"indexUid": "sales"},
                    {"indexUid": "products"},
                ]})).await;
                if failed_query_index.is_none() && !response["message"].is_null() {
                    response["message"] = serde_json::json!(null);
                }
                assert_eq!(
                    response,
                    invalid_response(failed_query_index),
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
            }
        }
    };
}

#[actix_rt::test]
async fn single_search_authorized_simple_token() {
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
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"*": null}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["*"]),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"sales": {}}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"sales": null}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["sales"]),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["sa*"]),
            "exp" => json!(null),
        },
    ];

    compute_authorized_single_search!(tenant_tokens, {}, 5);
}

#[actix_rt::test]
async fn multi_search_authorized_simple_token() {
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
            "searchRules" => json!({"sales": {}, "products": {}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sales", "products"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"*": {}}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"*": null}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["*"]),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"sales": {}, "products": {}}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"sales": null, "products": null}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["sales", "products"]),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["sa*", "pro*"]),
            "exp" => json!(null),
        },
    ];

    compute_authorized_multiple_search!(tenant_tokens, {}, {}, 5, 4);
}

#[actix_rt::test]
async fn single_search_authorized_filter_token() {
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

    compute_authorized_single_search!(tenant_tokens, {}, 3);
}

#[actix_rt::test]
async fn multi_search_authorized_filter_token() {
    let both_tenant_tokens = vec![
        hashmap! {
            "searchRules" => json!({"sales": {"filter": "color = blue"}, "products": {"filter": "doggos.age <= 5"}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {"filter": ["color = blue"]}, "products": {"filter": "doggos.age <= 5"}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        // filter on sales should override filters on *
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": "color = blue"},
                "products": {"filter": "doggos.age <= 5"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": "color = blue"},
                "products": {"filter": "doggos.age <= 5"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": ["color = blue"]},
                "products": {"filter": ["doggos.age <= 5"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": ["color = blue"]},
                "products": {"filter": ["doggos.age <= 5"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
    ];

    compute_authorized_multiple_search!(both_tenant_tokens, {}, {}, 3, 2);
}

#[actix_rt::test]
async fn filter_single_search_authorized_filter_token() {
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

    compute_authorized_single_search!(tenant_tokens, "color = yellow", 1);
}

#[actix_rt::test]
async fn filter_multi_search_authorized_filter_token() {
    let tenant_tokens = vec![
        hashmap! {
            "searchRules" => json!({"sales": {"filter": "color = blue"}, "products": {"filter": "doggos.age <= 5"}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": {"filter": ["color = blue"]}, "products": {"filter": ["doggos.age <= 5"]}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        // filter on sales should override filters on *
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": "color = blue"},
                "products": {"filter": "doggos.age <= 5"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": "color = blue"},
                "products": {"filter": "doggos.age <= 5"}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {"filter": "color = green"},
                "sales": {"filter": ["color = blue"]},
                "products": {"filter": ["doggos.age <= 5"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sales": {"filter": ["color = blue"]},
                "products": {"filter": ["doggos.age <= 5"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({
                "*": {},
                "sal*": {"filter": ["color = blue"]},
                "pro*": {"filter": ["doggos.age <= 5"]}
            }),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
    ];

    compute_authorized_multiple_search!(tenant_tokens, "color = yellow", "doggos.age > 4", 1, 1);
}

/// Tests that those Tenant Token are incompatible with the REFUSED_KEYS defined above.
#[actix_rt::test]
async fn error_single_search_token_forbidden_parent_key() {
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

    compute_forbidden_single_search!(
        tenant_tokens,
        SINGLE_REFUSED_KEYS,
        vec![vec![None; 7], vec![None; 7], vec![Some(0); 7], vec![Some(0); 7], vec![Some(0); 7]]
    );
}

/// Tests that those Tenant Token are incompatible with the REFUSED_KEYS defined above.
#[actix_rt::test]
async fn error_multi_search_token_forbidden_parent_key() {
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
            "searchRules" => json!({"sales": {}, "products": {}}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": null, "products": null}),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sales", "products"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sali*", "s*", "sales*", "pro*", "proa*", "products*"]),
            "exp" => json!((OffsetDateTime::now_utc() + Duration::hours(1)).unix_timestamp())
        },
    ];

    compute_forbidden_multiple_search!(
        tenant_tokens,
        BOTH_REFUSED_KEYS,
        vec![
            vec![None; 7],
            vec![None; 7],
            vec![Some(1); 7],
            vec![Some(1); 7],
            vec![Some(1); 7],
            vec![Some(0); 7],
            vec![Some(0); 7],
            vec![Some(0); 7]
        ]
    );
}

#[actix_rt::test]
async fn error_single_search_forbidden_token() {
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
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"products": null}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["products"]),
            "exp" => json!(null),
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

    let failed_query_indexes: Vec<_> =
        std::iter::repeat(Some(0)).take(5).chain(std::iter::repeat(None).take(6)).collect();

    let failed_query_indexes = vec![failed_query_indexes; ACCEPTED_KEYS_SINGLE.len()];

    compute_forbidden_single_search!(tenant_tokens, ACCEPTED_KEYS_SINGLE, failed_query_indexes);
}

#[actix_rt::test]
async fn error_multi_search_forbidden_token() {
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
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"products": null}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["products"]),
            "exp" => json!(null),
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
            "searchRules" => json!({"sales": {}}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!({"sales": null}),
            "exp" => json!(null),
        },
        hashmap! {
            "searchRules" => json!(["sales"]),
            "exp" => json!(null),
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
            "searchRules" => json!({"sales": {}, "products": {}}),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!({"sales": null, "products": {}}),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
        hashmap! {
            "searchRules" => json!(["sales", "products"]),
            "exp" => json!((OffsetDateTime::now_utc() - Duration::hours(1)).unix_timestamp())
        },
    ];

    let failed_query_indexes: Vec<_> = std::iter::repeat(Some(0))
        .take(5)
        .chain(std::iter::repeat(Some(1)).take(5))
        .chain(std::iter::repeat(None).take(6))
        .collect();

    let failed_query_indexes = vec![failed_query_indexes; ACCEPTED_KEYS_BOTH.len()];

    compute_forbidden_multiple_search!(tenant_tokens, ACCEPTED_KEYS_BOTH, failed_query_indexes);
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
    let (mut response, code) = server
        .multi_search(json!({"queries" : [{"indexUid": "sales"}, {"indexUid": "products"}]}))
        .await;
    response["message"] = serde_json::json!(null);
    assert_ne!(response, invalid_response(None));
    assert_ne!(code, 403);

    // wait until the key is expired.
    thread::sleep(time::Duration::new(1, 0));

    let (mut response, code) = server
        .multi_search(json!({"queries" : [{"indexUid": "sales"}, {"indexUid": "products"}]}))
        .await;
    response["message"] = serde_json::json!(null);
    assert_eq!(response, invalid_response(None));
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
    let (response, code) =
        server.multi_search(json!({"queries" : [{"indexUid": "products"}]})).await;
    assert_ne!(response, invalid_response(Some(0)));
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
    let (mut response, code) =
        server.multi_search(json!({"queries" : [{"indexUid": "products"}]})).await;
    response["message"] = serde_json::json!(null);
    assert_eq!(response, invalid_response(None));
    assert_eq!(code, 403);
}
