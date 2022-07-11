use std::collections::HashMap;

use once_cell::sync::Lazy;
use serde_json::{json, Value};

use crate::common::Server;

static DEFAULT_SETTINGS_VALUES: Lazy<HashMap<&'static str, Value>> = Lazy::new(|| {
    let mut map = HashMap::new();
    map.insert("displayed_attributes", json!(["*"]));
    map.insert("searchable_attributes", json!(["*"]));
    map.insert("filterable_attributes", json!([]));
    map.insert("distinct_attribute", json!(Value::Null));
    map.insert(
        "ranking_rules",
        json!([
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness"
        ]),
    );
    map.insert("stop_words", json!([]));
    map.insert("synonyms", json!({}));
    map.insert(
        "faceting",
        json!({
            "maxValuesPerFacet": json!(100),
        }),
    );
    map.insert(
        "pagination",
        json!({
            "maxTotalHits": json!(1000),
        }),
    );
    map
});

#[actix_rt::test]
async fn get_settings_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.index("test").settings().await;
    assert_eq!(code, 404, "{}", response)
}

#[actix_rt::test]
async fn get_settings() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    index.wait_task(0).await;
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    let settings = response.as_object().unwrap();
    assert_eq!(settings.keys().len(), 11);
    assert_eq!(settings["displayedAttributes"], json!(["*"]));
    assert_eq!(settings["searchableAttributes"], json!(["*"]));
    assert_eq!(settings["filterableAttributes"], json!([]));
    assert_eq!(settings["sortableAttributes"], json!([]));
    assert_eq!(settings["distinctAttribute"], json!(null));
    assert_eq!(
        settings["rankingRules"],
        json!([
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness"
        ])
    );
    assert_eq!(settings["stopWords"], json!([]));
    assert_eq!(
        settings["faceting"],
        json!({
            "maxValuesPerFacet": 100,
        })
    );
    assert_eq!(
        settings["pagination"],
        json!({
            "maxTotalHits": 1000,
        })
    );
}

#[actix_rt::test]
async fn error_update_settings_unknown_field() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.update_settings(json!({"foo": 12})).await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn test_partial_update() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, _code) = index
        .update_settings(json!({"displayedAttributes": ["foo"]}))
        .await;
    index.wait_task(0).await;
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["foo"]));
    assert_eq!(response["searchableAttributes"], json!(["*"]));

    let (_response, _) = index
        .update_settings(json!({"searchableAttributes": ["bar"]}))
        .await;
    index.wait_task(1).await;

    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["foo"]));
    assert_eq!(response["searchableAttributes"], json!(["bar"]));
}

#[actix_rt::test]
async fn error_delete_settings_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.delete_settings().await;
    assert_eq!(code, 202);

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "failed");
}

#[actix_rt::test]
async fn reset_all_settings() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "id": 1,
            "name": "curqui",
            "age": 99
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    assert_eq!(response["taskUid"], 0);
    index.wait_task(0).await;

    index
        .update_settings(json!({"displayedAttributes": ["name", "age"], "searchableAttributes": ["name"], "stopWords": ["the"], "filterableAttributes": ["age"], "synonyms": {"puppy": ["dog", "doggo", "potat"] }}))
        .await;
    index.wait_task(1).await;
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["name", "age"]));
    assert_eq!(response["searchableAttributes"], json!(["name"]));
    assert_eq!(response["stopWords"], json!(["the"]));
    assert_eq!(
        response["synonyms"],
        json!({"puppy": ["dog", "doggo", "potat"] })
    );
    assert_eq!(response["filterableAttributes"], json!(["age"]));

    index.delete_settings().await;
    index.wait_task(2).await;

    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["*"]));
    assert_eq!(response["searchableAttributes"], json!(["*"]));
    assert_eq!(response["stopWords"], json!([]));
    assert_eq!(response["filterableAttributes"], json!([]));
    assert_eq!(response["synonyms"], json!({}));

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert!(response.as_object().unwrap().get("age").is_some());
}

#[actix_rt::test]
async fn update_setting_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.update_settings(json!({})).await;
    assert_eq!(code, 202);
    let response = index.wait_task(0).await;
    assert_eq!(response["status"], "succeeded");
    let (_response, code) = index.get().await;
    assert_eq!(code, 200);
    index.delete_settings().await;
    let response = index.wait_task(1).await;
    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn error_update_setting_unexisting_index_invalid_uid() {
    let server = Server::new().await;
    let index = server.index("test##!  ");
    let (response, code) = index.update_settings(json!({})).await;
    assert_eq!(code, 400);

    let expected = json!({
        "message": "invalid index uid `test##!  `, the uid must be an integer or a string containing only alphanumeric characters a-z A-Z 0-9, hyphens - and underscores _.",
        "code": "invalid_index_uid",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_index_uid"});

    assert_eq!(response, expected);
}

macro_rules! test_setting_routes {
    ($($setting:ident $write_method:ident), *) => {
        $(
            mod $setting {
                use crate::common::Server;
                use super::DEFAULT_SETTINGS_VALUES;

                #[actix_rt::test]
                async fn get_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (_response, code) = server.service.get(url).await;
                    assert_eq!(code, 404);
                }

                #[actix_rt::test]
                async fn update_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (response, code) = server.service.$write_method(url, serde_json::Value::Null).await;
                    assert_eq!(code, 202, "{}", response);
                    server.index("").wait_task(0).await;
                    let (response, code) = server.index("test").get().await;
                    assert_eq!(code, 200, "{}", response);
                }

                #[actix_rt::test]
                async fn delete_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (_, code) = server.service.delete(url).await;
                    assert_eq!(code, 202);
                    let response = server.index("").wait_task(0).await;
                    assert_eq!(response["status"], "failed");
                }

                #[actix_rt::test]
                async fn get_default() {
                    let server = Server::new().await;
                    let index = server.index("test");
                    let (response, code) = index.create(None).await;
                    assert_eq!(code, 202, "{}", response);
                    index.wait_task(0).await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (response, code) = server.service.get(url).await;
                    assert_eq!(code, 200, "{}", response);
                    let expected = DEFAULT_SETTINGS_VALUES.get(stringify!($setting)).unwrap();
                    assert_eq!(expected, &response);
                }
            }
        )*
    };
}

test_setting_routes!(
    filterable_attributes put,
    displayed_attributes put,
    searchable_attributes put,
    distinct_attribute put,
    stop_words put,
    ranking_rules put,
    synonyms put,
    pagination patch,
    faceting patch
);

#[actix_rt::test]
async fn error_set_invalid_ranking_rules() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;

    let (_response, _code) = index
        .update_settings(json!({ "rankingRules": [ "manyTheFish"]}))
        .await;
    index.wait_task(1).await;
    let (response, code) = index.get_task(1).await;

    assert_eq!(code, 200);
    assert_eq!(response["status"], "failed");

    let expected_error = json!({
        "message": r#"`manyTheFish` ranking rule is invalid. Valid ranking rules are words, typo, sort, proximity, attribute, exactness and custom ranking rules."#,
        "code": "invalid_ranking_rule",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_ranking_rule"
    });

    assert_eq!(response["error"], expected_error);
}

#[actix_rt::test]
async fn set_and_reset_distinct_attribute_with_dedicated_route() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, _code) = index.update_distinct_attribute(json!("test")).await;
    index.wait_task(0).await;

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, "test");

    index.update_distinct_attribute(json!(null)).await;

    index.wait_task(1).await;

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, json!(null));
}
