use assert_json_diff::assert_json_eq;
use serde_json::json;
use std::convert::Into;
mod common;

#[actix_rt::test]
async fn write_all_and_delete() {
    let mut server = common::Server::test_server().await;
    // 2 - Send the settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(registered)",
            "desc(age)",
        ],
        "distinctAttribute": "id",
        "searchableAttributes": [
            "id",
            "name",
            "color",
            "gender",
            "email",
            "phone",
            "address",
            "registered",
            "about"
        ],
        "displayedAttributes": [
            "name",
            "gender",
            "email",
            "registered",
            "age",
        ],
        "stopWords": [
            "ad",
            "in",
            "ut",
        ],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
        "attributesForFaceting": ["name"],
    });

    server.update_all_settings(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (response, _status_code) = server.get_all_settings().await;

    assert_json_eq!(body, response, ordered: false);

    // 4 - Delete all settings

    server.delete_all_settings().await;

    // 5 - Get all settings and check if they are set to default values

    let (response, _status_code) = server.get_all_settings().await;

    let expect = json!({
        "rankingRules": [
          "typo",
          "words",
          "proximity",
          "attribute",
          "wordsPosition",
          "exactness"
        ],
        "distinctAttribute": null,
        "searchableAttributes": ["*"],
        "displayedAttributes": ["*"],
        "stopWords": [],
        "synonyms": {},
        "attributesForFaceting": [],
    });

    assert_json_eq!(expect, response, ordered: false);
}

#[actix_rt::test]
async fn write_all_and_update() {
    let mut server = common::Server::test_server().await;

    // 2 - Send the settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(registered)",
            "desc(age)",
        ],
        "distinctAttribute": "id",
        "searchableAttributes": [
            "id",
            "name",
            "color",
            "gender",
            "email",
            "phone",
            "address",
            "registered",
            "about"
        ],
        "displayedAttributes": [
            "name",
            "gender",
            "email",
            "registered",
            "age",
        ],
        "stopWords": [
            "ad",
            "in",
            "ut",
        ],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
        "attributesForFaceting": ["name"],
    });

    server.update_all_settings(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (response, _status_code) = server.get_all_settings().await;

    assert_json_eq!(body, response, ordered: false);

    // 4 - Update all settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(age)",
        ],
        "distinctAttribute": null,
        "searchableAttributes": [
            "name",
            "color",
            "age",
        ],
        "displayedAttributes": [
            "name",
            "color",
            "age",
            "registered",
            "picture",
        ],
        "stopWords": [],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
        "attributesForFaceting": ["title"],
    });

    server.update_all_settings(body).await;

    // 5 - Get all settings and check if the content is the same of (4)

    let (response, _status_code) = server.get_all_settings().await;

    let expected = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(age)",
        ],
        "distinctAttribute": null,
        "searchableAttributes": [
            "name",
            "color",
            "age",
        ],
        "displayedAttributes": [
            "name",
            "color",
            "age",
            "registered",
            "picture",
        ],
        "stopWords": [],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
        "attributesForFaceting": ["title"],
    });

    assert_json_eq!(expected, response, ordered: false);
}

#[actix_rt::test]
async fn test_default_settings() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "uid": "test",
    });
    server.create_index(body).await;

    // 1 - Get all settings and compare to the previous one

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness"
        ],
        "distinctAttribute": null,
        "searchableAttributes": ["*"],
        "displayedAttributes": ["*"],
        "stopWords": [],
        "synonyms": {},
        "attributesForFaceting": [],
    });

    let (response, _status_code) = server.get_all_settings().await;

    assert_json_eq!(body, response, ordered: false);
}

#[actix_rt::test]
async fn test_default_settings_2() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "uid": "test",
        "primaryKey": "id",
    });
    server.create_index(body).await;

    // 1 - Get all settings and compare to the previous one

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness"
        ],
        "distinctAttribute": null,
        "searchableAttributes": ["*"],
        "displayedAttributes": ["*"],
        "stopWords": [],
        "synonyms": {},
        "attributesForFaceting": [],
    });

    let (response, _status_code) = server.get_all_settings().await;

    assert_json_eq!(body, response, ordered: false);
}

// Test issue https://github.com/meilisearch/MeiliSearch/issues/516
#[actix_rt::test]
async fn write_setting_and_update_partial() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "uid": "test",
    });
    server.create_index(body).await;

    // 2 - Send the settings

    let body = json!({
        "searchableAttributes": [
            "id",
            "name",
            "color",
            "gender",
            "email",
            "phone",
            "address",
            "about"
        ],
        "displayedAttributes": [
            "name",
            "gender",
            "email",
            "registered",
            "age",
        ]
    });

    server.update_all_settings(body.clone()).await;

    // 2 - Send the settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(age)",
            "desc(registered)",
        ],
        "distinctAttribute": "id",
        "stopWords": [
            "ad",
            "in",
            "ut",
        ],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
    });

    server.update_all_settings(body.clone()).await;

    // 2 - Send the settings

    let expected = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(age)",
            "desc(registered)",
        ],
        "distinctAttribute": "id",
        "searchableAttributes": [
            "id",
            "name",
            "color",
            "gender",
            "email",
            "phone",
            "address",
            "about"
        ],
        "displayedAttributes": [
            "name",
            "gender",
            "email",
            "registered",
            "age",
        ],
        "stopWords": [
            "ad",
            "in",
            "ut",
        ],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
        "attributesForFaceting": [],
    });

    let (response, _status_code) = server.get_all_settings().await;

    assert_json_eq!(expected, response, ordered: false);
}

#[actix_rt::test]
async fn attributes_for_faceting_settings() {
    let mut server = common::Server::test_server().await;
    // initial attributes array should be empty
    let (response, _status_code) = server.get_request("/indexes/test/settings/attributes-for-faceting").await;
    assert_eq!(response, json!([]));
    // add an attribute and test for its presence
    let (_response, _status_code) = server.post_request_async(
        "/indexes/test/settings/attributes-for-faceting",
        json!(["foobar"])).await;
    let (response, _status_code) = server.get_request("/indexes/test/settings/attributes-for-faceting").await;
    assert_eq!(response, json!(["foobar"]));
    // remove all attributes and test for emptiness
    let (_response, _status_code) = server.delete_request_async(
        "/indexes/test/settings/attributes-for-faceting").await;
    let (response, _status_code) = server.get_request("/indexes/test/settings/attributes-for-faceting").await;
    assert_eq!(response, json!([]));
}

#[actix_rt::test]
async fn setting_ranking_rules_dont_mess_with_other_settings() {
    let mut server = common::Server::test_server().await;
    let body = json!({
        "rankingRules": ["asc(foobar)"]
    });
    server.update_all_settings(body).await;
    let (response, _) = server.get_all_settings().await;
    assert_eq!(response["rankingRules"].as_array().unwrap().len(), 1);
    assert_eq!(response["rankingRules"].as_array().unwrap().first().unwrap().as_str().unwrap(), "asc(foobar)");
    assert!(!response["searchableAttributes"].as_array().unwrap().iter().any(|e| e.as_str().unwrap() == "foobar"));
    assert!(!response["displayedAttributes"].as_array().unwrap().iter().any(|e| e.as_str().unwrap() == "foobar"));
}

#[actix_rt::test]
async fn displayed_and_searchable_attributes_reset_to_wildcard() {
    let mut server = common::Server::test_server().await;
    server.update_all_settings(json!({ "searchableAttributes": ["color"], "displayedAttributes": ["color"] })).await;
    let (response, _) = server.get_all_settings().await;

    assert_eq!(response["searchableAttributes"].as_array().unwrap()[0], "color");
    assert_eq!(response["displayedAttributes"].as_array().unwrap()[0], "color");

    server.delete_searchable_attributes().await;
    server.delete_displayed_attributes().await;

    let (response, _) = server.get_all_settings().await;

    assert_eq!(response["searchableAttributes"].as_array().unwrap().len(), 1);
    assert_eq!(response["displayedAttributes"].as_array().unwrap().len(), 1);
    assert_eq!(response["searchableAttributes"].as_array().unwrap()[0], "*");
    assert_eq!(response["displayedAttributes"].as_array().unwrap()[0], "*");

    let mut server = common::Server::test_server().await;
    server.update_all_settings(json!({ "searchableAttributes": ["color"], "displayedAttributes": ["color"] })).await;
    let (response, _) = server.get_all_settings().await;
    assert_eq!(response["searchableAttributes"].as_array().unwrap()[0], "color");
    assert_eq!(response["displayedAttributes"].as_array().unwrap()[0], "color");

    server.update_all_settings(json!({ "searchableAttributes": [], "displayedAttributes": [] })).await;

    let (response, _) = server.get_all_settings().await;

    assert_eq!(response["searchableAttributes"].as_array().unwrap().len(), 1);
    assert_eq!(response["displayedAttributes"].as_array().unwrap().len(), 1);
    assert_eq!(response["searchableAttributes"].as_array().unwrap()[0], "*");
    assert_eq!(response["displayedAttributes"].as_array().unwrap()[0], "*");
}

#[actix_rt::test]
async fn settings_that_contains_wildcard_is_wildcard() {
    let mut server = common::Server::test_server().await;
    server.update_all_settings(json!({ "searchableAttributes": ["color", "*"], "displayedAttributes": ["color", "*"] })).await;

    let (response, _) = server.get_all_settings().await;

    assert_eq!(response["searchableAttributes"].as_array().unwrap().len(), 1);
    assert_eq!(response["displayedAttributes"].as_array().unwrap().len(), 1);
    assert_eq!(response["searchableAttributes"].as_array().unwrap()[0], "*");
    assert_eq!(response["displayedAttributes"].as_array().unwrap()[0], "*");
}

#[actix_rt::test]
async fn test_displayed_attributes_field() {
    let mut server = common::Server::test_server().await;

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(registered)",
            "desc(age)",
        ],
        "distinctAttribute": "id",
        "searchableAttributes": [
            "id",
            "name",
            "color",
            "gender",
            "email",
            "phone",
            "address",
            "registered",
            "about"
        ],
        "displayedAttributes": [
            "age",
            "email",
            "gender",
            "name",
            "registered",
        ],
        "stopWords": [
            "ad",
            "in",
            "ut",
        ],
        "synonyms": {
            "road": ["avenue", "street"],
            "street": ["avenue"],
        },
        "attributesForFaceting": ["name"],
    });

    server.update_all_settings(body.clone()).await;

    let (response, _status_code) = server.get_all_settings().await;

    assert_json_eq!(body, response, ordered: true);
}