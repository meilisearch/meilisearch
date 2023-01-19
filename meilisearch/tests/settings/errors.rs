use meili_snap::*;
use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn settings_bad_displayed_attributes() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "displayedAttributes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.displayedAttributes`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_displayed_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_displayed_attributes"
    }
    "###);

    let (response, code) = index.update_settings_displayed_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_displayed_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_displayed_attributes"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_searchable_attributes() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "searchableAttributes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.searchableAttributes`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_searchable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_searchable_attributes"
    }
    "###);

    let (response, code) = index.update_settings_searchable_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_searchable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_searchable_attributes"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_filterable_attributes() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "filterableAttributes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.filterableAttributes`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_filterable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_filterable_attributes"
    }
    "###);

    let (response, code) = index.update_settings_filterable_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_filterable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_filterable_attributes"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_sortable_attributes() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "sortableAttributes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.sortableAttributes`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_sortable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_sortable_attributes"
    }
    "###);

    let (response, code) = index.update_settings_sortable_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_sortable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_sortable_attributes"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_ranking_rules() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "rankingRules": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.rankingRules`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_ranking_rules"
    }
    "###);

    let (response, code) = index.update_settings_ranking_rules(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_stop_words() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "stopWords": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.stopWords`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_stop_words",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_stop_words"
    }
    "###);

    let (response, code) = index.update_settings_stop_words(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_settings_stop_words",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_stop_words"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_synonyms() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "synonyms": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.synonyms`: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_synonyms",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_synonyms"
    }
    "###);

    let (response, code) = index.update_settings_synonyms(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_synonyms",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_synonyms"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_distinct_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "distinctAttribute": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.distinctAttribute`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_settings_distinct_attribute",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_distinct_attribute"
    }
    "###);

    let (response, code) = index.update_settings_distinct_attribute(json!(["doggo"])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_settings_distinct_attribute",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_distinct_attribute"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_typo_tolerance() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "typoTolerance": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.typoTolerance`: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_typo_tolerance",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_typo_tolerance"
    }
    "###);

    let (response, code) =
        index.update_settings(json!({ "typoTolerance": { "minWordSizeForTypos": "doggo" }})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.typoTolerance.minWordSizeForTypos`: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_typo_tolerance",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_typo_tolerance"
    }
    "###);

    let (response, code) = index.update_settings_typo_tolerance(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_typo_tolerance",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_typo_tolerance"
    }
    "###);

    let (response, code) = index
        .update_settings_typo_tolerance(
            json!({ "typoTolerance": { "minWordSizeForTypos": "doggo" }}),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `typoTolerance`: expected one of `enabled`, `minWordSizeForTypos`, `disableOnWords`, `disableOnAttributes`",
      "code": "invalid_settings_typo_tolerance",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_typo_tolerance"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_faceting() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "faceting": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.faceting`: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_faceting",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_faceting"
    }
    "###);

    let (response, code) = index.update_settings_faceting(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_faceting",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_faceting"
    }
    "###);
}

#[actix_rt::test]
async fn settings_bad_pagination() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.update_settings(json!({ "pagination": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.pagination`: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_pagination"
    }
    "###);

    let (response, code) = index.update_settings_pagination(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an object, but found a string: `\"doggo\"`",
      "code": "invalid_settings_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_pagination"
    }
    "###);
}
