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
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at `.displayedAttributes`.",
      "code": "invalid_settings_displayed_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-displayed-attributes"
    }
    "###);

    let (response, code) = index.update_settings_displayed_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at ``.",
      "code": "invalid_settings_displayed_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-displayed-attributes"
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
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at `.searchableAttributes`.",
      "code": "invalid_settings_searchable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-searchable-attributes"
    }
    "###);

    let (response, code) = index.update_settings_searchable_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at ``.",
      "code": "invalid_settings_searchable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-searchable-attributes"
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
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at `.filterableAttributes`.",
      "code": "invalid_settings_filterable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-filterable-attributes"
    }
    "###);

    let (response, code) = index.update_settings_filterable_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at ``.",
      "code": "invalid_settings_filterable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-filterable-attributes"
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
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at `.sortableAttributes`.",
      "code": "invalid_settings_sortable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-sortable-attributes"
    }
    "###);

    let (response, code) = index.update_settings_sortable_attributes(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at ``.",
      "code": "invalid_settings_sortable_attributes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-sortable-attributes"
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
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at `.rankingRules`.",
      "code": "invalid_settings_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-ranking-rules"
    }
    "###);

    let (response, code) = index.update_settings_ranking_rules(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at ``.",
      "code": "invalid_settings_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-ranking-rules"
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
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at `.stopWords`.",
      "code": "invalid_settings_stop_words",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-stop-words"
    }
    "###);

    let (response, code) = index.update_settings_stop_words(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Sequence at ``.",
      "code": "invalid_settings_stop_words",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-stop-words"
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
      "message": "invalid type: String `\"doggo\"`, expected a Map at `.synonyms`.",
      "code": "invalid_settings_synonyms",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-synonyms"
    }
    "###);

    let (response, code) = index.update_settings_synonyms(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Map at ``.",
      "code": "invalid_settings_synonyms",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-synonyms"
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
      "message": "invalid type: Sequence `[\"doggo\"]`, expected a String at `.distinctAttribute`.",
      "code": "invalid_settings_distinct_attribute",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-distinct-attribute"
    }
    "###);

    let (response, code) = index.update_settings_distinct_attribute(json!(["doggo"])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: Sequence `[\"doggo\"]`, expected a String at ``.",
      "code": "invalid_settings_distinct_attribute",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-distinct-attribute"
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
      "message": "invalid type: String `\"doggo\"`, expected a Map at `.typoTolerance`.",
      "code": "invalid_settings_typo_tolerance",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-typo-tolerance"
    }
    "###);

    let (response, code) = index.update_settings_typo_tolerance(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Map at ``.",
      "code": "invalid_settings_typo_tolerance",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-typo-tolerance"
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
      "message": "invalid type: String `\"doggo\"`, expected a Map at `.faceting`.",
      "code": "invalid_settings_faceting",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-faceting"
    }
    "###);

    let (response, code) = index.update_settings_faceting(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Map at ``.",
      "code": "invalid_settings_faceting",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-faceting"
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
      "message": "invalid type: String `\"doggo\"`, expected a Map at `.pagination`.",
      "code": "invalid_settings_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-pagination"
    }
    "###);

    let (response, code) = index.update_settings_pagination(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "invalid type: String `\"doggo\"`, expected a Map at ``.",
      "code": "invalid_settings_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-settings-pagination"
    }
    "###);
}
