use serde_json::json;

use super::DOCUMENTS;
use crate::common::Server;

#[actix_rt::test]
async fn search_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    index
        .search(json!({"q": "hello"}), |response, code| {
            assert_eq!(code, 404);
            assert_eq!(response, expected_response);
        })
        .await;
}

#[actix_rt::test]
async fn search_unexisting_parameter() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .search(json!({"marin": "hello"}), |response, code| {
            assert_eq!(code, 400, "{}", response);
            assert_eq!(response["code"], "bad_request");
        })
        .await;
}

#[actix_rt::test]
async fn search_invalid_highlight_and_crop_tags() {
    let server = Server::new().await;
    let index = server.index("test");

    let fields = &["cropMarker", "highlightPreTag", "highlightPostTag"];

    for field in fields {
        // object
        let (response, code) =
            index.search_post(json!({field.to_string(): {"marker": "<crop>"}})).await;
        assert_eq!(code, 400, "field {} passing object: {}", &field, response);
        assert_eq!(response["code"], "bad_request");

        // array
        let (response, code) =
            index.search_post(json!({field.to_string(): ["marker", "<crop>"]})).await;
        assert_eq!(code, 400, "field {} passing array: {}", &field, response);
        assert_eq!(response["code"], "bad_request");
    }
}

#[actix_rt::test]
async fn filter_invalid_syntax_object() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `title & Glass`.\n1:14 title & Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": "title & Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_syntax_array() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `title & Glass`.\n1:14 title & Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": ["title & Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_syntax_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "Found unexpected characters at the end of the filter: `XOR title = Glass`. You probably forgot an `OR` or an `AND` rule.\n15:32 title = Glass XOR title = Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": "title = Glass XOR title = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_array() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "Attribute `many` is not filterable. Available filterable attributes are: `title`.\n1:5 many = Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": ["many = Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "Attribute `many` is not filterable. Available filterable attributes are: `title`.\n1:5 many = Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": "many = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_attribute_array() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the _geoRadius(latitude, longitude, distance) built-in rule to filter on _geo field coordinates.\n1:5 _geo = Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": ["_geo = Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the _geoRadius(latitude, longitude, distance) built-in rule to filter on _geo field coordinates.\n1:5 _geo = Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": "_geo = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_attribute_array() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression.\n1:13 _geoDistance = Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": ["_geoDistance = Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression.\n1:13 _geoDistance = Glass",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": "_geoDistance = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn sort_geo_reserved_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"sortableAttributes": ["id"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a sort expression. Use the _geoPoint(latitude, longitude) built-in rule to sort on _geo field coordinates.",
        "code": "invalid_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_sort"
    });
    index
        .search(
            json!({
                "sort": ["_geo:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_reserved_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"sortableAttributes": ["id"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a sort expression.",
        "code": "invalid_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_sort"
    });
    index
        .search(
            json!({
                "sort": ["_geoDistance:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_unsortable_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"sortableAttributes": ["id"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "Attribute `title` is not sortable. Available sortable attributes are: `id`.",
        "code": "invalid_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_sort"
    });
    index
        .search(
            json!({
                "sort": ["title:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_invalid_syntax() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({"sortableAttributes": ["id"]})).await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "Invalid syntax for the sort parameter: expected expression ending by `:asc` or `:desc`, found `title`.",
        "code": "invalid_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_sort"
    });
    index
        .search(
            json!({
                "sort": ["title"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_unset_ranking_rule() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(
            json!({"sortableAttributes": ["title"], "rankingRules": ["proximity", "exactness"]}),
        )
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let expected_response = json!({
        "message": "The sort ranking rule must be specified in the ranking rules settings to use the sort parameter at search time.",
        "code": "invalid_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_sort"
    });
    index
        .search(
            json!({
                "sort": ["title:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}
