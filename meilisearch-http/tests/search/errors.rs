use crate::common::Server;
use serde_json::json;

use super::DOCUMENTS;

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
async fn filter_invalid_syntax_object() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "Invalid syntax for the filter parameter: ` --> 1:7\n  |\n1 | title & Glass\n  |       ^---\n  |\n  = expected word`.",
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

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "Invalid syntax for the filter parameter: ` --> 1:7\n  |\n1 | title & Glass\n  |       ^---\n  |\n  = expected word`.",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": [["title & Glass"]]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_syntax_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "Invalid syntax for the filter parameter: ` --> 1:15\n  |\n1 | title = Glass XOR title = Glass\n  |               ^---\n  |\n  = expected EOI, and, or or`.",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(
            json!({"filter": "title = Glass XOR title = Glass"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_array() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "Attribute `many` is not filterable. Available filterable attributes are: `title`.",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": [["many = Glass"]]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "Attribute `many` is not filterable. Available filterable attributes are: `title`.",
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

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the _geoRadius(latitude, longitude, distance) built-in rule to filter on _geo field coordinates.",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(json!({"filter": [["_geo = Glass"]]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the _geoRadius(latitude, longitude, distance) built-in rule to filter on _geo field coordinates.",
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

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression.",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(
            json!({"filter": [["_geoDistance = Glass"]]}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_reserved_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression.",
        "code": "invalid_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_filter"
    });
    index
        .search(
            json!({"filter": "_geoDistance = Glass"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_geo_reserved_attribute() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"sortableAttributes": ["id"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

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

    index
        .update_settings(json!({"sortableAttributes": ["id"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

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

    index
        .update_settings(json!({"sortableAttributes": ["id"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

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

    index
        .update_settings(json!({"sortableAttributes": ["id"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

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
    index.wait_update_id(1).await;

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
