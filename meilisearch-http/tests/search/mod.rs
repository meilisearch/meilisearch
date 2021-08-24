// This modules contains all the test concerning search. Each particular feture of the search
// should be tested in its own module to isolate tests and keep the tests readable.

mod errors;

use crate::common::Server;
use once_cell::sync::Lazy;
use serde_json::{json, Value};

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "id": "287947"
        },
        {
            "title": "Captain Marvel",
            "id": "299537"
        },
        {
            "title": "Escape Room",
            "id": "522681"
        },
        { "title": "How to Train Your Dragon: The Hidden World", "id": "166428"
        },
        {
            "title": "Glass",
            "id": "450465"
        }
    ])
});

#[actix_rt::test]
async fn simple_placeholder_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(0).await;

    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 5);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(0).await;

    index
        .search(json!({"q": "glass"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[actix_rt::test]
async fn search_multiple_params() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(0).await;

    index
        .search(
            json!({
                "q": "glass",
                "attributesToCrop": ["title:2"],
                "attributesToHighlight": ["title"],
                "limit": 1,
                "offset": 0,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_filter_string_notation() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    index
        .search(
            json!({
                "filter": "title = Glass"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_filter_array_notation() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let (response, code) = index
        .search_post(json!({
            "filter": ["title = Glass"]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 1);

    let (response, code) = index
        .search_post(json!({
            "filter": [["title = Glass", "title = \"Shazam!\"", "title = \"Escape Room\""]]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 3);
}

#[actix_rt::test]
async fn search_with_sort_on_numbers() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"sortableAttributes": ["id"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    index
        .search(
            json!({
                "sort": ["id:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_sort_on_strings() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"sortableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    index
        .search(
            json!({
                "sort": ["title:desc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_multiple_sort() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"sortableAttributes": ["id", "title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let (response, code) = index
        .search_post(json!({
            "sort": ["id:asc", "title:desc"]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 5);
}

#[actix_rt::test]
async fn search_facet_distribution() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({"filterableAttributes": ["title"]}))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    index
        .search(
            json!({
                "facetsDistribution": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetsDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert!(dist.get("title").is_some());
            },
        )
        .await;
}

#[actix_rt::test]
async fn displayed_attributes() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({ "displayedAttributes": ["title"] }))
        .await;

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_update_id(1).await;

    let (response, code) = index
        .search_post(json!({ "attributesToRetrieve": ["title", "id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert!(response["hits"].get("title").is_none());
}
