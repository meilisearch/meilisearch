use once_cell::sync::Lazy;
use serde_json::{json, Value};

use crate::common::index::Index;
use crate::common::Server;

async fn index_with_documents<'a>(server: &'a Server, documents: &Value) -> Index<'a> {
    let index = server.index("test");

    index.add_documents(documents.clone(), None).await;
    index.wait_task(0).await;
    index
}

static SIMPLE_SEARCH_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
    {
        "title": "Shazam!",
        "desc": "a Captain Marvel ersatz",
        "id": "1",
    },
    {
        "title": "Captain Planet",
        "desc": "He's not part of the Marvel Cinematic Universe",
        "id": "2",
    },
    {
        "title": "Captain Marvel",
        "desc": "a Shazam ersatz",
        "id": "3",
    }])
});

#[actix_rt::test]
async fn simple_search_on_title() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    // simple search should return 2 documents (ids: 2 and 3).
    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["title"]}),
            |response, code| {
                assert_eq!(200, code, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
            },
        )
        .await;
}

#[actix_rt::test]
async fn simple_prefix_search_on_title() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    // simple search should return 2 documents (ids: 2 and 3).
    index
        .search(json!({"q": "Captain Mar", "attributesToSearchOn": ["title"]}), |response, code| {
            assert_eq!(200, code, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 2);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search_on_title_matching_strategy_all() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;
    // simple search matching strategy all should only return 1 document (ids: 2).
    index
        .search(json!({"q": "Captain Marvel", "attributesToSearchOn": ["title"], "matchingStrategy": "all"}), |response, code| {
            assert_eq!(200, code, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search_on_unknown_field() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;
    // simple search on unknown field shouldn't return any document.
    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["unknown"]}),
            |response, code| {
                assert_eq!(200, code, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 0);
            },
        )
        .await;
}

#[actix_rt::test]
async fn simple_search_on_no_field() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;
    // simple search on no field shouldn't return any document.
    index
        .search(json!({"q": "Captain Marvel", "attributesToSearchOn": []}), |response, code| {
            assert_eq!(200, code, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 0);
        })
        .await;
}

#[actix_rt::test]
async fn word_ranking_rule_order() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    // Document 3 should appear before document 2.
    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["title"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                assert_eq!(200, code, "{}", response);
                assert_eq!(
                    response["hits"],
                    json!([
                        {"id": "3"},
                        {"id": "2"},
                    ])
                );
            },
        )
        .await;
}

#[actix_rt::test]
async fn word_ranking_rule_order_exact_words() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;
    index.update_settings_typo_tolerance(json!({"disableOnWords": ["Captain", "Marvel"]})).await;
    index.wait_task(1).await;

    // simple search should return 2 documents (ids: 2 and 3).
    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["title"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                assert_eq!(200, code, "{}", response);
                assert_eq!(
                    response["hits"],
                    json!([
                        {"id": "3"},
                        {"id": "2"},
                    ])
                );
            },
        )
        .await;
}

#[actix_rt::test]
async fn typo_ranking_rule_order() {
    let server = Server::new().await;
    let index = index_with_documents(
        &server,
        &json!([
        {
            "title": "Capitain Marivel",
            "desc": "Captain Marvel",
            "id": "1",
        },
        {
            "title": "Captain Marivel",
            "desc": "a Shazam ersatz",
            "id": "2",
        }]),
    )
    .await;

    // Document 2 should appear before document 1.
    index
        .search(json!({"q": "Captain Marvel", "attributesToSearchOn": ["title"], "attributesToRetrieve": ["id"]}), |response, code| {
            assert_eq!(200, code, "{}", response);
            assert_eq!(
                response["hits"],
                json!([
                    {"id": "2"},
                    {"id": "1"},
                ])
            );
        })
        .await;
}

#[actix_rt::test]
async fn attributes_ranking_rule_order() {
    let server = Server::new().await;
    let index = index_with_documents(
        &server,
        &json!([
        {
            "title": "Captain Marvel",
            "desc": "a Shazam ersatz",
            "footer": "The story of Captain Marvel",
            "id": "1",
        },
        {
            "title": "The Avengers",
            "desc": "Captain Marvel is far from the earth",
            "footer": "A super hero team",
            "id": "2",
        }]),
    )
    .await;

    // Document 2 should appear before document 1.
    index
        .search(json!({"q": "Captain Marvel", "attributesToSearchOn": ["desc", "footer"], "attributesToRetrieve": ["id"]}), |response, code| {
            assert_eq!(200, code, "{}", response);
            assert_eq!(
                response["hits"],
                json!([
                    {"id": "2"},
                    {"id": "1"},
                ])
            );
        })
        .await;
}

#[actix_rt::test]
async fn exactness_ranking_rule_order() {
    let server = Server::new().await;
    let index = index_with_documents(
        &server,
        &json!([
        {
            "title": "Captain Marvel",
            "desc": "Captain Marivel",
            "id": "1",
        },
        {
            "title": "Captain Marvel",
            "desc": "CaptainMarvel",
            "id": "2",
        }]),
    )
    .await;

    // Document 2 should appear before document 1.
    index
        .search(json!({"q": "Captain Marvel", "attributesToRetrieve": ["id"], "attributesToSearchOn": ["desc"]}), |response, code| {
            assert_eq!(200, code, "{}", response);
            assert_eq!(
                response["hits"],
                json!([
                    {"id": "2"},
                    {"id": "1"},
                ])
            );
        })
        .await;
}
