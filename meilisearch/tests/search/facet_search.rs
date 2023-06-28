use once_cell::sync::Lazy;
use serde_json::{json, Value};

use crate::common::Server;

pub(self) static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "genres": ["Action", "Adventure"],
            "id": "287947",
        },
        {
            "title": "Captain Marvel",
            "genres": ["Action", "Adventure"],
            "id": "299537",
        },
        {
            "title": "Escape Room",
            "genres": ["Horror", "Thriller", "Multiple Words"],
            "id": "522681",
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "genres": ["Action", "Comedy"],
            "id": "166428",
        },
        {
            "title": "Gläss",
            "genres": ["Thriller"],
            "id": "450465",
        }
    ])
});

#[actix_rt::test]
async fn simple_facet_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 200, "{}", response);
    assert_eq!(dbg!(response)["facetHits"].as_array().unwrap().len(), 2);

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "adventure"})).await;

    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 1);
}

#[actix_rt::test]
async fn non_filterable_facet_search_error() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;
    assert_eq!(code, 400, "{}", response);

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "adv"})).await;
    assert_eq!(code, 400, "{}", response);
}

#[actix_rt::test]
async fn facet_search_dont_support_words() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "words"})).await;

    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 0);
}
