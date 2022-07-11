// This modules contains all the test concerning search. Each particular feture of the search
// should be tested in its own module to isolate tests and keep the tests readable.

mod errors;
mod formatted;

use crate::common::Server;
use once_cell::sync::Lazy;
use serde_json::{json, Value};

pub(self) static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "id": "287947",
        },
        {
            "title": "Captain Marvel",
            "id": "299537",
        },
        {
            "title": "Escape Room",
            "id": "522681",
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "id": "166428",
        },
        {
            "title": "Glass",
            "id": "450465",
        }
    ])
});

pub(self) static NESTED_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
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

#[actix_rt::test]
async fn simple_placeholder_search() {
    let server = Server::new().await;
    let index = server.index("basic");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 5);
        })
        .await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 4);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"q": "glass"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(json!({"q": "pesti"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 2);
        })
        .await;
}

#[actix_rt::test]
async fn search_multiple_params() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

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

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "q": "pesti",
                "attributesToCrop": ["catto:2"],
                "attributesToHighlight": ["catto"],
                "limit": 2,
                "offset": 0,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
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
    index.wait_task(1).await;

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

    let index = server.index("nested");

    index
        .update_settings(json!({"filterableAttributes": ["cattos", "doggos.age"]}))
        .await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(3).await;

    index
        .search(
            json!({
                "filter": "cattos = pesti"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
                assert_eq!(response["hits"][0]["id"], json!(852));
            },
        )
        .await;

    index
        .search(
            json!({
                "filter": "doggos.age > 5"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
                assert_eq!(response["hits"][0]["id"], json!(654));
                assert_eq!(response["hits"][1]["id"], json!(951));
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
    index.wait_task(1).await;

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
    index.wait_task(1).await;

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

    let index = server.index("nested");

    index
        .update_settings(json!({"sortableAttributes": ["doggos.age"]}))
        .await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(3).await;

    index
        .search(
            json!({
                "sort": ["doggos.age:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
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
    index.wait_task(1).await;

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

    let index = server.index("nested");

    index
        .update_settings(json!({"sortableAttributes": ["doggos.name"]}))
        .await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(3).await;

    index
        .search(
            json!({
                "sort": ["doggos.name:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
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
    index.wait_task(1).await;

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
    index.wait_task(1).await;

    index
        .search(
            json!({
                "facets": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert!(dist.get("title").is_some());
            },
        )
        .await;

    let index = server.index("nested");

    index
        .update_settings(json!({"filterableAttributes": ["father", "doggos.name"]}))
        .await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(3).await;

    // TODO: TAMO: fix the test
    index
        .search(
            json!({
                // "facets": ["father", "doggos.name"]
                "facets": ["father"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["father"],
                    json!({ "jean": 1, "pierre": 1, "romain": 1, "jean-baptiste": 1})
                );
                /*
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
                */
            },
        )
        .await;

    index
        .update_settings(json!({"filterableAttributes": ["doggos"]}))
        .await;
    index.wait_task(4).await;

    index
        .search(
            json!({
                "facets": ["doggos.name"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
            },
        )
        .await;

    index
        .search(
            json!({
                "facets": ["doggos"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 3);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
                assert_eq!(
                    dist["doggos.age"],
                    json!({ "2": 1, "4": 1, "5": 1, "6": 1, "8": 1})
                );
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
    index.wait_task(1).await;

    let (response, code) = index
        .search_post(json!({ "attributesToRetrieve": ["title", "id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert!(response["hits"][0].get("title").is_some());
}

#[actix_rt::test]
async fn placeholder_search_is_hard_limited() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents: Vec<_> = (0..1200)
        .map(|i| json!({ "id": i, "text": "I am unique!" }))
        .collect();
    index.add_documents(documents.into(), None).await;
    index.wait_task(0).await;

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1000);
            },
        )
        .await;

    index
        .search(
            json!({
                "offset": 800,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    index
        .update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } }))
        .await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1200);
            },
        )
        .await;

    index
        .search(
            json!({
                "offset": 1000,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_is_hard_limited() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents: Vec<_> = (0..1200)
        .map(|i| json!({ "id": i, "text": "I am unique!" }))
        .collect();
    index.add_documents(documents.into(), None).await;
    index.wait_task(0).await;

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1000);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": "unique",
                "offset": 800,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    index
        .update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } }))
        .await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1200);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": "unique",
                "offset": 1000,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn faceting_max_values_per_facet() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({ "filterableAttributes": ["number"] }))
        .await;

    let documents: Vec<_> = (0..10_000)
        .map(|id| json!({ "id": id, "number": id * 10 }))
        .collect();
    index.add_documents(json!(documents), None).await;
    index.wait_task(1).await;

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let numbers = response["facetDistribution"]["number"].as_object().unwrap();
                assert_eq!(numbers.len(), 100);
            },
        )
        .await;

    index
        .update_settings(json!({ "faceting": { "maxValuesPerFacet": 10_000 } }))
        .await;
    index.wait_task(2).await;

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let numbers = dbg!(&response)["facetDistribution"]["number"]
                    .as_object()
                    .unwrap();
                assert_eq!(numbers.len(), 10_000);
            },
        )
        .await;
}
