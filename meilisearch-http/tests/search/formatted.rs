use super::*;
use crate::common::Server;
use serde_json::json;

#[actix_rt::test]
async fn formatted_contain_wildcard() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .update_settings(json!({ "displayedAttributes": ["id", "cattos"] }))
        .await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index.search(json!({ "q": "pesti", "attributesToRetrieve": ["father", "mother"], "attributesToHighlight": ["father", "mother", "*"], "attributesToCrop": ["doggos"], "showMatchesPosition": true }),
        |response, code|
        {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(
                response["hits"][0],
                json!({
                    "_formatted": {
                        "id": "852",
                        "cattos": "<em>pesti</em>",
                    },
                    "_matchesPosition": {"cattos": [{"start": 0, "length": 5}]},
                })
            );
        }
    )
    .await;

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToRetrieve": ["*"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
            "cattos": "pesti",
        })
    );

    let (response, code) = index
        .search_post(
            json!({ "q": "pesti", "attributesToRetrieve": ["*"], "attributesToHighlight": ["id"], "showMatchesPosition": true }),
        )
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
            "cattos": "pesti",
            "_formatted": {
                "id": "852",
                "cattos": "pesti",
            },
            "_matchesPosition": {"cattos": [{"start": 0, "length": 5}]},
        })
    );

    let (response, code) = index
        .search_post(
            json!({ "q": "pesti", "attributesToRetrieve": ["*"], "attributesToCrop": ["*"] }),
        )
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
            "cattos": "pesti",
            "_formatted": {
                "id": "852",
                "cattos": "pesti",
            }
        })
    );

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToCrop": ["*"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
            "cattos": "pesti",
            "_formatted": {
                "id": "852",
                "cattos": "pesti",
            }
        })
    );
}

#[actix_rt::test]
async fn format_nested() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToRetrieve": ["doggos"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
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
        })
    );

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToRetrieve": ["doggos.name"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "doggos": [
                {
                    "name": "bobby",
                },
                {
                    "name": "buddy",
                },
            ],
        })
    );

    let (response, code) = index
        .search_post(
            json!({ "q": "bobby", "attributesToRetrieve": ["doggos.name"], "showMatchesPosition": true }),
        )
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "doggos": [
                {
                    "name": "bobby",
                },
                {
                    "name": "buddy",
                },
            ],
            "_matchesPosition": {"doggos.name": [{"start": 0, "length": 5}]},
        })
    );

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToRetrieve": [], "attributesToHighlight": ["doggos.name"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "_formatted": {
                "doggos": [
                    {
                        "name": "bobby",
                    },
                    {
                        "name": "buddy",
                    },
                ],
            },
        })
    );

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToRetrieve": [], "attributesToCrop": ["doggos.name"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "_formatted": {
                "doggos": [
                    {
                        "name": "bobby",
                    },
                    {
                        "name": "buddy",
                    },
                ],
            },
        })
    );

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToRetrieve": ["doggos.name"], "attributesToHighlight": ["doggos.age"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "doggos": [
                {
                    "name": "bobby",
                },
                {
                    "name": "buddy",
                },
            ],
            "_formatted": {
                "doggos": [
                    {
                        "name": "bobby",
                        "age": "2",
                    },
                    {
                        "name": "buddy",
                        "age": "4",
                    },
                ],
            },
        })
    );

    let (response, code) = index
        .search_post(json!({ "q": "pesti", "attributesToRetrieve": [], "attributesToHighlight": ["doggos.age"], "attributesToCrop": ["doggos.name"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "_formatted": {
                "doggos": [
                    {
                        "name": "bobby",
                        "age": "2",
                    },
                    {
                        "name": "buddy",
                        "age": "4",
                    },
                ],
            },
        })
    );
}

#[actix_rt::test]
async fn displayedattr_2_smol() {
    let server = Server::new().await;
    let index = server.index("test");

    // not enough displayed for the other settings
    index
        .update_settings(json!({ "displayedAttributes": ["id"] }))
        .await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    let (response, code) = index
        .search_post(json!({ "attributesToRetrieve": ["father", "id"], "attributesToHighlight": ["mother"], "attributesToCrop": ["cattos"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToRetrieve": ["id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToHighlight": ["id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
            "_formatted": {
                "id": "852",
            }
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToCrop": ["id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
            "_formatted": {
                "id": "852",
            }
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToHighlight": ["id"], "attributesToCrop": ["id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
            "_formatted": {
                "id": "852",
            }
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToHighlight": ["cattos"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToCrop": ["cattos"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToRetrieve": ["cattos"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"][0], json!({}));

    let (response, code) = index
        .search_post(
            json!({ "attributesToRetrieve": ["cattos"], "attributesToHighlight": ["cattos"], "attributesToCrop": ["cattos"] }),
        )
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"][0], json!({}));

    let (response, code) = index
        .search_post(json!({ "attributesToRetrieve": ["cattos"], "attributesToHighlight": ["id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "_formatted": {
                "id": "852",
            }
        })
    );

    let (response, code) = index
        .search_post(json!({ "attributesToRetrieve": ["cattos"], "attributesToCrop": ["id"] }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "_formatted": {
                "id": "852",
            }
        })
    );
}
