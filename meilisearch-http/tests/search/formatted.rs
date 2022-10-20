use serde_json::json;

use super::*;
use crate::common::Server;

#[actix_rt::test]
async fn formatted_contain_wildcard() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({ "displayedAttributes": ["id", "cattos"] })).await;

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

    index
        .search(json!({ "q": "pesti", "attributesToRetrieve": ["*"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(
                response["hits"][0],
                json!({
                    "id": 852,
                    "cattos": "pesti",
                })
            );
        })
        .await;

    index
        .search(
            json!({ "q": "pesti", "attributesToRetrieve": ["*"], "attributesToHighlight": ["id"], "showMatchesPosition": true }),
            |response, code| {
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
            }
        )
        .await;

    index
        .search(
            json!({ "q": "pesti", "attributesToRetrieve": ["*"], "attributesToCrop": ["*"] }),
            |response, code| {
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
            },
        )
        .await;

    index
        .search(json!({ "q": "pesti", "attributesToCrop": ["*"] }), |response, code| {
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
        })
        .await;
}

#[actix_rt::test]
async fn format_nested() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({ "q": "pesti", "attributesToRetrieve": ["doggos"] }), |response, code| {
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
        })
        .await;

    index
        .search(
            json!({ "q": "pesti", "attributesToRetrieve": ["doggos.name"] }),
            |response, code| {
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
            },
        )
        .await;

    index
        .search(
            json!({ "q": "bobby", "attributesToRetrieve": ["doggos.name"], "showMatchesPosition": true }),
            |response, code| {
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
            }
        )
        .await;

    index
        .search(json!({ "q": "pesti", "attributesToRetrieve": [], "attributesToHighlight": ["doggos.name"] }),
        |response, code| {
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
        })
        .await;

    index
        .search(json!({ "q": "pesti", "attributesToRetrieve": [], "attributesToCrop": ["doggos.name"] }),
        |response, code| {
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
        })
        .await;

    index
        .search(json!({ "q": "pesti", "attributesToRetrieve": ["doggos.name"], "attributesToHighlight": ["doggos.age"] }),
        |response, code| {
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
        })
        .await;

    index
        .search(json!({ "q": "pesti", "attributesToRetrieve": [], "attributesToHighlight": ["doggos.age"], "attributesToCrop": ["doggos.name"] }),
        |response, code| {
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
        )
        .await;
}

#[actix_rt::test]
async fn displayedattr_2_smol() {
    let server = Server::new().await;
    let index = server.index("test");

    // not enough displayed for the other settings
    index.update_settings(json!({ "displayedAttributes": ["id"] })).await;

    let documents = NESTED_DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    index
        .search(json!({ "attributesToRetrieve": ["father", "id"], "attributesToHighlight": ["mother"], "attributesToCrop": ["cattos"] }),
        |response, code| {
    assert_eq!(code, 200, "{}", response);
    assert_eq!(
        response["hits"][0],
        json!({
            "id": 852,
        })
    );
        })
        .await;

    index
        .search(json!({ "attributesToRetrieve": ["id"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(
                response["hits"][0],
                json!({
                    "id": 852,
                })
            );
        })
        .await;

    index
        .search(json!({ "attributesToHighlight": ["id"] }), |response, code| {
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
        })
        .await;

    index
        .search(json!({ "attributesToCrop": ["id"] }), |response, code| {
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
        })
        .await;

    index
        .search(
            json!({ "attributesToHighlight": ["id"], "attributesToCrop": ["id"] }),
            |response, code| {
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
            },
        )
        .await;

    index
        .search(json!({ "attributesToHighlight": ["cattos"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(
                response["hits"][0],
                json!({
                    "id": 852,
                })
            );
        })
        .await;

    index
        .search(json!({ "attributesToCrop": ["cattos"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(
                response["hits"][0],
                json!({
                    "id": 852,
                })
            );
        })
        .await;

    index
        .search(json!({ "attributesToRetrieve": ["cattos"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"][0], json!({}));
        })
        .await;

    index
        .search(
            json!({ "attributesToRetrieve": ["cattos"], "attributesToHighlight": ["cattos"], "attributesToCrop": ["cattos"] }),
            |response, code| {
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"][0], json!({}));

            }
        )
        .await;

    index
        .search(
            json!({ "attributesToRetrieve": ["cattos"], "attributesToHighlight": ["id"] }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(
                    response["hits"][0],
                    json!({
                        "_formatted": {
                            "id": "852",
                        }
                    })
                );
            },
        )
        .await;

    index
        .search(
            json!({ "attributesToRetrieve": ["cattos"], "attributesToCrop": ["id"] }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(
                    response["hits"][0],
                    json!({
                        "_formatted": {
                            "id": "852",
                        }
                    })
                );
            },
        )
        .await;
}
