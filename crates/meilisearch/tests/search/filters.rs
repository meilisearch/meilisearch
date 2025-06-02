use meili_snap::{json_string, snapshot};
use meilisearch::Opt;
use tempfile::TempDir;

use super::test_settings_documents_indexing_swapping_and_search;
use crate::{
    common::{default_settings, shared_index_with_documents, Server, DOCUMENTS, NESTED_DOCUMENTS},
    json,
};

#[actix_rt::test]
async fn search_with_filter_string_notation() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (_, code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (task, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let res = index.wait_task(task.uid()).await.succeeded();
    snapshot!(res["status"], @r###""succeeded""###);

    index
        .search(
            json!({
                "filter": "title = Gläss"
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;

    let nested_index = server.unique_index();

    let (_, code) =
        nested_index.update_settings(json!({"filterableAttributes": ["cattos", "doggos.age"]})).await;
    snapshot!(code, @"202 Accepted");

    let documents = NESTED_DOCUMENTS.clone();
    let (task, code) = nested_index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let res = nested_index.wait_task(task.uid()).await.succeeded();
    snapshot!(res["status"], @r###""succeeded""###);

    nested_index
        .search(
            json!({
                "filter": "cattos = pésti"
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
                assert_eq!(response["hits"][0]["id"], json!(852));
            },
        )
        .await;

    nested_index
        .search(
            json!({
                "filter": "doggos.age > 5"
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
                assert_eq!(response["hits"][0]["id"], json!(654));
                assert_eq!(response["hits"][1]["id"], json!(951));
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_filter_array_notation() {
    let index = shared_index_with_documents().await;
    let (response, code) = index
        .search_post(json!({
            "filter": ["title = Gläss"]
        }))
        .await;
    assert_eq!(code, 200, "{response}");
    assert_eq!(response["hits"].as_array().unwrap().len(), 1);

    let (response, code) = index
        .search_post(json!({
            "filter": [["title = Gläss", "title = \"Shazam!\"", "title = \"Escape Room\""]]
        }))
        .await;
    assert_eq!(code, 200, "{response}");
    assert_eq!(response["hits"].as_array().unwrap().len(), 3);
}

#[actix_rt::test]
async fn search_with_contains_filter() {
    let temp = TempDir::new().unwrap();
    let server = Server::new_with_options(Opt {
        experimental_contains_filter: true,
        ..default_settings(temp.path())
    })
    .await
    .unwrap();
    let index = server.index("movies");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    let (request, _code) = index.add_documents(documents, None).await;
    index.wait_task(request.uid()).await.succeeded();

    let (response, code) = index
        .search_post(json!({
            "filter": "title CONTAINS cap"
        }))
        .await;
    assert_eq!(code, 200, "{response}");
    assert_eq!(response["hits"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn search_with_pattern_filter_settings() {
    // Check if the Equality filter works with patterns
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["cattos","doggos.age"]}]}),
        &json!({
            "filter": "cattos = pésti"
        }),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 852,
                "father": "jean",
                "mother": "michelle",
                "doggos": [
                  {
                    "name": "bobby",
                    "age": 2
                  },
                  {
                    "name": "buddy",
                    "age": 4
                  }
                ],
                "cattos": "pésti"
              }
            ]
            "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{
            "attributePatterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": false}
            }
        }]}),
        &json!({
            "filter": "cattos = pésti"
        }),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 852,
                "father": "jean",
                "mother": "michelle",
                "doggos": [
                  {
                    "name": "bobby",
                    "age": 2
                  },
                  {
                    "name": "buddy",
                    "age": 4
                  }
                ],
                "cattos": "pésti"
              }
            ]
            "###);
        },
    )
    .await;

    // Check if the Comparison filter works with patterns
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{
            "attributePatterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": false, "comparison": true}
            }
        }]}),
        &json!({
            "filter": "doggos.age > 2"
        }),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 852,
                "father": "jean",
                "mother": "michelle",
                "doggos": [
                  {
                    "name": "bobby",
                    "age": 2
                  },
                  {
                    "name": "buddy",
                    "age": 4
                  }
                ],
                "cattos": "pésti"
              },
              {
                "id": 654,
                "father": "pierre",
                "mother": "sabine",
                "doggos": [
                  {
                    "name": "gros bill",
                    "age": 8
                  }
                ],
                "cattos": [
                  "simba",
                  "pestiféré"
                ]
              },
              {
                "id": 951,
                "father": "jean-baptiste",
                "mother": "sophie",
                "doggos": [
                  {
                    "name": "turbo",
                    "age": 5
                  },
                  {
                    "name": "fast",
                    "age": 6
                  }
                ],
                "cattos": [
                  "moumoute",
                  "gomez"
                ]
              }
            ]
            "###);
        },
    )
    .await;
}

#[actix_rt::test]
async fn search_with_pattern_filter_settings_scenario_1() {
    let temp = TempDir::new().unwrap();
    let server = Server::new_with_options(Opt { ..default_settings(temp.path()) }).await.unwrap();

    eprintln!("Documents -> Settings -> test");
    let index = server.index("test");

    let (task, code) = index.add_documents(NESTED_DOCUMENTS.clone(), None).await;
    assert_eq!(code, 202, "{task}");
    let response = index.wait_task(task.uid()).await.succeeded();
    snapshot!(response["status"], @r###""succeeded""###);

    let (task, code) = index
        .update_settings(json!({"filterableAttributes": [{
            "attributePatterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": false}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{task}");
    let response = index.wait_task(task.uid()).await.succeeded();
    snapshot!(response["status"], @r###""succeeded""###);

    // Check if the Equality filter works
    index
        .search(
            json!({
                "filter": "cattos = pésti"
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "father": "jean",
                    "mother": "michelle",
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ],
                    "cattos": "pésti"
                  }
                ]
                "###);
            },
        )
        .await;

    // Check if the Comparison filter returns an error
    index
        .search(
            json!({
                "filter": "doggos.age > 2"
            }),
            |response, code| {
                snapshot!(code, @"400 Bad Request");
                snapshot!(json_string!(response), @r###"
                {
                  "message": "Index `test`: Filter operator `>` is not allowed for the attribute `doggos.age`.\n  - Note: allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.\n  - Note: field `doggos.age` matched rule #0 in `filterableAttributes`\n  - Hint: enable comparison in rule #0 by modifying the features.filter object\n  - Hint: prepend another rule matching `doggos.age` with appropriate filter features before rule #0",
                  "code": "invalid_search_filter",
                  "type": "invalid_request",
                  "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
                }
                "###);
            },
        )
        .await;

    // Update the settings activate comparison filter
    let (task, code) = index
        .update_settings(json!({"filterableAttributes": [{
            "attributePatterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": true}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{task}");
    let response = index.wait_task(task.uid()).await.succeeded();
    snapshot!(response["status"], @r###""succeeded""###);

    // Check if the Equality filter works
    index
        .search(
            json!({
                "filter": "cattos = pésti"
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "father": "jean",
                    "mother": "michelle",
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ],
                    "cattos": "pésti"
                  }
                ]
                "###);
            },
        )
        .await;

    // Check if the Comparison filter works
    index
        .search(
            json!({
                "filter": "doggos.age > 2"
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "father": "jean",
                    "mother": "michelle",
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ],
                    "cattos": "pésti"
                  },
                  {
                    "id": 654,
                    "father": "pierre",
                    "mother": "sabine",
                    "doggos": [
                      {
                        "name": "gros bill",
                        "age": 8
                      }
                    ],
                    "cattos": [
                      "simba",
                      "pestiféré"
                    ]
                  },
                  {
                    "id": 951,
                    "father": "jean-baptiste",
                    "mother": "sophie",
                    "doggos": [
                      {
                        "name": "turbo",
                        "age": 5
                      },
                      {
                        "name": "fast",
                        "age": 6
                      }
                    ],
                    "cattos": [
                      "moumoute",
                      "gomez"
                    ]
                  }
                ]
                "###);
            },
        )
        .await;

    // Update the settings deactivate equality filter
    let (task, code) = index
        .update_settings(json!({"filterableAttributes": [{
            "attributePatterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": false, "comparison": true}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{task}");
    let response = index.wait_task(task.uid()).await.succeeded();
    snapshot!(response["status"], @r###""succeeded""###);

    // Check if the Equality filter returns an error
    index
        .search(
            json!({
                "filter": "cattos = pésti"
            }),
            |response, code| {
                snapshot!(code, @"400 Bad Request");
                snapshot!(json_string!(response), @r###"
                {
                  "message": "Index `test`: Filter operator `=` is not allowed for the attribute `cattos`.\n  - Note: allowed operators: OR, AND, NOT, <, >, <=, >=, TO, IS EMPTY, IS NULL, EXISTS.\n  - Note: field `cattos` matched rule #0 in `filterableAttributes`\n  - Hint: enable equality in rule #0 by modifying the features.filter object\n  - Hint: prepend another rule matching `cattos` with appropriate filter features before rule #0",
                  "code": "invalid_search_filter",
                  "type": "invalid_request",
                  "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
                }
                "###);
            },
        )
        .await;

    // Check if the Comparison filter works
    index
        .search(
            json!({
                "filter": "doggos.age > 2"
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "father": "jean",
                    "mother": "michelle",
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ],
                    "cattos": "pésti"
                  },
                  {
                    "id": 654,
                    "father": "pierre",
                    "mother": "sabine",
                    "doggos": [
                      {
                        "name": "gros bill",
                        "age": 8
                      }
                    ],
                    "cattos": [
                      "simba",
                      "pestiféré"
                    ]
                  },
                  {
                    "id": 951,
                    "father": "jean-baptiste",
                    "mother": "sophie",
                    "doggos": [
                      {
                        "name": "turbo",
                        "age": 5
                      },
                      {
                        "name": "fast",
                        "age": 6
                      }
                    ],
                    "cattos": [
                      "moumoute",
                      "gomez"
                    ]
                  }
                ]
                "###);
            },
        )
        .await;

    // rollback the settings
    let (task, code) = index
        .update_settings(json!({"filterableAttributes": [{
            "attributePatterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": false}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{task}");
    let response = index.wait_task(task.uid()).await.succeeded();
    snapshot!(response["status"], @r###""succeeded""###);

    // Check if the Equality filter works
    index
        .search(
            json!({
                "filter": "cattos = pésti"
            }),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "father": "jean",
                    "mother": "michelle",
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ],
                    "cattos": "pésti"
                  }
                ]
                "###);
            },
        )
        .await;

    // Check if the Comparison filter returns an error
    index
        .search(
            json!({
                "filter": "doggos.age > 2"
            }),
            |response, code| {
                snapshot!(code, @"400 Bad Request");
                snapshot!(json_string!(response), @r###"
                {
                  "message": "Index `test`: Filter operator `>` is not allowed for the attribute `doggos.age`.\n  - Note: allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.\n  - Note: field `doggos.age` matched rule #0 in `filterableAttributes`\n  - Hint: enable comparison in rule #0 by modifying the features.filter object\n  - Hint: prepend another rule matching `doggos.age` with appropriate filter features before rule #0",
                  "code": "invalid_search_filter",
                  "type": "invalid_request",
                  "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
                }
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_filterable_attributes_priority() {
    // Test that the filterable attributes priority is respected

    // check if doggos.name is filterable
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [
            // deactivated filter
            {"attributePatterns": ["doggos.a*"], "features": {"facetSearch": false, "filter": {"equality": false, "comparison": false}}},
            // activated filter
            {"attributePatterns": ["doggos.*"]},
        ]}),
        &json!({
            "filter": "doggos.name = bobby"
        }),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 852,
                "father": "jean",
                "mother": "michelle",
                "doggos": [
                  {
                    "name": "bobby",
                    "age": 2
                  },
                  {
                    "name": "buddy",
                    "age": 4
                  }
                ],
                "cattos": "pésti"
              }
            ]
            "###);
        },
    )
    .await;

    // check if doggos.name is filterable 2
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [
            // deactivated filter
            {"attributePatterns": ["doggos"], "features": {"facetSearch": false, "filter": {"equality": false, "comparison": false}}},
            // activated filter
            {"attributePatterns": ["doggos.*"]},
        ]}),
        &json!({
            "filter": "doggos.name = bobby"
        }),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 852,
                "father": "jean",
                "mother": "michelle",
                "doggos": [
                  {
                    "name": "bobby",
                    "age": 2
                  },
                  {
                    "name": "buddy",
                    "age": 4
                  }
                ],
                "cattos": "pésti"
              }
            ]
            "###);
        },
    )
    .await;

    // check if doggos.age is not filterable
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [
            // deactivated filter
            {"attributePatterns": ["doggos.a*"], "features": {"facetSearch": false, "filter": {"equality": false, "comparison": false}}},
            // activated filter
            {"attributePatterns": ["doggos.*"]},
        ]}),
        &json!({
            "filter": "doggos.age > 2"
        }),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `[uuid]`: Attribute `doggos.age` is not filterable. Available filterable attribute patterns are: `doggos.*`.\n1:11 doggos.age > 2",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
        },
    )
    .await;

    // check if doggos is not filterable
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [
            // deactivated filter
            {"attributePatterns": ["doggos"], "features": {"facetSearch": false, "filter": {"equality": false, "comparison": false}}},
            // activated filter
            {"attributePatterns": ["doggos.*"]},
        ]}),
        &json!({
            "filter": "doggos EXISTS"
        }),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `[uuid]`: Attribute `doggos` is not filterable. Available filterable attribute patterns are: `doggos.*`.\n1:7 doggos EXISTS",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
        },
    )
    .await;
}
