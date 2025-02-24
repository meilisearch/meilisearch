use std::panic::UnwindSafe;

use actix_http::StatusCode;
use meili_snap::{json_string, snapshot};
use meilisearch::Opt;
use tempfile::TempDir;

use crate::{
    common::{
        default_settings, shared_index_with_documents, Server, Value, DOCUMENTS, NESTED_DOCUMENTS,
    },
    json,
};

async fn test_settings_documents_indexing_swapping_and_search(
    documents: &Value,
    settings: &Value,
    query: &Value,
    test: impl Fn(Value, StatusCode) + UnwindSafe + Clone,
) {
    let temp = TempDir::new().unwrap();
    let server = Server::new_with_options(Opt { ..default_settings(temp.path()) }).await.unwrap();

    eprintln!("Documents -> Settings -> test");
    let index = server.index("test");

    let (task, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);

    let (task, code) = index.update_settings(settings.clone()).await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);

    index.search(query.clone(), test.clone()).await;
    let (task, code) = server.delete_index("test").await;
    assert_eq!(code, 202, "{}", task);
    let response = server.wait_task(task.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);

    eprintln!("Settings -> Documents -> test");
    let index = server.index("test");

    let (task, code) = index.update_settings(settings.clone()).await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);

    let (task, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);

    index.search(query.clone(), test.clone()).await;
    let (task, code) = server.delete_index("test").await;
    assert_eq!(code, 202, "{}", task);
    let response = server.wait_task(task.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);
}

#[actix_rt::test]
async fn search_with_filter_string_notation() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_, code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (task, code) = index.add_documents(documents, None).await;
    meili_snap::snapshot!(code, @"202 Accepted");
    let res = index.wait_task(task.uid()).await;
    meili_snap::snapshot!(res["status"], @r###""succeeded""###);

    index
        .search(
            json!({
                "filter": "title = Gläss"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;

    let index = server.index("nested");

    let (_, code) =
        index.update_settings(json!({"filterableAttributes": ["cattos", "doggos.age"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = NESTED_DOCUMENTS.clone();
    let (task, code) = index.add_documents(documents, None).await;
    meili_snap::snapshot!(code, @"202 Accepted");
    let res = index.wait_task(task.uid()).await;
    meili_snap::snapshot!(res["status"], @r###""succeeded""###);

    index
        .search(
            json!({
                "filter": "cattos = pésti"
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
    let index = shared_index_with_documents().await;
    let (response, code) = index
        .search_post(json!({
            "filter": ["title = Gläss"]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 1);

    let (response, code) = index
        .search_post(json!({
            "filter": [["title = Gläss", "title = \"Shazam!\"", "title = \"Escape Room\""]]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
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
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn search_with_pattern_filter_settings() {
    // Check if the Equality filter works with patterns
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{"patterns": ["cattos","doggos.age"]}]}),
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
                "cattos": "pésti",
                "_vectors": {
                  "manual": [
                    1,
                    2,
                    3
                  ]
                }
              }
            ]
            "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{
            "patterns": ["cattos","doggos.age"],
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
                "cattos": "pésti",
                "_vectors": {
                  "manual": [
                    1,
                    2,
                    3
                  ]
                }
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
            "patterns": ["cattos","doggos.age"],
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
                "cattos": "pésti",
                "_vectors": {
                  "manual": [
                    1,
                    2,
                    3
                  ]
                }
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
                ],
                "_vectors": {
                  "manual": [
                    1,
                    2,
                    54
                  ]
                }
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
                ],
                "_vectors": {
                  "manual": [
                    10,
                    23,
                    32
                  ]
                }
              }
            ]
            "###);
        },
    )
    .await;
}
#[actix_rt::test]
async fn search_with_pattern_filter_settings_errors() {
    // Check if the Equality filter works with patterns
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{
            "patterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": false, "comparison": true}
            }
        }]}),
        &json!({
            "filter": "cattos = pésti"
        }),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `test`: Filter operator `=` is not allowed for the attribute `cattos`, allowed operators: OR, AND, NOT, <, >, <=, >=, TO, IS EMPTY, IS NULL, EXISTS.",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
    &NESTED_DOCUMENTS,
    &json!({"filterableAttributes": [{
        "patterns": ["cattos","doggos.age"],
        "features": {
            "facetSearch": false,
            "filter": {"equality": false, "comparison": true}
        }
    }]}),
    &json!({
        "filter": "cattos IN [pésti, simba]"
    }),
    |response, code| {
        snapshot!(code, @"400 Bad Request");
        snapshot!(json_string!(response), @r###"
        {
          "message": "Index `test`: Filter operator `=` is not allowed for the attribute `cattos`, allowed operators: OR, AND, NOT, <, >, <=, >=, TO, IS EMPTY, IS NULL, EXISTS.",
          "code": "invalid_search_filter",
          "type": "invalid_request",
          "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
        }
        "###);
    },
)
.await;

    // Check if the Comparison filter works with patterns
    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{"patterns": ["cattos","doggos.age"]}]}),
        &json!({
            "filter": "doggos.age > 2"
        }),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `test`: Filter operator `>` is not allowed for the attribute `doggos.age`, allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{
            "patterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": false}
            }
        }]}),
        &json!({
            "filter": "doggos.age > 2"
        }),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `test`: Filter operator `>` is not allowed for the attribute `doggos.age`, allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{
            "patterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": false}
            }
        }]}),
        &json!({
            "filter": "doggos.age 2 TO 4"
        }),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Index `test`: Filter operator `TO` is not allowed for the attribute `doggos.age`, allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.",
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
async fn search_with_pattern_filter_settings_scenario_1() {
    let temp = TempDir::new().unwrap();
    let server = Server::new_with_options(Opt { ..default_settings(temp.path()) }).await.unwrap();

    eprintln!("Documents -> Settings -> test");
    let index = server.index("test");

    let (task, code) = index.add_documents(NESTED_DOCUMENTS.clone(), None).await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);

    let (task, code) = index
        .update_settings(json!({"filterableAttributes": [{
            "patterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": false}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
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
                    "cattos": "pésti",
                    "_vectors": {
                      "manual": [
                        1,
                        2,
                        3
                      ]
                    }
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
                  "message": "Index `test`: Filter operator `>` is not allowed for the attribute `doggos.age`, allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.",
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
            "patterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": true}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
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
                    "cattos": "pésti",
                    "_vectors": {
                      "manual": [
                        1,
                        2,
                        3
                      ]
                    }
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
                    "cattos": "pésti",
                    "_vectors": {
                      "manual": [
                        1,
                        2,
                        3
                      ]
                    }
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
                    ],
                    "_vectors": {
                      "manual": [
                        1,
                        2,
                        54
                      ]
                    }
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
                    ],
                    "_vectors": {
                      "manual": [
                        10,
                        23,
                        32
                      ]
                    }
                  }
                ]
                "###);
            },
        )
        .await;

    // Update the settings deactivate equality filter
    let (task, code) = index
        .update_settings(json!({"filterableAttributes": [{
            "patterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": false, "comparison": true}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
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
                  "message": "Index `test`: Filter operator `=` is not allowed for the attribute `cattos`, allowed operators: OR, AND, NOT, <, >, <=, >=, TO, IS EMPTY, IS NULL, EXISTS.",
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
                    "cattos": "pésti",
                    "_vectors": {
                      "manual": [
                        1,
                        2,
                        3
                      ]
                    }
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
                    ],
                    "_vectors": {
                      "manual": [
                        1,
                        2,
                        54
                      ]
                    }
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
                    ],
                    "_vectors": {
                      "manual": [
                        10,
                        23,
                        32
                      ]
                    }
                  }
                ]
                "###);
            },
        )
        .await;

    // rollback the settings
    let (task, code) = index
        .update_settings(json!({"filterableAttributes": [{
            "patterns": ["cattos","doggos.age"],
            "features": {
                "facetSearch": false,
                "filter": {"equality": true, "comparison": false}
            }
        }]}))
        .await;
    assert_eq!(code, 202, "{}", task);
    let response = index.wait_task(task.uid()).await;
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
                    "cattos": "pésti",
                    "_vectors": {
                      "manual": [
                        1,
                        2,
                        3
                      ]
                    }
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
                  "message": "Index `test`: Filter operator `>` is not allowed for the attribute `doggos.age`, allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.",
                  "code": "invalid_search_filter",
                  "type": "invalid_request",
                  "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
                }
                "###);
            },
        )
        .await;
}
