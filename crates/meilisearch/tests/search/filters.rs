use meili_snap::{json_string, snapshot};
use meilisearch::Opt;
use tempfile::TempDir;

use super::test_settings_documents_indexing_swapping_and_search;
use crate::common::{
    default_settings, shared_index_for_fragments, shared_index_with_documents,
    shared_index_with_nested_documents, Server, DOCUMENTS, NESTED_DOCUMENTS,
};
use crate::json;

#[actix_rt::test]
async fn search_with_filter_string_notation() {
    let index = shared_index_with_documents().await;

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

    let nested_index = shared_index_with_nested_documents().await;

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
    server.wait_task(request.uid()).await.succeeded();

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
    let server = Server::new_shared();

    eprintln!("Documents -> Settings -> test");
    let index = server.unique_index();

    let (task, code) = index.add_documents(NESTED_DOCUMENTS.clone(), None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

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
    server.wait_task(task.uid()).await.succeeded();

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
                  "message": "Index `[uuid]`: Filter operator `>` is not allowed for the attribute `doggos.age`.\n  - Note: allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.\n  - Note: field `doggos.age` matched rule #0 in `filterableAttributes`\n  - Hint: enable comparison in rule #0 by modifying the features.filter object\n  - Hint: prepend another rule matching `doggos.age` with appropriate filter features before rule #0",
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
    server.wait_task(task.uid()).await.succeeded();

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
    server.wait_task(task.uid()).await.succeeded();

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
                  "message": "Index `[uuid]`: Filter operator `=` is not allowed for the attribute `cattos`.\n  - Note: allowed operators: OR, AND, NOT, <, >, <=, >=, TO, IS EMPTY, IS NULL, EXISTS.\n  - Note: field `cattos` matched rule #0 in `filterableAttributes`\n  - Hint: enable equality in rule #0 by modifying the features.filter object\n  - Hint: prepend another rule matching `cattos` with appropriate filter features before rule #0",
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
    server.wait_task(task.uid()).await.succeeded();

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
                  "message": "Index `[uuid]`: Filter operator `>` is not allowed for the attribute `doggos.age`.\n  - Note: allowed operators: OR, AND, NOT, =, !=, IN, IS EMPTY, IS NULL, EXISTS.\n  - Note: field `doggos.age` matched rule #0 in `filterableAttributes`\n  - Hint: enable comparison in rule #0 by modifying the features.filter object\n  - Hint: prepend another rule matching `doggos.age` with appropriate filter features before rule #0",
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

#[actix_rt::test]
async fn vector_filter_all_embedders() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "kefir"
        },
        {
          "name": "echo"
        },
        {
          "name": "intel"
        },
        {
          "name": "dustin"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_missing_fragment() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.fragments EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(value, @r#"
    {
      "message": "The vector filter is missing a fragment name.\n24:31 _vectors.rest.fragments EXISTS",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "#);
}

#[actix_rt::test]
async fn vector_filter_nonexistent_embedder() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.other EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(value, @r#"
    {
      "message": "Index `[uuid]`: The embedder `other` does not exist. Available embedders are: `rest`.\n10:15 _vectors.other EXISTS",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "#);
}

#[actix_rt::test]
async fn vector_filter_all_embedders_user_provided() {
    let index = shared_index_for_fragments().await;

    // This one is counterintuitive, but it is the same as the previous one.
    // It's because userProvided is interpreted as an embedder name
    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.userProvided EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(value, @r#"
    {
      "message": "Index `[uuid]`: The embedder `userProvided` does not exist. Available embedders are: `rest`.\n10:22 _vectors.userProvided EXISTS",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "#);
}

#[actix_rt::test]
async fn vector_filter_specific_embedder() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "kefir"
        },
        {
          "name": "echo"
        },
        {
          "name": "intel"
        },
        {
          "name": "dustin"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_user_provided() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.userProvided EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "echo"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_specific_fragment() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.fragments.withBreed EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "intel"
        },
        {
          "name": "dustin"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2,
      "requestUid": "[uuid]"
    }
    "###);

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.fragments.basic EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "kefir"
        },
        {
          "name": "intel"
        },
        {
          "name": "dustin"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_non_existant_fragment() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.fragments.withBred EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(value, @r#"
    {
      "message": "Index `[uuid]`: The fragment `withBred` does not exist on embedder `rest`. Available fragments on this embedder are: `basic`, `withBreed`. Did you mean `withBreed`?\n25:33 _vectors.rest.fragments.withBred EXISTS",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "#);
}

#[actix_rt::test]
async fn vector_filter_document_template_but_fragments_used() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.documentTemplate EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_document_template() {
    let (_mock, setting) = crate::vector::create_mock().await;
    let server = crate::vector::get_server_vector().await;
    let index = server.index("doggo");

    let (_response, code) = server.set_features(json!({"multimodal": true})).await;
    snapshot!(code, @"200 OK");

    let (response, code) = index
        .update_settings(json!({
            "embedders": {
                "rest": setting,
            },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let documents = json!([
        {"id": 0, "name": "kefir"},
        {"id": 1, "name": "echo", "_vectors": { "rest": [1, 1, 1] }},
        {"id": 2, "name": "intel"},
        {"id": 3, "name": "iko" }
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(value.uid()).await.succeeded();

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.documentTemplate EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "kefir"
        },
        {
          "name": "intel"
        },
        {
          "name": "iko"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_feature_gate() {
    let index = shared_index_with_documents().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(value, @r#"
    {
      "message": "using a vector filter requires enabling the `multimodal` experimental feature. See https://github.com/orgs/meilisearch/discussions/846\n1:9 _vectors EXISTS",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "#);
}

#[actix_rt::test]
async fn vector_filter_negation() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.userProvided NOT EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "kefir"
        },
        {
          "name": "intel"
        },
        {
          "name": "dustin"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_or_combination() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": "_vectors.rest.fragments.withBreed EXISTS OR _vectors.rest.userProvided EXISTS",
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "echo"
        },
        {
          "name": "intel"
        },
        {
          "name": "dustin"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}

#[actix_rt::test]
async fn vector_filter_regenerate() {
    let index = shared_index_for_fragments().await;

    let (value, _code) = index
        .search_post(json!({
            "filter": format!("_vectors.rest.regenerate EXISTS"),
            "attributesToRetrieve": ["name"]
        }))
        .await;
    snapshot!(json_string!(value, { ".processingTimeMs" => "[duration]", ".requestUid" => "[uuid]" }), @r###"
    {
      "hits": [
        {
          "name": "kefir"
        },
        {
          "name": "intel"
        },
        {
          "name": "dustin"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "requestUid": "[uuid]"
    }
    "###);
}
