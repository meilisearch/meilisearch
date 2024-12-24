use insta::{allow_duplicates, assert_json_snapshot};

use super::*;
use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn search_formatted_from_sdk() {
    let server = Server::new_shared();
    let index = server.unique_index();

    index
        .update_settings(
            json!({ "filterableAttributes": ["genre"], "searchableAttributes": ["title"] }),
        )
        .await;

    let documents = json!([
      { "id": 123,  "title": "Pride and Prejudice",                     "genre": "romance" },
      { "id": 456,  "title": "Le Petit Prince",                         "genre": "adventure" },
      { "id": 1,    "title": "Alice In Wonderland",                     "genre": "adventure" },
      { "id": 2,    "title": "Le Rouge et le Noir",                     "genre": "romance" },
      { "id": 1344, "title": "The Hobbit",                              "genre": "adventure" },
      { "id": 4,    "title": "Harry Potter and the Half-Blood Prince",  "genre": "fantasy" },
      { "id": 7,    "title": "Harry Potter and the Chamber of Secrets", "genre": "fantasy" },
      { "id": 42,   "title": "The Hitchhiker's Guide to the Galaxy" }
    ]);
    let (response, _) = index.add_documents(documents, None).await;
    index.wait_task(response.uid()).await;

    index
        .search(
            json!({ "q":"prince",
              "attributesToCrop": ["title"],
              "cropLength": 2,
              "filter": "genre = adventure",
              "attributesToHighlight": ["title"],
              "attributesToRetrieve": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                  assert_json_snapshot!(response["hits"][0],
                        { "._rankingScore" => "[score]" },
                        @r###"
                  {
                    "title": "Le Petit Prince",
                    "_formatted": {
                      "title": "…Petit <em>Prince</em>"
                    }
                  }
                  "###);
                }
            },
        )
        .await;
}

#[actix_rt::test]
async fn formatted_contain_wildcard() {
    let server = Server::new_shared();
    let index = server.unique_index();

    index.update_settings(json!({ "displayedAttributes": ["id", "cattos"] })).await;

    let documents = NESTED_DOCUMENTS.clone();
    let (response, _) = index.add_documents(documents, None).await;
    index.wait_task(response.uid()).await.succeeded();

    index.search(json!({ "q": "pésti", "attributesToRetrieve": ["father", "mother"], "attributesToHighlight": ["father", "mother", "*"], "attributesToCrop": ["doggos"], "showMatchesPosition": true }),
        |response, code|
        {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
              assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
              {
                "_formatted": {
                  "id": "852",
                  "cattos": "<em>pésti</em>"
                },
                "_matchesPosition": {
                  "cattos": [
                    {
                      "start": 0,
                      "length": 5
                    }
                  ]
                }
              }
              "###);
            }
    }
    )
    .await;

    index
        .search(json!({ "q": "pésti", "attributesToRetrieve": ["*"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "id": 852,
                  "cattos": "pésti"
                }
                "###)
            }
        })
        .await;

    index
        .search(
            json!({ "q": "pésti", "attributesToRetrieve": ["*"], "attributesToHighlight": ["id"], "showMatchesPosition": true }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                  assert_json_snapshot!(response["hits"][0],
                 { "._rankingScore" => "[score]" },
                 @r###"
                  {
                    "id": 852,
                    "cattos": "pésti",
                    "_formatted": {
                      "id": "852",
                      "cattos": "pésti"
                    },
                    "_matchesPosition": {
                      "cattos": [
                        {
                          "start": 0,
                          "length": 5
                        }
                      ]
                    }
                  }
                  "###)
             }
        })
        .await;

    index
        .search(
            json!({ "q": "pésti", "attributesToRetrieve": ["*"], "attributesToCrop": ["*"] }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                    assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
                    {
                      "id": 852,
                      "cattos": "pésti",
                      "_formatted": {
                        "id": "852",
                        "cattos": "pésti"
                      }
                    }
                    "###);
                }
            },
        )
        .await;

    index
        .search(json!({ "q": "pésti", "attributesToCrop": ["*"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "id": 852,
                  "cattos": "pésti",
                  "_formatted": {
                    "id": "852",
                    "cattos": "pésti"
                  }
                }
                "###)
            }
        })
        .await;
}

#[actix_rt::test]
async fn format_nested() {
    let index = shared_index_with_nested_documents().await;

    index
        .search(json!({ "q": "pésti", "attributesToRetrieve": ["doggos"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "doggos": [
                    {
                      "name": "bobby",
                      "age": 2
                    },
                    {
                      "name": "buddy",
                      "age": 4
                    }
                  ]
                }
                "###)
            }
        })
        .await;

    index
        .search(
            json!({ "q": "pésti", "attributesToRetrieve": ["doggos.name"] }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                    assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
                    {
                      "doggos": [
                        {
                          "name": "bobby"
                        },
                        {
                          "name": "buddy"
                        }
                      ]
                    }
                    "###)
                }
            },
        )
        .await;

    index
        .search(
            json!({ "q": "bobby", "attributesToRetrieve": ["doggos.name"], "showMatchesPosition": true }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                    assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
                    {
                      "doggos": [
                        {
                          "name": "bobby"
                        },
                        {
                          "name": "buddy"
                        }
                      ],
                      "_matchesPosition": {
                        "doggos.name": [
                          {
                            "start": 0,
                            "length": 5,
                            "indices": [
                              0
                            ]
                          }
                        ]
                      }
                    }
                    "###)
                }
            }
        )
        .await;

    index
        .search(json!({ "q": "pésti", "attributesToRetrieve": [], "attributesToHighlight": ["doggos.name"] }),
        |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "_formatted": {
                    "doggos": [
                      {
                        "name": "bobby"
                      },
                      {
                        "name": "buddy"
                      }
                    ]
                  }
                }
                "###)
            }
        })
        .await;

    index
        .search(json!({ "q": "pésti", "attributesToRetrieve": [], "attributesToCrop": ["doggos.name"] }),
        |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "_formatted": {
                    "doggos": [
                      {
                        "name": "bobby"
                      },
                      {
                        "name": "buddy"
                      }
                    ]
                  }
                }
                "###)
            }
        })
        .await;

    index
        .search(json!({ "q": "pésti", "attributesToRetrieve": ["doggos.name"], "attributesToHighlight": ["doggos.age"] }),
        |response, code| {
    assert_eq!(code, 200, "{}", response);
    allow_duplicates! {
        assert_json_snapshot!(response["hits"][0],
        { "._rankingScore" => "[score]" },
        @r###"
        {
          "doggos": [
            {
              "name": "bobby"
            },
            {
              "name": "buddy"
            }
          ],
          "_formatted": {
            "doggos": [
              {
                "name": "bobby",
                "age": "2"
              },
              {
                "name": "buddy",
                "age": "4"
              }
            ]
          }
        }
        "###)
    }
    })
        .await;

    index
        .search(json!({ "q": "pésti", "attributesToRetrieve": [], "attributesToHighlight": ["doggos.age"], "attributesToCrop": ["doggos.name"] }),
        |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                    assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
                    {
                      "_formatted": {
                        "doggos": [
                          {
                            "name": "bobby",
                            "age": "2"
                          },
                          {
                            "name": "buddy",
                            "age": "4"
                          }
                        ]
                      }
                    }
                    "###)
                }
            }
        )
        .await;
}

#[actix_rt::test]
async fn displayedattr_2_smol() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // not enough displayed for the other settings
    index.update_settings(json!({ "displayedAttributes": ["id"] })).await;

    let documents = NESTED_DOCUMENTS.clone();
    let (response, _) = index.add_documents(documents, None).await;
    index.wait_task(response.uid()).await.succeeded();

    index
        .search(json!({ "attributesToRetrieve": ["father", "id"], "attributesToHighlight": ["mother"], "attributesToCrop": ["cattos"] }),
        |response, code| {
    assert_eq!(code, 200, "{}", response);
    allow_duplicates! {
        assert_json_snapshot!(response["hits"][0],
        { "._rankingScore" => "[score]" },
        @r###"
        {
          "id": 852
        }
        "###)
    }
        })
        .await;

    index
        .search(json!({ "attributesToRetrieve": ["id"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "id": 852
                }
                "###)
            }
        })
        .await;

    index
        .search(json!({ "attributesToHighlight": ["id"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "id": 852,
                  "_formatted": {
                    "id": "852"
                  }
                }
                "###)
            }
        })
        .await;

    index
        .search(json!({ "attributesToCrop": ["id"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "id": 852,
                  "_formatted": {
                    "id": "852"
                  }
                }
                "###)
            }
        })
        .await;

    index
        .search(
            json!({ "attributesToHighlight": ["id"], "attributesToCrop": ["id"] }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                    assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
                    {
                      "id": 852,
                      "_formatted": {
                        "id": "852"
                      }
                    }
                    "###)
                }
            },
        )
        .await;

    index
        .search(json!({ "attributesToHighlight": ["cattos"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "id": 852
                }
                "###)
            }
        })
        .await;

    index
        .search(json!({ "attributesToCrop": ["cattos"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @r###"
                {
                  "id": 852
                }
                "###)
            }
        })
        .await;

    index
        .search(json!({ "attributesToRetrieve": ["cattos"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            allow_duplicates! {
                assert_json_snapshot!(response["hits"][0],
                { "._rankingScore" => "[score]" },
                @"{}")
            }
        })
        .await;

    index
        .search(
            json!({ "attributesToRetrieve": ["cattos"], "attributesToHighlight": ["cattos"], "attributesToCrop": ["cattos"] }),
            |response, code| {
    assert_eq!(code, 200, "{}", response);
    allow_duplicates! {
        assert_json_snapshot!(response["hits"][0],
        { "._rankingScore" => "[score]" },
        @"{}")
    }

            }
        )
        .await;

    index
        .search(
            json!({ "attributesToRetrieve": ["cattos"], "attributesToHighlight": ["id"] }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                    assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
                    {
                      "_formatted": {
                        "id": "852"
                      }
                    }
                    "###)
                }
            },
        )
        .await;

    index
        .search(
            json!({ "attributesToRetrieve": ["cattos"], "attributesToCrop": ["id"] }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                allow_duplicates! {
                    assert_json_snapshot!(response["hits"][0],
                    { "._rankingScore" => "[score]" },
                    @r###"
                    {
                      "_formatted": {
                        "id": "852"
                      }
                    }
                    "###)
                }
            },
        )
        .await;
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn test_cjk_highlight() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
        { "id": 0, "title": "この度、クーポンで無料で頂きました。" },
        { "id": 1, "title": "大卫到了扫罗那里" },
    ]);
    let (response, _) = index.add_documents(documents, None).await;
    index.wait_task(response.uid()).await.succeeded();

    index
        .search(json!({"q": "で", "attributesToHighlight": ["title"]}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(
                response["hits"][0]["_formatted"]["title"],
                json!("この度、クーポン<em>で</em>無料<em>で</em>頂きました。")
            );
        })
        .await;

    index
        .search(json!({"q": "大卫", "attributesToHighlight": ["title"]}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(
                response["hits"][0]["_formatted"]["title"],
                json!("<em>大卫</em>到了扫罗那里")
            );
        })
        .await;
}
