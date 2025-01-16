use meili_snap::{json_string, snapshot};
use once_cell::sync::Lazy;

use crate::common::index::Index;
use crate::common::{Server, Value};
use crate::json;

async fn index_with_documents<'a>(server: &'a Server, documents: &Value) -> Index<'a> {
    let index = server.index("test");

    let (task, _code) = index.add_documents(documents.clone(), None).await;
    index.wait_task(task.uid()).await.succeeded();
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
                snapshot!(code, @"200 OK");
                snapshot!(response["hits"].as_array().unwrap().len(), @"2");
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_no_searchable_attribute_set() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["unknown"]}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(response["hits"].as_array().unwrap().len(), @"0");
            },
        )
        .await;

    let (task, _status_code) = index.update_settings_searchable_attributes(json!(["*"])).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["unknown"]}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(response["hits"].as_array().unwrap().len(), @"0");
            },
        )
        .await;

    let (task, _status_code) = index.update_settings_searchable_attributes(json!(["*"])).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["unknown", "title"]}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(response["hits"].as_array().unwrap().len(), @"2");
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_on_all_attributes() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    index
        .search(json!({"q": "Captain Marvel", "attributesToSearchOn": ["*"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"].as_array().unwrap().len(), @"3");
        })
        .await;
}

#[actix_rt::test]
async fn search_on_all_attributes_restricted_set() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;
    let (task, _status_code) = index.update_settings_searchable_attributes(json!(["title"])).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "Captain Marvel", "attributesToSearchOn": ["*"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"].as_array().unwrap().len(), @"2");
        })
        .await;
}

#[actix_rt::test]
async fn simple_prefix_search_on_title() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    // simple search should return 2 documents (ids: 2 and 3).
    index
        .search(json!({"q": "Captain Mar", "attributesToSearchOn": ["title"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"].as_array().unwrap().len(), @"2");
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
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"].as_array().unwrap().len(), @"1");
        })
        .await;
}

#[actix_rt::test]
async fn simple_search_on_no_field() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;
    // simple search on no field shouldn't return any document.
    index
        .search(json!({"q": "Captain Marvel", "attributesToSearchOn": []}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"].as_array().unwrap().len(), @"0");
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
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]),
                    @r###"
                [
                  {
                    "id": "3"
                  },
                  {
                    "id": "2"
                  }
                ]
                "###
                );
            },
        )
        .await;
}

#[actix_rt::test]
async fn word_ranking_rule_order_exact_words() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;
    let (task, _status_code) = index
        .update_settings_typo_tolerance(json!({"disableOnWords": ["Captain", "Marvel"]}))
        .await;
    index.wait_task(task.uid()).await.succeeded();

    // simple search should return 2 documents (ids: 2 and 3).
    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["title"], "attributesToRetrieve": ["id"]}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]),
                    @r###"
                [
                  {
                    "id": "3"
                  },
                  {
                    "id": "2"
                  }
                ]
                "###
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]),
                @r###"
            [
              {
                "id": "2"
              },
              {
                "id": "1"
              }
            ]
            "###
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
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]),
                @r###"
            [
              {
                "id": "1"
              },
              {
                "id": "2"
              }
            ]
            "###
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
            "desc": "Captain the Marvel",
            "id": "2",
        }]),
    )
    .await;

    // Document 2 should appear before document 1.
    index
        .search(json!({"q": "Captain Marvel", "attributesToRetrieve": ["id"], "attributesToSearchOn": ["desc"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]),
                @r###"
            [
              {
                "id": "2"
              },
              {
                "id": "1"
              }
            ]
            "###
            );
        })
        .await;
}

#[actix_rt::test]
async fn search_on_exact_field() {
    let server = Server::new().await;
    let index = index_with_documents(
        &server,
        &json!([
        {
            "title": "Captain Marvel",
            "exact": "Captain Marivel",
            "id": "1",
        },
        {
            "title": "Captain Marivel",
            "exact": "Captain the Marvel",
            "id": "2",
        }]),
    )
    .await;

    let (response, code) =
        index.update_settings_typo_tolerance(json!({ "disableOnAttributes": ["exact"] })).await;
    assert_eq!(202, code, "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();
    // Searching on an exact attribute should only return the document matching without typo.
    index
        .search(json!({"q": "Marvel", "attributesToSearchOn": ["exact"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"].as_array().unwrap().len(), @"1");
        })
        .await;
}

#[actix_rt::test]
async fn phrase_search_on_title() {
    let server = Server::new().await;
    let documents = json!([
      { "id": 8, "desc": "Document Review", "title": "Document Review Specialist II" },
      { "id": 5, "desc": "Document Review", "title": "Document Review Attorney" },
      { "id": 4, "desc": "Document Review", "title": "Document Review Manager - Cyber Incident Response (Remote)" },
      { "id": 3, "desc": "Document Review", "title": "Document Review Paralegal" },
      { "id": 2, "desc": "Document Review", "title": "Document Controller (Saudi National)" },
      { "id": 1, "desc": "Document Review", "title": "Document Reviewer" },
      { "id": 7, "desc": "Document Review", "title": "Document Review Specialist II" },
      { "id": 6, "desc": "Document Review", "title": "Document Review (Entry Level)" }
    ]);
    let index = index_with_documents(&server, &documents).await;

    index
        .search(
            json!({"q": "\"Document Review\"", "attributesToSearchOn": ["title"], "attributesToRetrieve": ["title"]}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Document Review Specialist II"
                  },
                  {
                    "title": "Document Review Attorney"
                  },
                  {
                    "title": "Document Review Manager - Cyber Incident Response (Remote)"
                  },
                  {
                    "title": "Document Review Paralegal"
                  },
                  {
                    "title": "Document Review Specialist II"
                  },
                  {
                    "title": "Document Review (Entry Level)"
                  }
                ]
                "###);
            },
        )
        .await;
}
