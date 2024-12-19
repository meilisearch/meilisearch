use meili_snap::{json_string, snapshot};
use once_cell::sync::Lazy;

use crate::common::Server;
use crate::json;

static DOCUMENTS: Lazy<crate::common::Value> = Lazy::new(|| {
    json!([
        {
            "id": 1,
            "a": "Soup of the day",
            "b": "manythefishou",
        },
        {
            "id": 2,
            "a": "Soup of day so",
            "b": "manythe manythelazyfish",
        },
        {
            "id": 3,
            "a": "the Soup of day",
            "b": "manythelazyfish",
        },
    ])
});

#[actix_rt::test]
async fn add_docs_and_disable() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, _code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
            "prefixSearch": "disabled",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(response.uid()).await;

    // only 1 document should match
    index
        .search(json!({"q": "so", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "Soup of day <em>so</em>",
                  "b": "manythe manythelazyfish"
                }
              }
            ]
            "###);
        })
        .await;

    // only 1 document should match
    index
        .search(json!({"q": "manythe", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "Soup of day so",
                  "b": "<em>manythe</em> manythelazyfish"
                }
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn disable_and_add_docs() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
            "prefixSearch": "disabled",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(response.uid()).await;

    let (response, _code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(response.uid()).await;

    // only 1 document should match
    index
        .search(json!({"q": "so", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "Soup of day <em>so</em>",
                  "b": "manythe manythelazyfish"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "manythe", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "Soup of day so",
                  "b": "<em>manythe</em> manythelazyfish"
                }
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn disable_add_docs_and_enable() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
            "prefixSearch": "disabled",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(response.uid()).await;

    let (response, _code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
            "prefixSearch": "indexingTime",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(2).await;

    // all documents should match
    index
        .search(json!({"q": "so", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "manythefishou",
                "_formatted": {
                  "id": "1",
                  "a": "<em>So</em>up of the day",
                  "b": "manythefishou"
                }
              },
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "<em>So</em>up of day <em>so</em>",
                  "b": "manythe manythelazyfish"
                }
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "manythelazyfish",
                "_formatted": {
                  "id": "3",
                  "a": "the <em>So</em>up of day",
                  "b": "manythelazyfish"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "manythe", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "manythefishou",
                "_formatted": {
                  "id": "1",
                  "a": "Soup of the day",
                  "b": "<em>manythe</em>fishou"
                }
              },
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "Soup of day so",
                  "b": "<em>manythe</em> <em>manythe</em>lazyfish"
                }
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "manythelazyfish",
                "_formatted": {
                  "id": "3",
                  "a": "the Soup of day",
                  "b": "<em>manythe</em>lazyfish"
                }
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn disable_add_docs_and_reset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
            "prefixSearch": "disabled",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(response.uid()).await;

    let (response, _code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(response.uid()).await;

    let (response, code) = index
        .update_settings(json!({
            "prefixSearch": serde_json::Value::Null,
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(2).await;

    // all documents should match
    index
        .search(json!({"q": "so", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "manythefishou",
                "_formatted": {
                  "id": "1",
                  "a": "<em>So</em>up of the day",
                  "b": "manythefishou"
                }
              },
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "<em>So</em>up of day <em>so</em>",
                  "b": "manythe manythelazyfish"
                }
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "manythelazyfish",
                "_formatted": {
                  "id": "3",
                  "a": "the <em>So</em>up of day",
                  "b": "manythelazyfish"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "manythe", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "manythefishou",
                "_formatted": {
                  "id": "1",
                  "a": "Soup of the day",
                  "b": "<em>manythe</em>fishou"
                }
              },
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "Soup of day so",
                  "b": "<em>manythe</em> <em>manythe</em>lazyfish"
                }
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "manythelazyfish",
                "_formatted": {
                  "id": "3",
                  "a": "the Soup of day",
                  "b": "<em>manythe</em>lazyfish"
                }
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn default_behavior() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(response.uid()).await;

    let (response, _code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(response.uid()).await;

    // all documents should match
    index
        .search(json!({"q": "so", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "manythefishou",
                "_formatted": {
                  "id": "1",
                  "a": "<em>So</em>up of the day",
                  "b": "manythefishou"
                }
              },
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "<em>So</em>up of day <em>so</em>",
                  "b": "manythe manythelazyfish"
                }
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "manythelazyfish",
                "_formatted": {
                  "id": "3",
                  "a": "the <em>So</em>up of day",
                  "b": "manythelazyfish"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "manythe", "attributesToHighlight": ["a", "b"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "manythefishou",
                "_formatted": {
                  "id": "1",
                  "a": "Soup of the day",
                  "b": "<em>manythe</em>fishou"
                }
              },
              {
                "id": 2,
                "a": "Soup of day so",
                "b": "manythe manythelazyfish",
                "_formatted": {
                  "id": "2",
                  "a": "Soup of day so",
                  "b": "<em>manythe</em> <em>manythe</em>lazyfish"
                }
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "manythelazyfish",
                "_formatted": {
                  "id": "3",
                  "a": "the Soup of day",
                  "b": "<em>manythe</em>lazyfish"
                }
              }
            ]
            "###);
        })
        .await;
}
