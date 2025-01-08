use meili_snap::{json_string, snapshot};

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn set_and_reset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _code) = index
        .update_settings(json!({
            "nonSeparatorTokens": ["#", "&"],
            "separatorTokens": ["&sep", "<br/>"],
            "dictionary": ["J.R.R.", "J. R. R."],
        }))
        .await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, _) = index.settings().await;
    snapshot!(json_string!(response["nonSeparatorTokens"]), @r###"
    [
      "#",
      "&"
    ]
    "###);
    snapshot!(json_string!(response["separatorTokens"]), @r###"
    [
      "&sep",
      "<br/>"
    ]
    "###);
    snapshot!(json_string!(response["dictionary"]), @r###"
    [
      "J. R. R.",
      "J.R.R."
    ]
    "###);

    let (task, _status_code) = index
        .update_settings(json!({
            "nonSeparatorTokens": null,
            "separatorTokens": null,
            "dictionary": null,
        }))
        .await;

    index.wait_task(task.uid()).await.succeeded();

    let (response, _) = index.settings().await;
    snapshot!(json_string!(response["nonSeparatorTokens"]), @"[]");
    snapshot!(json_string!(response["separatorTokens"]), @"[]");
    snapshot!(json_string!(response["dictionary"]), @"[]");
}

#[actix_rt::test]
async fn set_and_search() {
    let documents = json!([
        {
            "id": 1,
            "content": "Mac & cheese",
        },
        {
            "id": 2,
            "content": "G#D#G#D#G#C#D#G#C#",
        },
        {
            "id": 3,
            "content": "Mac&sep&&sepcheese",
        },
    ]);

    let server = Server::new().await;
    let index = server.index("test");

    let (add_task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(add_task.uid()).await.succeeded();

    let (update_task, _code) = index
        .update_settings(json!({
            "nonSeparatorTokens": ["#", "&"],
            "separatorTokens": ["<br/>", "&sep"],
            "dictionary": ["#", "A#", "B#", "C#", "D#", "E#", "F#", "G#"],
        }))
        .await;
    index.wait_task(update_task.uid()).await.succeeded();

    index
        .search(json!({"q": "&", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "content": "Mac & cheese",
                "_formatted": {
                  "id": "1",
                  "content": "Mac <em>&</em> cheese"
                }
              },
              {
                "id": 3,
                "content": "Mac&sep&&sepcheese",
                "_formatted": {
                  "id": "3",
                  "content": "Mac&sep<em>&</em>&sepcheese"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(
            json!({"q": "Mac & cheese", "attributesToHighlight": ["content"]}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 1,
                    "content": "Mac & cheese",
                    "_formatted": {
                      "id": "1",
                      "content": "<em>Mac</em> <em>&</em> <em>cheese</em>"
                    }
                  },
                  {
                    "id": 3,
                    "content": "Mac&sep&&sepcheese",
                    "_formatted": {
                      "id": "3",
                      "content": "<em>Mac</em>&sep<em>&</em>&sep<em>cheese</em>"
                    }
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({"q": "Mac&sep&&sepcheese", "attributesToHighlight": ["content"]}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 1,
                    "content": "Mac & cheese",
                    "_formatted": {
                      "id": "1",
                      "content": "<em>Mac</em> <em>&</em> <em>cheese</em>"
                    }
                  },
                  {
                    "id": 3,
                    "content": "Mac&sep&&sepcheese",
                    "_formatted": {
                      "id": "3",
                      "content": "<em>Mac</em>&sep<em>&</em>&sep<em>cheese</em>"
                    }
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(json!({"q": "C#D#G", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "content": "G#D#G#D#G#C#D#G#C#",
                "_formatted": {
                  "id": "2",
                  "content": "<em>G</em>#<em>D#</em><em>G</em>#<em>D#</em><em>G</em>#<em>C#</em><em>D#</em><em>G</em>#<em>C#</em>"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "#", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @"[]");
        })
        .await;
}

#[actix_rt::test]
async fn advanced_synergies() {
    let documents = json!([
        {
            "id": 1,
            "content": "J.R.R. Tolkien",
        },
        {
            "id": 2,
            "content": "J. R. R. Tolkien",
        },
        {
            "id": 3,
            "content": "jrr Tolkien",
        },
        {
            "id": 4,
            "content": "J.K. Rowlings",
        },
        {
            "id": 5,
            "content": "J. K. Rowlings",
        },
        {
            "id": 6,
            "content": "jk Rowlings",
        },
    ]);

    let server = Server::new().await;
    let index = server.index("test");

    let (add_task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(add_task.uid()).await.succeeded();

    let (update_task, _code) = index
        .update_settings(json!({
            "dictionary": ["J.R.R.", "J. R. R."],
            "synonyms": {
                "J.R.R.": ["jrr", "J. R. R."],
                "J. R. R.": ["jrr", "J.R.R."],
                "jrr": ["J.R.R.", "J. R. R."],
                "J.K.": ["jk", "J. K."],
                "J. K.": ["jk", "J.K."],
                "jk": ["J.K.", "J. K."],
            }
        }))
        .await;
    index.wait_task(update_task.uid()).await.succeeded();

    index
        .search(json!({"q": "J.R.R.", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "content": "J.R.R. Tolkien",
                "_formatted": {
                  "id": "1",
                  "content": "<em>J.R.R.</em> Tolkien"
                }
              },
              {
                "id": 2,
                "content": "J. R. R. Tolkien",
                "_formatted": {
                  "id": "2",
                  "content": "<em>J. R. R.</em> Tolkien"
                }
              },
              {
                "id": 3,
                "content": "jrr Tolkien",
                "_formatted": {
                  "id": "3",
                  "content": "<em>jrr</em> Tolkien"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "jrr", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 3,
                "content": "jrr Tolkien",
                "_formatted": {
                  "id": "3",
                  "content": "<em>jrr</em> Tolkien"
                }
              },
              {
                "id": 1,
                "content": "J.R.R. Tolkien",
                "_formatted": {
                  "id": "1",
                  "content": "<em>J.R.R.</em> Tolkien"
                }
              },
              {
                "id": 2,
                "content": "J. R. R. Tolkien",
                "_formatted": {
                  "id": "2",
                  "content": "<em>J. R. R.</em> Tolkien"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "J. R. R.", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "content": "J. R. R. Tolkien",
                "_formatted": {
                  "id": "2",
                  "content": "<em>J. R. R.</em> Tolkien"
                }
              },
              {
                "id": 1,
                "content": "J.R.R. Tolkien",
                "_formatted": {
                  "id": "1",
                  "content": "<em>J.R.R.</em> Tolkien"
                }
              },
              {
                "id": 3,
                "content": "jrr Tolkien",
                "_formatted": {
                  "id": "3",
                  "content": "<em>jrr</em> Tolkien"
                }
              }
            ]
            "###);
        })
        .await;

    // Only update dictionary, the synonyms should be recomputed.
    let (_response, _code) = index
        .update_settings(json!({
            "dictionary": ["J.R.R.", "J. R. R.", "J.K.", "J. K."],
        }))
        .await;
    index.wait_task(_response.uid()).await.succeeded();

    index
        .search(json!({"q": "jk", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 6,
                "content": "jk Rowlings",
                "_formatted": {
                  "id": "6",
                  "content": "<em>jk</em> Rowlings"
                }
              },
              {
                "id": 4,
                "content": "J.K. Rowlings",
                "_formatted": {
                  "id": "4",
                  "content": "<em>J.K.</em> Rowlings"
                }
              },
              {
                "id": 5,
                "content": "J. K. Rowlings",
                "_formatted": {
                  "id": "5",
                  "content": "<em>J. K.</em> Rowlings"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "J.K.", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 4,
                "content": "J.K. Rowlings",
                "_formatted": {
                  "id": "4",
                  "content": "<em>J.K.</em> Rowlings"
                }
              },
              {
                "id": 5,
                "content": "J. K. Rowlings",
                "_formatted": {
                  "id": "5",
                  "content": "<em>J. K.</em> Rowlings"
                }
              },
              {
                "id": 6,
                "content": "jk Rowlings",
                "_formatted": {
                  "id": "6",
                  "content": "<em>jk</em> Rowlings"
                }
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "J. K.", "attributesToHighlight": ["content"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 5,
                "content": "J. K. Rowlings",
                "_formatted": {
                  "id": "5",
                  "content": "<em>J. K.</em> Rowlings"
                }
              },
              {
                "id": 4,
                "content": "J.K. Rowlings",
                "_formatted": {
                  "id": "4",
                  "content": "<em>J.K.</em> Rowlings"
                }
              },
              {
                "id": 6,
                "content": "jk Rowlings",
                "_formatted": {
                  "id": "6",
                  "content": "<em>jk</em> Rowlings"
                }
              },
              {
                "id": 2,
                "content": "J. R. R. Tolkien",
                "_formatted": {
                  "id": "2",
                  "content": "<em>J. R.</em> R. Tolkien"
                }
              }
            ]
            "###);
        })
        .await;
}
