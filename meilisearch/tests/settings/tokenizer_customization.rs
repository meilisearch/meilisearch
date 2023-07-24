use meili_snap::{json_string, snapshot};
use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn set_and_reset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, _code) = index
        .update_settings(json!({
            "nonSeparatorTokens": ["#", "&"],
            "separatorTokens": ["&sep", "<br/>"],
            "dictionary": ["J.R.R.", "J. R. R."],
        }))
        .await;
    index.wait_task(0).await;

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

    index
        .update_settings(json!({
            "nonSeparatorTokens": null,
            "separatorTokens": null,
            "dictionary": null,
        }))
        .await;

    index.wait_task(1).await;

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

    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    let (_response, _code) = index
        .update_settings(json!({
            "nonSeparatorTokens": ["#", "&"],
            "separatorTokens": ["<br/>", "&sep"],
            "dictionary": ["#", "A#", "B#", "C#", "D#", "E#", "F#", "G#"],
        }))
        .await;
    index.wait_task(1).await;

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
