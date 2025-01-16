use meili_snap::{json_string, snapshot};
use once_cell::sync::Lazy;

use crate::common::Server;
use crate::json;

static DOCUMENTS: Lazy<crate::common::Value> = Lazy::new(|| {
    json!([
        {
            "id": 1,
            "a": "Soup of the day",
            "b": "many the fish",
        },
        {
            "id": 2,
            "a": "Soup of day",
            "b": "many the lazy fish",
        },
        {
            "id": 3,
            "a": "the Soup of day",
            "b": "many the fish",
        },
    ])
});

#[actix_rt::test]
async fn attribute_scale_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _status_code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
            "proximityPrecision": "byAttribute",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();

    // the expected order is [1, 3, 2] instead of [3, 1, 2]
    // because the attribute scale doesn't make the difference between 1 and 3.
    index
        .search(json!({"q": "the soup of day"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              }
            ]
            "###);
        })
        .await;

    // the expected order is [1, 2, 3] instead of [1, 3, 2]
    // because the attribute scale sees all the word in the same attribute
    // and so doesn't make the difference between the documents.
    index
        .search(json!({"q": "many the fish"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn attribute_scale_phrase_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _status_code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (task, _code) = index
        .update_settings(json!({
            "proximityPrecision": "byAttribute",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    index.wait_task(task.uid()).await.succeeded();

    // the expected order is [1, 3] instead of [3, 1]
    // because the attribute scale doesn't make the difference between 1 and 3.
    // But 2 shouldn't be returned because "the" is not in the same attribute.
    index
        .search(json!({"q": "\"the soup of day\""}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              }
            ]
            "###);
        })
        .await;

    // the expected order is [1, 2, 3] instead of [1, 3]
    // because the attribute scale sees all the word in the same attribute
    // and so doesn't make the difference between the documents.
    index
        .search(json!({"q": "\"many the fish\""}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn word_scale_set_and_reset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _status_code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(task.uid()).await.succeeded();

    // Set and reset the setting ensuring the swap between the 2 settings is applied.
    let (update_task1, _code) = index
        .update_settings(json!({
            "proximityPrecision": "byAttribute",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    index.wait_task(update_task1.uid()).await.succeeded();

    let (update_task2, _code) = index
        .update_settings(json!({
            "proximityPrecision": "byWord",
            "rankingRules": ["words", "typo", "proximity"],
        }))
        .await;
    index.wait_task(update_task2.uid()).await.succeeded();

    // [3, 1, 2]
    index
        .search(json!({"q": "the soup of day"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              },
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              }
            ]
            "###);
        })
        .await;

    // [1, 3, 2]
    index
        .search(json!({"q": "many the fish"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              }
            ]
            "###);
        })
        .await;

    // [3]
    index
        .search(json!({"q": "\"the soup of day\""}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              }
            ]
            "###);
        })
        .await;

    // [1, 3]
    index
        .search(json!({"q": "\"many the fish\""}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn attribute_scale_default_ranking_rules() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _status_code) = index.add_documents(DOCUMENTS.clone(), None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
            "proximityPrecision": "byAttribute"
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();

    // the expected order is [3, 1, 2]
    index
        .search(json!({"q": "the soup of day"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              },
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              }
            ]
            "###);
        })
        .await;

    // the expected order is [1, 3, 2] instead of [1, 3]
    // because the attribute scale sees all the word in the same attribute
    // and so doesn't remove the document 2.
    index
        .search(json!({"q": "\"many the fish\""}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              }
            ]
            "###);
        })
        .await;
}
