use meili_snap::{json_string, snapshot};
use once_cell::sync::Lazy;

use crate::common::Server;
use crate::json;

static DOCUMENTS: Lazy<crate::common::Value> = Lazy::new(|| {
    json!([
        {
            "id": 1,
            "meta": {
                "title": "Soup of the day",
                "description": "many the fish",
            }
        },
        {
            "id": 2,
            "meta": {
                "title": "Soup of day",
                "description": "many the lazy fish",
            }
        },
        {
            "id": 3,
            "meta": {
                "title": "the Soup of day",
                "description": "many the fish",
            }
        },
    ])
});

#[actix_rt::test]
async fn nested_field_becomes_searchable() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _status_code) = index.add_documents(DOCUMENTS.clone(), None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
            "searchableAttributes": ["meta.title"]
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    // We expect no documents when searching for
    // a nested non-searchable field
    index
        .search(json!({"q": "many fish"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"[]"###);
        })
        .await;

    let (response, code) = index
        .update_settings(json!({
            "searchableAttributes": ["meta.title", "meta.description"]
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    // We expect all the documents when the nested field becomes searchable
    index
        .search(json!({"q": "many fish"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "meta": {
                  "title": "Soup of the day",
                  "description": "many the fish"
                }
              },
              {
                "id": 3,
                "meta": {
                  "title": "the Soup of day",
                  "description": "many the fish"
                }
              },
              {
                "id": 2,
                "meta": {
                  "title": "Soup of day",
                  "description": "many the lazy fish"
                }
              }
            ]
            "###);
        })
        .await;

    let (response, code) = index
        .update_settings(json!({
            "searchableAttributes": ["meta.title"]
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    // We expect no documents when searching for
    // a nested non-searchable field
    index
        .search(json!({"q": "many fish"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"[]"###);
        })
        .await;
}
