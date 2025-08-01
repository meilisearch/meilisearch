use meili_snap::{json_string, snapshot};
use serde_json::Value;

use crate::common::{shared_does_not_exists_index, Server};
use crate::json;

#[actix_rt::test]
async fn create_and_get_index() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.get().await;

    assert_eq!(code, 200);
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());
    assert_eq!(response["createdAt"], response["updatedAt"]);
    assert_eq!(response["primaryKey"], Value::Null);
    assert_eq!(response.as_object().unwrap().len(), 4);
}

#[actix_rt::test]
async fn error_get_unexisting_index() {
    let index = shared_does_not_exists_index().await;

    let (response, code) = index.get().await;

    let expected_response = json!({
        "message": "Index `DOES_NOT_EXISTS` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn no_index_return_empty_list() {
    let server = Server::new().await;
    let (response, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);
    assert!(response["results"].is_array());
    assert!(response["results"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn list_multiple_indexes() {
    let server = Server::new_shared();

    let index_without_key = server.unique_index();
    let (response_without_key, _status_code) = index_without_key.create(None).await;

    let index_with_key = server.unique_index();
    let (response_with_key, _status_code) = index_with_key.create(Some("key")).await;

    server.wait_task(response_without_key.uid()).await.succeeded();
    server.wait_task(response_with_key.uid()).await.succeeded();

    let (response, code) = server.list_indexes(None, Some(1000)).await;
    assert_eq!(code, 200);
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert!(arr.len() >= 2, "Expected at least 2 indexes.");
    assert!(arr
        .iter()
        .any(|entry| entry["uid"] == index_without_key.uid && entry["primaryKey"] == Value::Null));
    assert!(arr
        .iter()
        .any(|entry| entry["uid"] == index_with_key.uid && entry["primaryKey"] == "key"));
}

#[actix_rt::test]
async fn get_and_paginate_indexes() {
    let server = Server::new().await;
    const NB_INDEXES: usize = 50;
    for i in 0..NB_INDEXES {
        let (task, code) = server.index(format!("test_{i:02}")).create(None).await;
        assert_eq!(code, 202);
        server.wait_task(task.uid()).await;
    }

    // basic
    let (response, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["total"], json!(NB_INDEXES));
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 20);
    // ensuring we get all the indexes in the alphabetical order
    assert!((0..20)
        .map(|idx| format!("test_{idx:02}"))
        .zip(arr)
        .all(|(expected, entry)| entry["uid"] == expected));

    // with an offset
    let (response, code) = server.list_indexes(Some(15), None).await;
    assert_eq!(code, 200);
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["offset"], json!(15));
    assert_eq!(response["total"], json!(NB_INDEXES));
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 20);
    assert!((15..35)
        .map(|idx| format!("test_{idx:02}"))
        .zip(arr)
        .all(|(expected, entry)| entry["uid"] == expected));

    // with an offset and not enough elements
    let (response, code) = server.list_indexes(Some(45), None).await;
    assert_eq!(code, 200);
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["offset"], json!(45));
    assert_eq!(response["total"], json!(NB_INDEXES));
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 5);
    assert!((45..50)
        .map(|idx| format!("test_{idx:02}"))
        .zip(arr)
        .all(|(expected, entry)| entry["uid"] == expected));

    // with a limit lower than the default
    let (response, code) = server.list_indexes(None, Some(5)).await;
    assert_eq!(code, 200);
    assert_eq!(response["limit"], json!(5));
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["total"], json!(NB_INDEXES));
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 5);
    assert!((0..5)
        .map(|idx| format!("test_{idx:02}"))
        .zip(arr)
        .all(|(expected, entry)| entry["uid"] == expected));

    // with a limit higher than the default
    let (response, code) = server.list_indexes(None, Some(40)).await;
    assert_eq!(code, 200);
    assert_eq!(response["limit"], json!(40));
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["total"], json!(NB_INDEXES));
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 40);
    assert!((0..40)
        .map(|idx| format!("test_{idx:02}"))
        .zip(arr)
        .all(|(expected, entry)| entry["uid"] == expected));

    // with a limit higher than the default
    let (response, code) = server.list_indexes(None, Some(80)).await;
    assert_eq!(code, 200);
    assert_eq!(response["limit"], json!(80));
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["total"], json!(NB_INDEXES));
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 50);
    assert!((0..50)
        .map(|idx| format!("test_{idx:02}"))
        .zip(arr)
        .all(|(expected, entry)| entry["uid"] == expected));

    // with a limit and an offset
    let (response, code) = server.list_indexes(Some(20), Some(10)).await;
    assert_eq!(code, 200);
    assert_eq!(response["limit"], json!(10));
    assert_eq!(response["offset"], json!(20));
    assert_eq!(response["total"], json!(NB_INDEXES));
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 10);
    assert!((20..30)
        .map(|idx| format!("test_{idx:02}"))
        .zip(arr)
        .all(|(expected, entry)| entry["uid"] == expected));
}

#[actix_rt::test]
async fn get_invalid_index_uid() {
    let server = Server::new_shared();
    let (response, code) =
        server.create_index_fail(json!({ "uid": "this is not a valid index name" })).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.uid`: `this is not a valid index name` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}
