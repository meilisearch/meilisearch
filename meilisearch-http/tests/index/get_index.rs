use crate::common::Server;
use serde_json::json;
use serde_json::Value;

#[actix_rt::test]
async fn create_and_get_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;

    assert_eq!(code, 202);

    index.wait_task(0).await;

    let (response, code) = index.get().await;

    assert_eq!(code, 200);
    assert_eq!(response["uid"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());
    assert_eq!(response["createdAt"], response["updatedAt"]);
    assert_eq!(response["primaryKey"], Value::Null);
    assert_eq!(response.as_object().unwrap().len(), 4);
}

#[actix_rt::test]
async fn error_get_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.get().await;

    let expected_response = json!({
        "message": "Index `test` not found.",
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
    let server = Server::new().await;
    server.index("test").create(None).await;
    server.index("test1").create(Some("key")).await;

    server.index("test").wait_task(1).await;

    let (response, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200);
    assert!(response["results"].is_array());
    let arr = response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr
        .iter()
        .any(|entry| entry["uid"] == "test" && entry["primaryKey"] == Value::Null));
    assert!(arr
        .iter()
        .any(|entry| entry["uid"] == "test1" && entry["primaryKey"] == "key"));
}

#[actix_rt::test]
async fn get_and_paginate_indexes() {
    let server = Server::new().await;
    const NB_INDEXES: usize = 50;
    for i in 0..NB_INDEXES {
        server.index(&format!("test_{i:02}")).create(None).await;
    }

    server
        .index(&format!("test_{NB_INDEXES}"))
        .wait_task(NB_INDEXES as u64 - 1)
        .await;

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
    let server = Server::new().await;
    let index = server.index("this is not a valid index name");
    let (response, code) = index.get().await;

    assert_eq!(code, 404);
    assert_eq!(
        response,
        json!(
        {
        "message": "Index `this is not a valid index name` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
            })
    );
}
