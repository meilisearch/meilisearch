use serde_json::json;
use serde_json::Value;
use assert_json_diff::assert_json_include;

mod common;

#[actix_rt::test]
async fn check_first_update_should_bring_up_processed_status_after_first_docs_addition() {
    let mut server = common::Server::with_uid("test");

    let body = json!({
        "uid": "test",
    });

    // 1. Create Index
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    let dataset = include_bytes!("assets/test_set.json");

    let body: Value = serde_json::from_slice(dataset).unwrap();

    // 2. Index the documents from movies.json, present inside of assets directory
    server.add_or_replace_multiple_documents(body).await;

    // 3. Fetch the status of the indexing done above.
    let (response, status_code) = server.get_all_updates_status().await;

    // 4. Verify the fetch is successful and indexing status is 'processed'
    assert_eq!(status_code, 200);
    assert_eq!(response[0]["status"], "processed");
}

#[actix_rt::test]
async fn return_error_when_get_update_status_of_unexisting_index() {
    let mut server = common::Server::with_uid("test");

    // 1. Fetch the status of unexisting index.
    let (_, status_code) = server.get_all_updates_status().await;

    // 2. Verify the fetch returned 404
    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn return_empty_when_get_update_status_of_empty_index() {
    let mut server = common::Server::with_uid("test");

    let body = json!({
        "uid": "test",
    });

    // 1. Create Index
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    // 2. Fetch the status of empty index.
    let (response, status_code) = server.get_all_updates_status().await;

    // 3. Verify the fetch is successful, and no document are returned
    assert_eq!(status_code, 200);
    assert_eq!(response, json!([]));
}

#[actix_rt::test]
async fn return_update_status_of_pushed_documents() {
    let mut server = common::Server::with_uid("test");

    let body = json!({
        "uid": "test",
    });

    // 1. Create Index
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));


    let bodies = vec![
        json!([{
        "title": "Test",
        "comment": "comment test"
        }]),
        json!([{
        "title": "Test1",
        "comment": "comment test1"
        }]),
        json!([{
        "title": "Test2",
        "comment": "comment test2"
        }]),
    ];

    let mut update_ids = Vec::new();
    let mut bodies = bodies.into_iter();

    let url = "/indexes/test/documents?primaryKey=title";
    let (response, status_code) = server.post_request(&url, bodies.next().unwrap()).await;
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    update_ids.push(update_id);
    server.wait_update_id(update_id).await;

    let url = "/indexes/test/documents";
    for body in bodies {
        let (response, status_code) = server.post_request(&url, body).await;
        assert_eq!(status_code, 202);
        let update_id = response["updateId"].as_u64().unwrap();
        update_ids.push(update_id);
    }

    // 2. Fetch the status of index.
    let (response, status_code) = server.get_all_updates_status().await;

    // 3. Verify the fetch is successful, and updates are returned

    let expected = json!([{
        "type": {
            "name": "DocumentsAddition",
            "number": 1,
        },
        "updateId": update_ids[0]
    },{
        "type": {
            "name": "DocumentsAddition",
            "number": 1,
        },
        "updateId": update_ids[1]
    },{
        "type": {
            "name": "DocumentsAddition",
            "number": 1,
        },
        "updateId": update_ids[2]
    },]);

    assert_eq!(status_code, 200);
    assert_json_include!(actual: json!(response), expected: expected);
}

#[actix_rt::test]
async fn return_error_if_index_does_not_exist() {
    let mut server = common::Server::with_uid("test");

    let (response, status_code) = server.get_update_status(42).await;

    assert_eq!(status_code, 404);
    assert_eq!(response["errorCode"], "index_not_found");
}

#[actix_rt::test]
async fn return_error_if_update_does_not_exist() {
    let mut server = common::Server::with_uid("test");

    let body = json!({
        "uid": "test",
    });

    // 1. Create Index
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    let (response, status_code) = server.get_update_status(42).await;

    assert_eq!(status_code, 404);
    assert_eq!(response["errorCode"], "not_found");
}

#[actix_rt::test]
async fn should_return_existing_update() {
    let mut server = common::Server::with_uid("test");

    let body = json!({
        "uid": "test",
    });

    // 1. Create Index
    let (response, status_code) = server.create_index(body).await;
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    let body = json!([{
        "title": "Test",
        "comment": "comment test"
    }]);

    let url = "/indexes/test/documents?primaryKey=title";
    let (response, status_code) = server.post_request(&url, body).await;
    assert_eq!(status_code, 202);

    let update_id = response["updateId"].as_u64().unwrap();

    let expected = json!({
        "type": {
            "name": "DocumentsAddition",
            "number": 1,
        },
        "updateId": update_id
    });

    let (response, status_code) = server.get_update_status(update_id).await;

    assert_eq!(status_code, 200);
    assert_json_include!(actual: json!(response), expected: expected);
}
