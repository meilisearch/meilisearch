use actix_web::http::header::{ContentType, ACCEPT_ENCODING};
use actix_web::test;
use meili_snap::{json_string, snapshot};
use meilisearch::Opt;

use crate::common::encoder::Encoder;
use crate::common::{default_settings, Server, Value};
use crate::json;

#[actix_rt::test]
async fn create_index_no_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(response.uid()).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], json!(null));
}

#[actix_rt::test]
async fn create_index_with_gzip_encoded_request() {
    let server = Server::new_shared();
    let index = server.unique_index_with_encoder(Encoder::Gzip);
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(response.uid()).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], json!(null));
}

#[actix_rt::test]
async fn create_index_with_gzip_encoded_request_and_receiving_brotli_encoded_response() {
    let server = Server::new_shared();
    let app = server.init_web_app().await;

    let body = serde_json::to_string(&json!({
        "uid": "test",
        "primaryKey": None::<&str>,
    }))
    .unwrap();
    let req = test::TestRequest::post()
        .uri("/indexes")
        .insert_header(Encoder::Gzip.header().unwrap())
        .insert_header((ACCEPT_ENCODING, "br"))
        .insert_header(ContentType::json())
        .set_payload(Encoder::Gzip.encode(body))
        .to_request();

    let res = test::call_service(&app, req).await;

    assert_eq!(res.status(), 202);

    let bytes = test::read_body(res).await;
    let decoded = Encoder::Brotli.decode(bytes);
    let parsed_response =
        serde_json::from_slice::<Value>(decoded.into().as_ref()).expect("Expecting valid json");

    assert_eq!(parsed_response["indexUid"], "test");
}

#[actix_rt::test]
async fn create_index_with_zlib_encoded_request() {
    let server = Server::new_shared();
    let index = server.unique_index_with_encoder(Encoder::Deflate);
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(response.uid()).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], json!(null));
}

#[actix_rt::test]
async fn create_index_with_brotli_encoded_request() {
    let server = Server::new_shared();
    let index = server.unique_index_with_encoder(Encoder::Brotli);
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(response.uid()).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], json!(null));
}

#[actix_rt::test]
async fn create_index_with_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.create(Some("primary")).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(response.uid()).await.succeeded();

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], "primary");
}

#[actix_rt::test]
async fn create_index_with_invalid_primary_key() {
    let documents = json!([ { "id": 2, "title": "Pride and Prejudice" } ]);

    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.add_documents(documents, Some("title")).await;
    assert_eq!(code, 202);
    index.wait_task(response.uid()).await.failed();

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], json!(null));

    let documents = json!([ { "id": "e".repeat(513) } ]);

    let (response, code) = index.add_documents(documents, Some("id")).await;
    assert_eq!(code, 202);
    index.wait_task(response.uid()).await.failed();

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], json!(null));
}

#[actix_rt::test]
async fn test_create_multiple_indexes() {
    let server = Server::new_shared();
    let index1 = server.unique_index();
    let index2 = server.unique_index();
    let index3 = server.unique_index();
    let index4 = server.unique_index();

    let (task1, _) = index1.create(None).await;
    let (task2, _) = index2.create(None).await;
    let (task3, _) = index3.create(None).await;

    index1.wait_task(task1.uid()).await.succeeded();
    index2.wait_task(task2.uid()).await.succeeded();
    index3.wait_task(task3.uid()).await.succeeded();

    assert_eq!(index1.get().await.1, 200);
    assert_eq!(index2.get().await.1, 200);
    assert_eq!(index3.get().await.1, 200);
    assert_eq!(index4.get().await.1, 404);
}

#[actix_rt::test]
async fn error_create_existing_index() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (_, code) = index.create(Some("primary")).await;

    assert_eq!(code, 202);

    let (task, _) = index.create(Some("primary")).await;

    let response = index.wait_task(task.uid()).await;
    let msg = format!(
        "Index `{}` already exists.",
        task["indexUid"].as_str().expect("indexUid should exist").trim_matches('"')
    );

    let expected_response = json!({
        "message": msg,
        "code": "index_already_exists",
        "type": "invalid_request",
        "link":"https://docs.meilisearch.com/errors#index_already_exists"
    });

    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn error_create_with_invalid_index_uid() {
    let server = Server::new().await;
    let index = server.index("test test#!");
    let (response, code) = index.create(None).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.uid`: `test test#!` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn send_task_id() {
    let temp = tempfile::tempdir().unwrap();

    let options =
        Opt { experimental_replication_parameters: true, ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();

    let app = server.init_web_app().await;
    let index = server.index("catto");
    let (response, code) = index.create(None).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": 0,
      "indexUid": "catto",
      "status": "enqueued",
      "type": "indexCreation",
      "enqueuedAt": "[date]"
    }
    "###);

    let body = serde_json::to_string(&json!({
        "uid": "doggo",
        "primaryKey": None::<&str>,
    }))
    .unwrap();
    let req = test::TestRequest::post()
        .uri("/indexes")
        .insert_header(("TaskId", "25"))
        .insert_header(ContentType::json())
        .set_payload(body)
        .to_request();

    let res = test::call_service(&app, req).await;
    snapshot!(res.status(), @"202 Accepted");

    let bytes = test::read_body(res).await;
    let response = serde_json::from_slice::<Value>(&bytes).expect("Expecting valid json");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": 25,
      "indexUid": "doggo",
      "status": "enqueued",
      "type": "indexCreation",
      "enqueuedAt": "[date]"
    }
    "###);

    let body = serde_json::to_string(&json!({
        "uid": "girafo",
        "primaryKey": None::<&str>,
    }))
    .unwrap();
    let req = test::TestRequest::post()
        .uri("/indexes")
        .insert_header(("TaskId", "12"))
        .insert_header(ContentType::json())
        .set_payload(body)
        .to_request();

    let res = test::call_service(&app, req).await;
    snapshot!(res.status(), @"400 Bad Request");

    let bytes = test::read_body(res).await;
    let response = serde_json::from_slice::<Value>(&bytes).expect("Expecting valid json");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Received bad task id: 12 should be >= to 26.",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}
