use actix_web::http::header::ContentType;
use actix_web::test;
use http::header::ACCEPT_ENCODING;
use meili_snap::{json_string, snapshot};
use serde_json::{json, Value};

use crate::common::encoder::Encoder;
use crate::common::Server;

#[actix_rt::test]
async fn create_index_no_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], Value::Null);
}

#[actix_rt::test]
async fn create_index_with_gzip_encoded_request() {
    let server = Server::new().await;
    let index = server.index_with_encoder("test", Encoder::Gzip);
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], Value::Null);
}

#[actix_rt::test]
async fn create_index_with_gzip_encoded_request_and_receiving_brotli_encoded_response() {
    let server = Server::new().await;
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

    assert_eq!(parsed_response["taskUid"], 0);
    assert_eq!(parsed_response["indexUid"], "test");
}

#[actix_rt::test]
async fn create_index_with_zlib_encoded_request() {
    let server = Server::new().await;
    let index = server.index_with_encoder("test", Encoder::Deflate);
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], Value::Null);
}

#[actix_rt::test]
async fn create_index_with_brotli_encoded_request() {
    let server = Server::new().await;
    let index = server.index_with_encoder("test", Encoder::Brotli);
    let (response, code) = index.create(None).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], Value::Null);
}

#[actix_rt::test]
async fn create_index_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.create(Some("primary")).await;

    assert_eq!(code, 202);

    assert_eq!(response["status"], "enqueued");

    let response = index.wait_task(0).await;

    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexCreation");
    assert_eq!(response["details"]["primaryKey"], "primary");
}

#[actix_rt::test]
async fn create_index_with_invalid_primary_key() {
    let document = json!([ { "id": 2, "title": "Pride and Prejudice" } ]);

    let server = Server::new().await;
    let index = server.index("movies");
    let (_response, code) = index.add_documents(document, Some("title")).await;
    assert_eq!(code, 202);

    index.wait_task(0).await;

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], Value::Null);
}

#[actix_rt::test]
async fn test_create_multiple_indexes() {
    let server = Server::new().await;
    let index1 = server.index("test1");
    let index2 = server.index("test2");
    let index3 = server.index("test3");
    let index4 = server.index("test4");

    index1.create(None).await;
    index2.create(None).await;
    index3.create(None).await;

    index1.wait_task(0).await;
    index1.wait_task(1).await;
    index1.wait_task(2).await;

    assert_eq!(index1.get().await.1, 200);
    assert_eq!(index2.get().await.1, 200);
    assert_eq!(index3.get().await.1, 200);
    assert_eq!(index4.get().await.1, 404);
}

#[actix_rt::test]
async fn error_create_existing_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(Some("primary")).await;

    assert_eq!(code, 202);

    index.create(Some("primary")).await;

    let response = index.wait_task(1).await;

    let expected_response = json!({
        "message": "Index `test` already exists.",
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
      "message": "Invalid value at `.uid`: `test test#!` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}
