use crate::common::{GetAllDocumentsOptions, Server};
use actix_web::test;
use chrono::DateTime;
use meilisearch_http::{analytics, create_app};
use serde_json::{json, Value};

/// This is the basic usage of our API and every other tests uses the content-type application/json
#[actix_rt::test]
async fn add_documents_test_json_content_types() {
    let document = json!([
        {
            "id": 1,
            "content": "Bouvier Bernois",
        }
    ]);

    // this is a what is expected and should work
    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 202);
    assert_eq!(response["uid"], 0);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 202);
    assert_eq!(response["uid"], 1);
}

/// any other content-type is must be refused
#[actix_rt::test]
async fn error_add_documents_test_bad_content_types() {
    let document = json!([
        {
            "id": 1,
            "content": "Leonberg",
        }
    ]);

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/plain"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"The Content-Type `text/plain` is invalid. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`"#
        )
    );
    assert_eq!(response["code"], "invalid_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://docs.meilisearch.com/errors#invalid_content_type"
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/plain"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"The Content-Type `text/plain` is invalid. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`"#
        )
    );
    assert_eq!(response["code"], "invalid_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://docs.meilisearch.com/errors#invalid_content_type"
    );
}

/// missing content-type must be refused
#[actix_rt::test]
async fn error_add_documents_test_no_content_type() {
    let document = json!([
        {
            "id": 1,
            "content": "Leonberg",
        }
    ]);

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`"#
        )
    );
    assert_eq!(response["code"], "missing_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://docs.meilisearch.com/errors#missing_content_type"
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`"#
        )
    );
    assert_eq!(response["code"], "missing_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://docs.meilisearch.com/errors#missing_content_type"
    );
}

#[actix_rt::test]
async fn error_add_malformed_csv_documents() {
    let document = "id, content\n1234, hello, world\n12, hello world";

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(
            r#"The `csv` payload provided is malformed. `CSV error: record 1 (line: 2, byte: 12): found record with 3 fields, but the previous record has 2 fields`."#
        )
    );
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#malformed_payload")
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(
            r#"The `csv` payload provided is malformed. `CSV error: record 1 (line: 2, byte: 12): found record with 3 fields, but the previous record has 2 fields`."#
        )
    );
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#malformed_payload")
    );
}

#[actix_rt::test]
async fn error_add_malformed_json_documents() {
    let document = r#"[{"id": 1}, {id: 2}]"#;

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(
            r#"The `json` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 1 column 14`."#
        )
    );
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#malformed_payload")
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(
            r#"The `json` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 1 column 14`."#
        )
    );
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#malformed_payload")
    );
}

#[actix_rt::test]
async fn error_add_malformed_ndjson_documents() {
    let document = "{\"id\": 1}\n{id: 2}";

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(
            r#"The `ndjson` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 1 column 2`."#
        )
    );
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#malformed_payload")
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(
            r#"The `ndjson` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 1 column 2`."#
        )
    );
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#malformed_payload")
    );
}

#[actix_rt::test]
async fn error_add_missing_payload_csv_documents() {
    let document = "";

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["message"], json!(r#"A csv payload is missing."#));
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#missing_payload")
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["message"], json!(r#"A csv payload is missing."#));
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#missing_payload")
    );
}

#[actix_rt::test]
async fn error_add_missing_payload_json_documents() {
    let document = "";

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["message"], json!(r#"A json payload is missing."#));
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#missing_payload")
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["message"], json!(r#"A json payload is missing."#));
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#missing_payload")
    );
}

#[actix_rt::test]
async fn error_add_missing_payload_ndjson_documents() {
    let document = "";

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(r#"A ndjson payload is missing."#)
    );
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#missing_payload")
    );

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(
        response["message"],
        json!(r#"A ndjson payload is missing."#)
    );
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://docs.meilisearch.com/errors#missing_payload")
    );
}

#[actix_rt::test]
async fn add_documents_no_index_creation() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    assert_eq!(response["uid"], 0);
    /*
     * currently we don’t check these field to stay ISO with meilisearch
     * assert_eq!(response["status"], "pending");
     * assert_eq!(response["meta"]["type"], "DocumentsAddition");
     * assert_eq!(response["meta"]["format"], "Json");
     * assert_eq!(response["meta"]["primaryKey"], Value::Null);
     * assert!(response.get("enqueuedAt").is_some());
     */

    index.wait_task(0).await;

    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["uid"], 0);
    assert_eq!(response["type"], "documentsAddition");
    assert_eq!(response["details"]["receivedDocuments"], 1);
    assert_eq!(response["details"]["indexedDocuments"], 1);

    let processed_at =
        DateTime::parse_from_rfc3339(response["finishedAt"].as_str().unwrap()).unwrap();
    let enqueued_at =
        DateTime::parse_from_rfc3339(response["enqueuedAt"].as_str().unwrap()).unwrap();
    assert!(processed_at > enqueued_at);

    // index was created, and primary key was infered.
    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "id");
}

#[actix_rt::test]
async fn error_document_add_create_index_bad_uid() {
    let server = Server::new().await;
    let index = server.index("883  fj!");
    let (response, code) = index.add_documents(json!([{"id": 1}]), None).await;

    let expected_response = json!({
        "message": "`883  fj!` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
        "code": "invalid_index_uid",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    });

    assert_eq!(code, 400);
    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_document_update_create_index_bad_uid() {
    let server = Server::new().await;
    let index = server.index("883  fj!");
    let (response, code) = index.update_documents(json!([{"id": 1}]), None).await;

    let expected_response = json!({
        "message": "`883  fj!` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
        "code": "invalid_index_uid",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    });

    assert_eq!(code, 400);
    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn document_addition_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "primary": 1,
            "content": "foo",
        }
    ]);
    let (response, code) = index.add_documents(documents, Some("primary")).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_task(0).await;

    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["uid"], 0);
    assert_eq!(response["type"], "documentsAddition");
    assert_eq!(response["details"]["receivedDocuments"], 1);
    assert_eq!(response["details"]["indexedDocuments"], 1);

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn document_update_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "primary": 1,
            "content": "foo",
        }
    ]);
    let (_response, code) = index.update_documents(documents, Some("primary")).await;
    assert_eq!(code, 202);

    index.wait_task(0).await;

    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["uid"], 0);
    assert_eq!(response["type"], "documentsPartial");
    assert_eq!(response["details"]["indexedDocuments"], 1);
    assert_eq!(response["details"]["receivedDocuments"], 1);

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn replace_document() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_task(0).await;

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (_response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    index.wait_task(1).await;

    let (response, code) = index.get_task(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert_eq!(response.to_string(), r##"{"doc_id":1,"other":"bar"}"##);
}

#[actix_rt::test]
async fn error_add_no_documents() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, code) = index.add_documents(json!([]), None).await;

    let expected_response = json!({
        "message": "The `json` payload must contain at least one document.",
        "code": "malformed_payload",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#malformed_payload"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn update_document() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (_response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    index.wait_task(0).await;

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_task(1).await;

    let (response, code) = index.get_task(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        response.to_string(),
        r##"{"doc_id":1,"content":"foo","other":"bar"}"##
    );
}

#[actix_rt::test]
async fn add_larger_dataset() {
    let server = Server::new().await;
    let index = server.index("test");
    let update_id = index.load_test_set().await;
    let (response, code) = index.get_task(update_id).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "documentsAddition");
    assert_eq!(response["details"]["indexedDocuments"], 77);
    assert_eq!(response["details"]["receivedDocuments"], 77);
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            limit: Some(1000),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 77);
}

#[actix_rt::test]
async fn update_larger_dataset() {
    let server = Server::new().await;
    let index = server.index("test");
    let documents = serde_json::from_str(include_str!("../assets/test_set.json")).unwrap();
    index.update_documents(documents, None).await;
    index.wait_task(0).await;
    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["type"], "documentsPartial");
    assert_eq!(response["details"]["indexedDocuments"], 77);
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            limit: Some(1000),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 77);
}

#[actix_rt::test]
async fn error_add_documents_bad_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "docid": "foo & bar",
            "content": "foobar"
        }
    ]);
    index.add_documents(documents, None).await;
    index.wait_task(1).await;
    let (response, code) = index.get_task(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], json!("failed"));
    assert_eq!(response["error"]["message"], json!("Document identifier `foo & bar` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_)."));
    assert_eq!(response["error"]["code"], json!("invalid_document_id"));
    assert_eq!(response["error"]["type"], json!("invalid_request"));
    assert_eq!(
        response["error"]["link"],
        json!("https://docs.meilisearch.com/errors#invalid_document_id")
    );
}

#[actix_rt::test]
async fn error_update_documents_bad_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "docid": "foo & bar",
            "content": "foobar"
        }
    ]);
    index.update_documents(documents, None).await;
    let response = index.wait_task(1).await;
    assert_eq!(response["status"], json!("failed"));
    assert_eq!(response["error"]["message"], json!("Document identifier `foo & bar` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_)."));
    assert_eq!(response["error"]["code"], json!("invalid_document_id"));
    assert_eq!(response["error"]["type"], json!("invalid_request"));
    assert_eq!(
        response["error"]["link"],
        json!("https://docs.meilisearch.com/errors#invalid_document_id")
    );
}

#[actix_rt::test]
async fn error_add_documents_missing_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    index.add_documents(documents, None).await;
    index.wait_task(1).await;
    let (response, code) = index.get_task(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "failed");
    assert_eq!(
        response["error"]["message"],
        json!(r#"Document doesn't have a `docid` attribute: `{"id":"11","content":"foobar"}`."#)
    );
    assert_eq!(response["error"]["code"], json!("missing_document_id"));
    assert_eq!(response["error"]["type"], json!("invalid_request"));
    assert_eq!(
        response["error"]["link"],
        json!("https://docs.meilisearch.com/errors#missing_document_id")
    );
}

#[actix_rt::test]
async fn error_update_documents_missing_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    index.update_documents(documents, None).await;
    let response = index.wait_task(1).await;
    assert_eq!(response["status"], "failed");
    assert_eq!(
        response["error"]["message"],
        r#"Document doesn't have a `docid` attribute: `{"id":"11","content":"foobar"}`."#
    );
    assert_eq!(response["error"]["code"], "missing_document_id");
    assert_eq!(response["error"]["type"], "invalid_request");
    assert_eq!(
        response["error"]["link"],
        "https://docs.meilisearch.com/errors#missing_document_id"
    );
}

#[actix_rt::test]
#[ignore] // // TODO: Fix in an other PR: this does not provoke any error.
async fn error_document_field_limit_reached() {
    let server = Server::new().await;
    let index = server.index("test");

    index.create(Some("id")).await;

    let mut big_object = std::collections::HashMap::new();
    big_object.insert("id".to_owned(), "wow");
    for i in 0..65535 {
        let key = i.to_string();
        big_object.insert(key, "I am a text!");
    }

    let documents = json!([big_object]);

    let (_response, code) = index.update_documents(documents, Some("id")).await;
    assert_eq!(code, 202);

    index.wait_task(0).await;
    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    // Documents without a primary key are not accepted.
    assert_eq!(response["status"], "failed");

    let expected_error = json!({
        "message": "A document cannot contain more than 65,535 fields.",
        "code": "document_fields_limit_reached",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#document_fields_limit_reached"
    });

    assert_eq!(response["error"], expected_error);
}

#[actix_rt::test]
async fn error_add_documents_invalid_geo_field() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("id")).await;
    index
        .update_settings(json!({"sortableAttributes": ["_geo"]}))
        .await;

    let documents = json!([
        {
            "id": "11",
            "_geo": "foobar"
        }
    ]);

    index.add_documents(documents, None).await;
    index.wait_task(2).await;
    let (response, code) = index.get_task(2).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "failed");

    let expected_error = json!({
        "message": r#"The document with the id: `11` contains an invalid _geo field: `foobar`."#,
        "code": "invalid_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_geo_field"
    });

    assert_eq!(response["error"], expected_error);
}

#[actix_rt::test]
async fn error_add_documents_payload_size() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("id")).await;
    let document = json!(
        {
            "id": "11",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec metus erat, consequat in blandit venenatis, ultricies eu ipsum. Etiam luctus elit et mollis ultrices. Nam turpis risus, dictum non eros in, eleifend feugiat elit. Morbi non dolor pulvinar, sagittis mi sed, ultricies lorem. Nulla ultricies sem metus. Donec at suscipit quam, sed elementum mi. Suspendisse potenti. Fusce pharetra turpis tortor, sed eleifend odio dapibus ut. Nulla facilisi. Suspendisse elementum, dui eget aliquet dignissim, ex tellus aliquam nisl, at eleifend nisl metus tempus diam. Mauris fermentum sollicitudin efficitur. Donec dignissim est vitae elit finibus faucibus"
        }
    );
    let documents: Vec<_> = (0..16000).into_iter().map(|_| document.clone()).collect();
    let documents = json!(documents);
    let (response, code) = index.add_documents(documents, None).await;

    let expected_response = json!({
        "message": "The provided payload reached the size limit.",
        "code": "payload_too_large",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#payload_too_large"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 413);
}

#[actix_rt::test]
async fn error_primary_key_inference() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "title": "11",
            "desc": "foobar"
        }
    ]);

    index.add_documents(documents, None).await;
    index.wait_task(0).await;
    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "failed");

    let expected_error = json!({
        "message": r#"The primary key inference process failed because the engine did not find any fields containing `id` substring in their name. If your document identifier does not contain any `id` substring, you can set the primary key of the index."#,
        "code": "primary_key_inference_failed",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#primary_key_inference_failed"
    });

    assert_eq!(response["error"], expected_error);
}
