use actix_web::test;
use serde_json::{json, Value};

use crate::common::Server;

#[actix_rt::test]
async fn error_api_key_bad_content_types() {
    let content = json!({
        "indexes": ["products"],
        "actions": [
            "documents.add"
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_payload(content.to_string())
        .insert_header(("content-type", "text/plain"))
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"The Content-Type `text/plain` is invalid. Accepted values for the Content-Type header are: `application/json`"#
        )
    );
    assert_eq!(response["code"], "invalid_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://www.meilisearch.com/docs/reference/errors/error_codes#invalid-content-type"
    );

    // patch
    let req = test::TestRequest::patch()
        .uri("/keys/d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .set_payload(content.to_string())
        .insert_header(("content-type", "text/plain"))
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"The Content-Type `text/plain` is invalid. Accepted values for the Content-Type header are: `application/json`"#
        )
    );
    assert_eq!(response["code"], "invalid_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://www.meilisearch.com/docs/reference/errors/error_codes#invalid-content-type"
    );
}

#[actix_rt::test]
async fn error_api_key_empty_content_types() {
    let content = json!({
        "indexes": ["products"],
        "actions": [
            "documents.add"
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_payload(content.to_string())
        .insert_header(("content-type", ""))
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"The Content-Type `` is invalid. Accepted values for the Content-Type header are: `application/json`"#
        )
    );
    assert_eq!(response["code"], "invalid_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://www.meilisearch.com/docs/reference/errors/error_codes#invalid-content-type"
    );

    // patch
    let req = test::TestRequest::patch()
        .uri("/keys/d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .set_payload(content.to_string())
        .insert_header(("content-type", ""))
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"The Content-Type `` is invalid. Accepted values for the Content-Type header are: `application/json`"#
        )
    );
    assert_eq!(response["code"], "invalid_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://www.meilisearch.com/docs/reference/errors/error_codes#invalid-content-type"
    );
}

#[actix_rt::test]
async fn error_api_key_missing_content_types() {
    let content = json!({
        "indexes": ["products"],
        "actions": [
            "documents.add"
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_payload(content.to_string())
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`"#
        )
    );
    assert_eq!(response["code"], "missing_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://www.meilisearch.com/docs/reference/errors/error_codes#missing-content-type"
    );

    // patch
    let req = test::TestRequest::patch()
        .uri("/keys/d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .set_payload(content.to_string())
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`"#
        )
    );
    assert_eq!(response["code"], "missing_content_type");
    assert_eq!(response["type"], "invalid_request");
    assert_eq!(
        response["link"],
        "https://www.meilisearch.com/docs/reference/errors/error_codes#missing-content-type"
    );
}

#[actix_rt::test]
async fn error_api_key_empty_payload() {
    let content = "";

    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_payload(content)
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://www.meilisearch.com/docs/reference/errors/error_codes#missing-payload")
    );
    assert_eq!(response["message"], json!(r#"A json payload is missing."#));

    // patch
    let req = test::TestRequest::patch()
        .uri("/keys/d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .set_payload(content)
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["code"], json!("missing_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://www.meilisearch.com/docs/reference/errors/error_codes#missing-payload")
    );
    assert_eq!(response["message"], json!(r#"A json payload is missing."#));
}

#[actix_rt::test]
async fn error_api_key_malformed_payload() {
    let content = r#"{"malormed": "payload""#;

    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/keys")
        .set_payload(content)
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://www.meilisearch.com/docs/reference/errors/error_codes#malformed-payload")
    );
    assert_eq!(
        response["message"],
        json!(
            r#"The json payload provided is malformed. `EOF while parsing an object at line 1 column 22`."#
        )
    );

    // patch
    let req = test::TestRequest::patch()
        .uri("/keys/d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .set_payload(content)
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 400);
    assert_eq!(response["code"], json!("malformed_payload"));
    assert_eq!(response["type"], json!("invalid_request"));
    assert_eq!(
        response["link"],
        json!("https://www.meilisearch.com/docs/reference/errors/error_codes#malformed-payload")
    );
    assert_eq!(
        response["message"],
        json!(
            r#"The json payload provided is malformed. `EOF while parsing an object at line 1 column 22`."#
        )
    );
}
