#![allow(dead_code)]

mod common;

use crate::common::Server;
use actix_web::test;
use meilisearch_http::{analytics, create_app};
use serde_json::{json, Value};

#[actix_rt::test]
async fn error_json_bad_content_type() {
    let routes = [
        // all the POST routes except the dumps that can be created without any body or content-type
        // and the search that is not a strict json
        "/indexes",
        "/indexes/doggo/documents/delete-batch",
        "/indexes/doggo/search",
        "/indexes/doggo/settings",
        "/indexes/doggo/settings/displayed-attributes",
        "/indexes/doggo/settings/distinct-attribute",
        "/indexes/doggo/settings/filterable-attributes",
        "/indexes/doggo/settings/ranking-rules",
        "/indexes/doggo/settings/searchable-attributes",
        "/indexes/doggo/settings/sortable-attributes",
        "/indexes/doggo/settings/stop-words",
        "/indexes/doggo/settings/synonyms",
    ];
    let bad_content_types = [
        "application/csv",
        "application/x-ndjson",
        "application/x-www-form-urlencoded",
        "text/plain",
        "json",
        "application",
        "json/application",
    ];

    let document = "{}";
    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;
    for route in routes {
        // Good content-type, we probably have an error since we didn't send anything in the json
        // so we only ensure we didn't get a bad media type error.
        let req = test::TestRequest::post()
            .uri(route)
            .set_payload(document)
            .insert_header(("content-type", "application/json"))
            .to_request();
        let res = test::call_service(&app, req).await;
        let status_code = res.status();
        assert_ne!(status_code, 415,
        "calling the route `{}` with a content-type of json isn't supposed to throw a bad media type error", route);

        // No content-type.
        let req = test::TestRequest::post()
            .uri(route)
            .set_payload(document)
            .to_request();
        let res = test::call_service(&app, req).await;
        let status_code = res.status();
        let body = test::read_body(res).await;
        let response: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(status_code, 415, "calling the route `{}` without content-type is supposed to throw a bad media type error", route);
        assert_eq!(
            response,
            json!({
                    "message": r#"A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`"#,
                    "code": "missing_content_type",
                    "type": "invalid_request",
                    "link": "https://docs.meilisearch.com/errors#missing_content_type",
            }),
            "when calling the route `{}` with no content-type",
            route,
        );

        for bad_content_type in bad_content_types {
            // Always bad content-type
            let req = test::TestRequest::post()
                .uri(route)
                .set_payload(document.to_string())
                .insert_header(("content-type", bad_content_type))
                .to_request();
            let res = test::call_service(&app, req).await;
            let status_code = res.status();
            let body = test::read_body(res).await;
            let response: Value = serde_json::from_slice(&body).unwrap_or_default();
            assert_eq!(status_code, 415);
            let expected_error_message = format!(
                r#"The Content-Type `{}` is invalid. Accepted values for the Content-Type header are: `application/json`"#,
                bad_content_type
            );
            assert_eq!(
                response,
                json!({
                        "message": expected_error_message,
                        "code": "invalid_content_type",
                        "type": "invalid_request",
                        "link": "https://docs.meilisearch.com/errors#invalid_content_type",
                }),
                "when calling the route `{}` with a content-type of `{}`",
                route,
                bad_content_type,
            );
        }
    }
}

#[actix_rt::test]
async fn extract_actual_content_type() {
    let route = "/indexes/doggo/documents";
    let documents = "[{}]";
    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        &server.service.auth,
        true,
        &server.service.options,
        analytics::MockAnalytics::new(&server.service.options).0
    ))
    .await;

    // Good content-type, we probably have an error since we didn't send anything in the json
    // so we only ensure we didn't get a bad media type error.
    let req = test::TestRequest::post()
        .uri(route)
        .set_payload(documents)
        .insert_header(("content-type", "application/json; charset=utf-8"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    assert_ne!(status_code, 415,
    "calling the route `{}` with a content-type of json isn't supposed to throw a bad media type error", route);

    let req = test::TestRequest::put()
        .uri(route)
        .set_payload(documents)
        .insert_header(("content-type", "application/json; charset=latin-1"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    assert_ne!(status_code, 415,
    "calling the route `{}` with a content-type of json isn't supposed to throw a bad media type error", route);
}
