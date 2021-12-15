#![allow(dead_code)]
mod common;

use crate::common::Server;
use serde_json::json;

#[actix_rt::test]
async fn get_unexisting_dump_status() {
    let server = Server::new().await;

    let (response, code) = server.get_dump_status("foobar").await;
    assert_eq!(code, 404);

    let expected_response = json!({
    "message": "Dump `foobar` not found.",
    "code": "dump_not_found",
    "type": "invalid_request",
    "link": "https://docs.meilisearch.com/errors#dump_not_found"
    });

    assert_eq!(response, expected_response);
}
