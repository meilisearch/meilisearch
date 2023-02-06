use meili_snap::*;
use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn get_all_documents_bad_offset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.get_all_documents_raw("?offset").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `` as a positive integer",
      "code": "invalid_document_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_offset"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?offset=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `doggo` as a positive integer",
      "code": "invalid_document_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_offset"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?offset=-1").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `-1` as a positive integer",
      "code": "invalid_document_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_offset"
    }
    "###);
}

#[actix_rt::test]
async fn get_all_documents_bad_limit() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.get_all_documents_raw("?limit").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `` as a positive integer",
      "code": "invalid_document_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_limit"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?limit=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_document_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_limit"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?limit=-1").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `-1` as a positive integer",
      "code": "invalid_document_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_limit"
    }
    "###);
}

#[actix_rt::test]
async fn delete_documents_batch() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.delete_batch_raw(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Json deserialize error: invalid type: string \"doggo\", expected a sequence at line 1 column 7",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}
