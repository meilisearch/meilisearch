use meili_snap::*;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn get_indexes_bad_offset() {
    let server = Server::new_shared();

    let (response, code) = server.list_indexes_raw("?offset=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `doggo` as a positive integer",
      "code": "invalid_index_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_offset"
    }
    "###);
}

#[actix_rt::test]
async fn get_indexes_bad_limit() {
    let server = Server::new_shared();

    let (response, code) = server.list_indexes_raw("?limit=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_index_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_limit"
    }
    "###);
}

#[actix_rt::test]
async fn get_indexes_unknown_field() {
    let server = Server::new_shared();

    let (response, code) = server.list_indexes_raw("?doggo=nolimit").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown parameter `doggo`: expected one of `offset`, `limit`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn create_index_missing_uid() {
    let server = Server::new_shared();

    let (response, code) = server.create_index_fail(json!({ "primaryKey": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `uid`",
      "code": "missing_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn create_index_bad_uid() {
    let server = Server::new_shared();

    let (response, code) = server.create_index_fail(json!({ "uid": "the best doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.uid`: `the best doggo` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);

    let (response, code) = server.create_index_fail(json!({ "uid": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.uid`: expected a string, but found a boolean: `true`",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn create_index_bad_primary_key() {
    let server = Server::new_shared();

    let (response, code) = server
        .create_index_fail(json!({ "uid": "doggo", "primaryKey": ["the", "best", "doggo"] }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.primaryKey`: expected a string, but found an array: `[\"the\",\"best\",\"doggo\"]`",
      "code": "invalid_index_primary_key",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_primary_key"
    }
    "###);
}

#[actix_rt::test]
async fn create_index_unknown_field() {
    let server = Server::new_shared();

    let (response, code) =
        server.create_index_fail(json!({ "uid": "doggo", "doggo": "bernese" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `doggo`: expected one of `uid`, `primaryKey`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn get_index_bad_uid() {
    let server = Server::new_shared();
    let (response, code) = server.get_index_fail("the good doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "`the good doggo` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn update_index_bad_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.update_raw(json!({ "primaryKey": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.primaryKey`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_index_primary_key",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_primary_key"
    }
    "###);
}

#[actix_rt::test]
async fn update_index_immutable_uid() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.update_raw(json!({ "uid": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `uid`: expected one of `primaryKey`",
      "code": "immutable_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn update_index_immutable_created_at() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.update_raw(json!({ "createdAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `createdAt`: expected one of `primaryKey`",
      "code": "immutable_index_created_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_index_created_at"
    }
    "###);
}

#[actix_rt::test]
async fn update_index_immutable_updated_at() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.update_raw(json!({ "updatedAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `updatedAt`: expected one of `primaryKey`",
      "code": "immutable_index_updated_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_index_updated_at"
    }
    "###);
}

#[actix_rt::test]
async fn update_index_unknown_field() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, code) = index.update_raw(json!({ "doggo": "bork" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `doggo`: expected one of `primaryKey`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn update_index_bad_uid() {
    let server = Server::new_shared();
    let (response, code) =
        server.update_raw_index_fail("the good doggo", json!({ "primaryKey": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "`the good doggo` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn delete_index_bad_uid() {
    let server = Server::new_shared();
    let (response, code) = server.delete_index_fail("the good doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "`the good doggo` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}
