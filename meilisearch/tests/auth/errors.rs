use meili_snap::*;
use uuid::Uuid;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn create_api_key_bad_description() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.add_api_key(json!({ "description": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.description`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_description",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_name() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.add_api_key(json!({ "name": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.name`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_name",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_uid() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "uid": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.uid`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_uid"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "uid": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.uid`: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `o` at 2",
      "code": "invalid_api_key_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_uid"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_actions() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "actions": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.actions`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "actions": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown value `doggo` at `.actions[0]`: expected one of `*`, `search`, `documents.*`, `documents.add`, `documents.get`, `documents.delete`, `indexes.*`, `indexes.create`, `indexes.get`, `indexes.update`, `indexes.delete`, `indexes.swap`, `tasks.*`, `tasks.cancel`, `tasks.delete`, `tasks.get`, `settings.*`, `settings.get`, `settings.update`, `stats.*`, `stats.get`, `metrics.*`, `metrics.get`, `dumps.*`, `dumps.create`, `snapshots.*`, `snapshots.create`, `version`, `keys.create`, `keys.get`, `keys.update`, `keys.delete`, `experimental.get`, `experimental.update`",
      "code": "invalid_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_indexes() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "indexes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.indexes`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "indexes": ["good doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.indexes[0]`: `good doggo` is not a valid index uid pattern. Index uid patterns can be an integer or a string containing only alphanumeric characters, hyphens (-), underscores (_), and optionally end with a star (*).",
      "code": "invalid_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "expires_at": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `expires_at`: did you mean `expiresAt`? expected one of `description`, `name`, `uid`, `actions`, `indexes`, `expiresAt`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "expires_at": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `expires_at`: did you mean `expiresAt`? expected one of `description`, `name`, `uid`, `actions`, `indexes`, `expiresAt`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_missing_action() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) =
        server.add_api_key(json!({ "indexes": ["doggo"], "expiresAt": null })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `actions`",
      "code": "missing_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_actions"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_missing_indexes() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server
        .add_api_key(json!({ "uid": Uuid::nil() , "actions": ["*"], "expiresAt": null }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `indexes`",
      "code": "missing_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_missing_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server
        .add_api_key(json!({ "uid": Uuid::nil(), "actions": ["*"], "indexes": ["doggo"] }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `expiresAt`",
      "code": "missing_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_expires_at"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_unexpected_field() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server
        .add_api_key(json!({ "uid": Uuid::nil(), "actions": ["*"], "indexes": ["doggo"], "expiresAt": null, "doggo": "bork" }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `doggo`: expected one of `description`, `name`, `uid`, `actions`, `indexes`, `expiresAt`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn list_api_keys_bad_offset() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.list_api_keys("?offset=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `doggo` as a positive integer",
      "code": "invalid_api_key_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_offset"
    }
    "###);
}

#[actix_rt::test]
async fn list_api_keys_bad_limit() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.list_api_keys("?limit=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_api_key_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_limit"
    }
    "###);
}

#[actix_rt::test]
async fn list_api_keys_unexpected_field() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.list_api_keys("?doggo=no_limit").await;
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
async fn patch_api_keys_bad_description() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "description": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.description`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_description",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_bad_name() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "name": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.name`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_name",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_uid() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "uid": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `uid`: expected one of `description`, `name`",
      "code": "immutable_api_key_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_uid"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_actions() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "actions": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `actions`: expected one of `description`, `name`",
      "code": "immutable_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_actions"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_indexes() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "indexes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `indexes`: expected one of `description`, `name`",
      "code": "immutable_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "expiresAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `expiresAt`: expected one of `description`, `name`",
      "code": "immutable_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_expires_at"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_created_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "createdAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `createdAt`: expected one of `description`, `name`",
      "code": "immutable_api_key_created_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_created_at"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_updated_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "updatedAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `updatedAt`: expected one of `description`, `name`",
      "code": "immutable_api_key_updated_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_updated_at"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_unknown_field() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "doggo": "bork" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `doggo`: expected one of `description`, `name`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}
