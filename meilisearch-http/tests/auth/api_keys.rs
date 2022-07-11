use crate::common::Server;
use assert_json_diff::assert_json_include;
use serde_json::{json, Value};
use std::{thread, time};

#[actix_rt::test]
async fn add_valid_api_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "name": "indexing-key",
        "description": "Indexing API key",
        "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let expected_response = json!({
        "name": "indexing-key",
        "description": "Indexing API key",
        "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "key": "d9e776b8412f1db6974c9a5556b961c3559440b6588216f4ea5d9ed49f7c8f3c",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected_response);
}

#[actix_rt::test]
async fn add_valid_api_key_expired_at() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13"
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let expected_response = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected_response);
}

#[actix_rt::test]
async fn add_valid_api_key_no_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00"
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let expected_response = json!({
        "actions": ["documents.add"],
        "indexes": [
            "products"
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected_response);
}

#[actix_rt::test]
async fn add_valid_api_key_null_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": Value::Null,
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00"
    });

    let (response, code) = server.add_api_key(content).await;
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let expected_response = json!({
        "actions": ["documents.add"],
        "indexes": [
            "products"
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_no_header() {
    let server = Server::new_auth().await;
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(401, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The Authorization header is missing. It must use the bearer authorization method.",
        "code": "missing_authorization_header",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(403, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_missing_parameter() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    // missing indexes
    let content = json!({
        "description": "Indexing API key",
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": "`indexes` field is mandatory.",
        "code": "missing_parameter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#missing_parameter"
    });

    assert_eq!(response, expected_response);

    // missing actions
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": "`actions` field is mandatory.",
        "code": "missing_parameter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#missing_parameter"
    });

    assert_eq!(response, expected_response);

    // missing expiration date
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": "`expiresAt` field is mandatory.",
        "code": "missing_parameter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#missing_parameter"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": {"name":"products"},
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`description` field value `{"name":"products"}` is invalid. It should be a string or specified as a null value."#,
        "code": "invalid_api_key_description",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_name() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "name": {"name":"products"},
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`name` field value `{"name":"products"}` is invalid. It should be a string or specified as a null value."#,
        "code": "invalid_api_key_name",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_indexes() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": {"name":"products"},
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`indexes` field value `{"name":"products"}` is invalid. It should be an array of string representing index names."#,
        "code": "invalid_api_key_indexes",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_index_uids() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": Value::Null,
        "indexes": ["invalid index # / \\name with spaces"],
        "actions": [
            "documents.add"
        ],
        "expiresAt": "2050-11-13T00:00:00"
    });
    let (response, code) = server.add_api_key(content).await;

    let expected_response = json!({
        "message": r#"`indexes` field value `["invalid index # / \\name with spaces"]` is invalid. It should be an array of string representing index names."#,
        "code": "invalid_api_key_indexes",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_actions() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": {"name":"products"},
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`actions` field value `{"name":"products"}` is invalid. It should be an array of string representing action names."#,
        "code": "invalid_api_key_actions",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    });

    assert_eq!(response, expected_response);

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "doc.add"
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`actions` field value `["doc.add"]` is invalid. It should be an array of string representing action names."#,
        "code": "invalid_api_key_actions",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": {"name":"products"}
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`expiresAt` field value `{"name":"products"}` is invalid. It should follow the RFC 3339 format to represents a date or datetime in the future or specified as a null value. e.g. 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'."#,
        "code": "invalid_api_key_expires_at",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_expires_at"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_expires_at_in_the_past() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2010-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`expiresAt` field value `"2010-11-13T00:00:00Z"` is invalid. It should follow the RFC 3339 format to represents a date or datetime in the future or specified as a null value. e.g. 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'."#,
        "code": "invalid_api_key_expires_at",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_expires_at"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_uid() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "uid": "aaaaabbbbbccc",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": r#"`uid` field value `"aaaaabbbbbccc"` is invalid. It should be a valid UUID v4 string or omitted."#,
        "code": "invalid_api_key_uid",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_uid"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_add_api_key_parameters_uid_already_exist() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let content = json!({
        "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "indexes": ["products"],
        "actions": ["search"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    // first creation is valid.
    let (response, code) = server.add_api_key(content.clone()).await;
    assert_eq!(201, code, "{:?}", &response);

    // uid already exist.
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(409, code, "{:?}", &response);

    let expected_response = json!({
        "message": "`uid` field value `4bc0887a-0e41-4f3b-935d-0c451dcee9c8` is already an existing API key.",
        "code": "api_key_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#api_key_already_exists"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn get_api_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let uid = "4bc0887a-0e41-4f3b-935d-0c451dcee9c8";
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "uid": uid.to_string(),
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    let key = response["key"].as_str().unwrap();

    let expected_response = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "uid": uid.to_string(),
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    // get with uid
    let (response, code) = server.get_api_key(&uid).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());
    assert_json_include!(actual: response, expected: &expected_response);

    // get with key
    let (response, code) = server.get_api_key(&key).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());
    assert_json_include!(actual: response, expected: &expected_response);
}

#[actix_rt::test]
async fn error_get_api_key_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server
        .get_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    assert_eq!(401, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The Authorization header is missing. It must use the bearer authorization method.",
        "code": "missing_authorization_header",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_get_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server
        .get_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    assert_eq!(403, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_get_api_key_not_found() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let (response, code) = server
        .get_api_key("d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    assert_eq!(404, code, "{:?}", &response);

    let expected_response = json!({
        "message": "API key `d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4` not found.",
        "code": "api_key_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#api_key_not_found"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn list_api_keys() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);

    let (response, code) = server.list_api_keys().await;
    assert_eq!(200, code, "{:?}", &response);

    let expected_response = json!({ "results":
        [
            {
                "description": "Indexing API key",
                "indexes": ["products"],
                "actions": [
                    "search",
                    "documents.add",
                    "documents.get",
                    "documents.delete",
                    "indexes.create",
                    "indexes.get",
                    "indexes.update",
                    "indexes.delete",
                    "tasks.get",
                    "settings.get",
                    "settings.update",
                    "stats.get",
                    "dumps.create",
                ],
                "expiresAt": "2050-11-13T00:00:00Z"
            },
            {
                "name": "Default Search API Key",
                "description": "Use it to search from the frontend",
                "indexes": ["*"],
                "actions": ["search"],
                "expiresAt": serde_json::Value::Null,
            },
            {
                "name": "Default Admin API Key",
                "description": "Use it for anything that is not a search operation. Caution! Do not expose it on a public frontend",
                "indexes": ["*"],
                "actions": ["*"],
                "expiresAt": serde_json::Value::Null,
            }
        ],
        "limit": 20,
        "offset": 0,
        "total": 3,
    });

    assert_json_include!(actual: response, expected: expected_response);
}

#[actix_rt::test]
async fn error_list_api_keys_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server.list_api_keys().await;
    assert_eq!(401, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The Authorization header is missing. It must use the bearer authorization method.",
        "code": "missing_authorization_header",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_list_api_keys_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server.list_api_keys().await;
    assert_eq!(403, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn delete_api_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    let uid = response["uid"].as_str().unwrap();

    let (response, code) = server.delete_api_key(&uid).await;
    assert_eq!(204, code, "{:?}", &response);

    // check if API key no longer exist.
    let (response, code) = server.get_api_key(&uid).await;
    assert_eq!(404, code, "{:?}", &response);
}

#[actix_rt::test]
async fn error_delete_api_key_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server
        .delete_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    assert_eq!(401, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The Authorization header is missing. It must use the bearer authorization method.",
        "code": "missing_authorization_header",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_delete_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server
        .delete_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    assert_eq!(403, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_delete_api_key_not_found() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let (response, code) = server
        .delete_api_key("d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    assert_eq!(404, code, "{:?}", &response);

    let expected_response = json!({
        "message": "API key `d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4` not found.",
        "code": "api_key_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#api_key_not_found"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn patch_api_key_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let uid = response["uid"].as_str().unwrap();
    let created_at = response["createdAt"].as_str().unwrap();
    let updated_at = response["updatedAt"].as_str().unwrap();

    // Add a description
    let content = json!({ "description": "Indexing API key" });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert_ne!(response["updatedAt"].as_str().unwrap(), updated_at);
    assert_eq!(response["createdAt"].as_str().unwrap(), created_at);

    let expected = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected);

    // Change the description
    let content = json!({ "description": "Product API key" });

    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());

    let expected = json!({
        "description": "Product API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected);

    // Remove the description
    let content = json!({ "description": serde_json::Value::Null });

    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());

    let expected = json!({
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected);
}

#[actix_rt::test]
async fn patch_api_key_name() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let uid = response["uid"].as_str().unwrap();
    let created_at = response["createdAt"].as_str().unwrap();
    let updated_at = response["updatedAt"].as_str().unwrap();

    // Add a name
    let content = json!({ "name": "Indexing API key" });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());
    assert_ne!(response["updatedAt"].as_str().unwrap(), updated_at);
    assert_eq!(response["createdAt"].as_str().unwrap(), created_at);

    let expected = json!({
        "name": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected);

    // Change the name
    let content = json!({ "name": "Product API key" });

    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());

    let expected = json!({
        "name": "Product API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected);

    // Remove the name
    let content = json!({ "name": serde_json::Value::Null });

    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(200, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["expiresAt"].is_string());
    assert!(response["createdAt"].is_string());

    let expected = json!({
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    assert_json_include!(actual: response, expected: expected);
}

#[actix_rt::test]
async fn error_patch_api_key_indexes() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let uid = response["uid"].as_str().unwrap();

    let content = json!({ "indexes": ["products", "prices"] });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected = json!({"message": "The `indexes` field cannot be modified for the given resource.",
        "code": "immutable_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#immutable_field"
    });

    assert_json_include!(actual: response, expected: expected);
}

#[actix_rt::test]
async fn error_patch_api_key_actions() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let uid = response["uid"].as_str().unwrap();

    let content = json!({
        "actions": [
            "search",
            "documents.get",
            "indexes.get",
            "tasks.get",
            "settings.get",
        ],
    });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected = json!({"message": "The `actions` field cannot be modified for the given resource.",
        "code": "immutable_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#immutable_field"
    });

    assert_json_include!(actual: response, expected: expected);
}

#[actix_rt::test]
async fn error_patch_api_key_expiration_date() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());
    assert!(response["createdAt"].is_string());
    assert!(response["updatedAt"].is_string());

    let uid = response["uid"].as_str().unwrap();

    let content = json!({ "expiresAt": "2055-11-13T00:00:00Z" });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected = json!({"message": "The `expiresAt` field cannot be modified for the given resource.",
        "code": "immutable_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#immutable_field"
    });

    assert_json_include!(actual: response, expected: expected);
}

#[actix_rt::test]
async fn error_patch_api_key_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server
        .patch_api_key(
            "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
            json!({}),
        )
        .await;
    assert_eq!(401, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The Authorization header is missing. It must use the bearer authorization method.",
        "code": "missing_authorization_header",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_patch_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server
        .patch_api_key(
            "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
            json!({}),
        )
        .await;
    assert_eq!(403, code, "{:?}", &response);

    let expected_response = json!({
        "message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_patch_api_key_not_found() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let (response, code) = server
        .patch_api_key(
            "d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
            json!({}),
        )
        .await;
    assert_eq!(404, code, "{:?}", &response);

    let expected_response = json!({
        "message": "API key `d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4` not found.",
        "code": "api_key_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#api_key_not_found"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_patch_api_key_indexes_invalid_parameters() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    // must pass if add_valid_api_key test passes.
    assert_eq!(201, code, "{:?}", &response);
    assert!(response["key"].is_string());

    let uid = response["uid"].as_str().unwrap();

    // invalid description
    let content = json!({
        "description": 13
    });

    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": "`description` field value `13` is invalid. It should be a string or specified as a null value.",
        "code": "invalid_api_key_description",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    });

    assert_eq!(response, expected_response);

    // invalid name
    let content = json!({
        "name": 13
    });

    let (response, code) = server.patch_api_key(&uid, content).await;
    assert_eq!(400, code, "{:?}", &response);

    let expected_response = json!({
        "message": "`name` field value `13` is invalid. It should be a string or specified as a null value.",
        "code": "invalid_api_key_name",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    });

    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn error_access_api_key_routes_no_master_key_set() {
    let mut server = Server::new().await;

    let expected_response = json!({
        "message": "The Authorization header is missing. It must use the bearer authorization method.",
        "code": "missing_authorization_header",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    });
    let expected_code = 401;

    let (response, code) = server.add_api_key(json!({})).await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);

    let (response, code) = server.patch_api_key("content", json!({})).await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);

    let (response, code) = server.get_api_key("content").await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);

    let (response, code) = server.list_api_keys().await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);

    server.use_api_key("MASTER_KEY");

    let expected_response = json!({"message": "The provided API key is invalid.",
        "code": "invalid_api_key",
        "type": "auth",
        "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    });
    let expected_code = 403;

    let (response, code) = server.add_api_key(json!({})).await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);

    let (response, code) = server.patch_api_key("content", json!({})).await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);

    let (response, code) = server.get_api_key("content").await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);

    let (response, code) = server.list_api_keys().await;

    assert_eq!(expected_code, code, "{:?}", &response);
    assert_eq!(response, expected_response);
}
