//! To test the webhook, we need to spawn a new server with a URL listening for
//! post requests. The webhook handle starts a server and forwards all the
//! received requests into a channel for you to handle.

use std::path::PathBuf;
use std::sync::Arc;

use actix_http::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceResponse};
use actix_web::web::{Bytes, Data};
use actix_web::{post, App, HttpRequest, HttpResponse, HttpServer};
use meili_snap::{json_string, snapshot};
use meilisearch::Opt;
use tokio::sync::mpsc;
use url::Url;
use uuid::Uuid;

use crate::common::{self, default_settings, Server};
use crate::json;

#[post("/")]
async fn forward_body(
    req: HttpRequest,
    sender: Data<mpsc::UnboundedSender<Vec<u8>>>,
    body: Bytes,
) -> HttpResponse {
    let headers = req.headers();
    assert_eq!(headers.get("content-type").unwrap(), "application/x-ndjson");
    assert_eq!(headers.get("transfer-encoding").unwrap(), "chunked");
    assert_eq!(headers.get("accept-encoding").unwrap(), "gzip");
    assert_eq!(headers.get("content-encoding").unwrap(), "gzip");

    let body = body.to_vec();
    sender.send(body).unwrap();
    HttpResponse::Ok().into()
}

fn create_app(
    sender: Arc<mpsc::UnboundedSender<Vec<u8>>>,
) -> actix_web::App<
    impl ServiceFactory<
        actix_web::dev::ServiceRequest,
        Config = (),
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
        InitError = (),
    >,
> {
    App::new().service(forward_body).app_data(Data::from(sender))
}

struct WebhookHandle {
    pub server_handle: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    pub url: String,
    pub receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}

async fn create_webhook_server() -> WebhookHandle {
    let (sender, receiver) = mpsc::unbounded_channel();
    let sender = Arc::new(sender);

    // By listening on the port 0, the system will give us any available port.
    let server =
        HttpServer::new(move || create_app(sender.clone())).bind(("127.0.0.1", 0)).unwrap();
    let (ip, scheme) = server.addrs_with_scheme()[0];
    let url = format!("{scheme}://{ip}/");

    let server_handle = tokio::spawn(server.run());
    WebhookHandle { server_handle, url, receiver }
}

#[actix_web::test]
async fn cli_only() {
    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(Url::parse("https://example-cli.com/").unwrap()),
        task_webhook_authorization_header: Some(String::from("Bearer a-secret-token")),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    let (webhooks, code) = server.get_webhooks().await;
    snapshot!(code, @"200 OK");
    snapshot!(webhooks, @r###"
    {
      "results": [
        {
          "uuid": "00000000-0000-0000-0000-000000000000",
          "isEditable": false,
          "url": "https://example-cli.com/",
          "headers": {
            "Authorization": "Bearer a-XXXX..."
          }
        }
      ]
    }
    "###);
}

#[actix_web::test]
async fn single_receives_data() {
    let WebhookHandle { server_handle, url, mut receiver } = create_webhook_server().await;

    let server = Server::new().await;

    let (value, code) = server.create_webhook(json!({ "url": url })).await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]", ".url" => "[ignored]" }), @r#"
    {
      "uuid": "[uuid]",
      "isEditable": true,
      "url": "[ignored]",
      "headers": {}
    }
    "#);

    // May be flaky: we're relying on the fact that while the first document addition is processed, the other
    // operations will be received and will be batched together. If it doesn't happen it's not a problem
    // the rest of the test won't assume anything about the number of tasks per batch.
    let index = server.index("tamo");
    for i in 0..5 {
        let (_, _status) = index.add_documents(json!({ "id": i, "doggo": "bone" }), None).await;
    }

    let mut nb_tasks = 0;
    while let Some(payload) = receiver.recv().await {
        let payload = String::from_utf8(payload).unwrap();
        let jsonl = payload.split('\n');
        for json in jsonl {
            if json.is_empty() {
                break; // we reached EOF
            }
            nb_tasks += 1;
            let json: serde_json::Value = serde_json::from_str(json).unwrap();
            snapshot!(common::Value(json),
            @r###"
            {
              "uid": "[uid]",
              "batchUid": "[batch_uid]",
              "indexUid": "tamo",
              "status": "succeeded",
              "type": "documentAdditionOrUpdate",
              "canceledBy": null,
              "details": {
                "receivedDocuments": 1,
                "indexedDocuments": 1
              },
              "error": null,
              "duration": "[duration]",
              "enqueuedAt": "[date]",
              "startedAt": "[date]",
              "finishedAt": "[date]"
            }
            "###);
        }
        if nb_tasks == 5 {
            break;
        }
    }

    assert!(nb_tasks == 5, "We should have received the 5 tasks but only received {nb_tasks}");

    server_handle.abort();
}

#[actix_web::test]
async fn multiple_receive_data() {
    let WebhookHandle { server_handle: handle1, url: url1, receiver: mut receiver1 } =
        create_webhook_server().await;
    let WebhookHandle { server_handle: handle2, url: url2, receiver: mut receiver2 } =
        create_webhook_server().await;
    let WebhookHandle { server_handle: handle3, url: url3, receiver: mut receiver3 } =
        create_webhook_server().await;

    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(Url::parse(&url3).unwrap()),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    for url in [url1, url2] {
        let (value, code) = server.create_webhook(json!({ "url": url })).await;
        snapshot!(code, @"201 Created");
        snapshot!(json_string!(value, { ".uuid" => "[uuid]", ".url" => "[ignored]" }), @r#"
        {
          "uuid": "[uuid]",
          "isEditable": true,
          "url": "[ignored]",
          "headers": {}
        }
        "#);
    }
    let index = server.index("tamo");
    let (_, status) = index.add_documents(json!({ "id": 1, "doggo": "bone" }), None).await;
    snapshot!(status, @"202 Accepted");

    let mut count1 = 0;
    let mut count2 = 0;
    let mut count3 = 0;
    while count1 == 0 || count2 == 0 || count3 == 0 {
        tokio::select! {
            msg = receiver1.recv() => { if msg.is_some() { count1 += 1; } },
            msg = receiver2.recv() => { if msg.is_some() { count2 += 1; } },
            msg = receiver3.recv() => { if msg.is_some() { count3 += 1; } },
        }
    }

    assert_eq!(count1, 1);
    assert_eq!(count2, 1);
    assert_eq!(count3, 1);

    handle1.abort();
    handle2.abort();
    handle3.abort();
}

#[actix_web::test]
async fn cli_with_dumps() {
    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(Url::parse("http://defined-in-test-cli.com").unwrap()),
        task_webhook_authorization_header: Some(String::from(
            "Bearer a-secret-token-defined-in-test-cli",
        )),
        import_dump: Some(PathBuf::from("../dump/tests/assets/v6-with-webhooks.dump")),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    let (webhooks, code) = server.get_webhooks().await;
    snapshot!(code, @"200 OK");
    snapshot!(webhooks, @r###"
    {
      "results": [
        {
          "uuid": "00000000-0000-0000-0000-000000000000",
          "isEditable": false,
          "url": "http://defined-in-test-cli.com/",
          "headers": {
            "Authorization": "Bearer a-secXXXXXX..."
          }
        },
        {
          "uuid": "627ea538-733d-4545-8d2d-03526eb381ce",
          "isEditable": true,
          "url": "https://example.com/authorization-less",
          "headers": {}
        },
        {
          "uuid": "771b0a28-ef28-4082-b984-536f82958c65",
          "isEditable": true,
          "url": "https://example.com/hook",
          "headers": {
            "authorization": "XXX..."
          }
        },
        {
          "uuid": "f3583083-f8a7-4cbf-a5e7-fb3f1e28a7e9",
          "isEditable": true,
          "url": "https://third.com",
          "headers": {}
        }
      ]
    }
    "###);
}

#[actix_web::test]
async fn reserved_names() {
    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(Url::parse("https://example-cli.com/").unwrap()),
        task_webhook_authorization_header: Some(String::from("Bearer a-secret-token")),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    let (value, code) = server
        .patch_webhook(Uuid::nil().to_string(), json!({ "url": "http://localhost:8080" }))
        .await;
    snapshot!(value, @r#"
    {
      "message": "Webhook `[uuid]` is immutable. The webhook defined from the command line cannot be modified using the API.",
      "code": "immutable_webhook",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_webhook"
    }
    "#);
    snapshot!(code, @"400 Bad Request");

    let (value, code) = server.delete_webhook(Uuid::nil().to_string()).await;
    snapshot!(value, @r#"
    {
      "message": "Webhook `[uuid]` is immutable. The webhook defined from the command line cannot be modified using the API.",
      "code": "immutable_webhook",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_webhook"
    }
    "#);
    snapshot!(code, @"400 Bad Request");
}

#[actix_web::test]
async fn over_limits() {
    let server = Server::new().await;

    // Too many webhooks
    let mut uuids = Vec::new();
    for _ in 0..20 {
        let (value, code) = server.create_webhook(json!({ "url": "http://localhost:8080" } )).await;
        snapshot!(code, @"201 Created");
        uuids.push(value.get("uuid").unwrap().as_str().unwrap().to_string());
    }
    let (value, code) = server.create_webhook(json!({ "url": "http://localhost:8080" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Defining too many webhooks would crush the server. Please limit the number of webhooks to 20. You may use a third-party proxy server to dispatch events to more than 20 endpoints.",
      "code": "invalid_webhooks",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhooks"
    }
    "#);

    // Reset webhooks
    for uuid in uuids {
        let (_value, code) = server.delete_webhook(&uuid).await;
        snapshot!(code, @"204 No Content");
    }

    // Test too many headers
    let (value, code) = server.create_webhook(json!({ "url": "http://localhost:8080" })).await;
    snapshot!(code, @"201 Created");
    let uuid = value.get("uuid").unwrap().as_str().unwrap();
    for i in 0..200 {
        let header_name = format!("header_{i}");
        let (_value, code) =
            server.patch_webhook(uuid, json!({ "headers": { header_name: "" } })).await;
        snapshot!(code, @"200 OK");
    }
    let (value, code) =
        server.patch_webhook(uuid, json!({ "headers": { "header_200": "" } })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Too many headers for the webhook `[uuid]`. Please limit the number of headers to 200. Hint: To remove an already defined header set its value to `null`",
      "code": "invalid_webhook_headers",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_headers"
    }
    "#);
}

#[actix_web::test]
async fn post_get_delete() {
    let server = Server::new().await;

    let (value, code) = server
        .create_webhook(json!({
            "url": "https://example.com/hook",
            "headers": { "authorization": "TOKEN" }
        }))
        .await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r###"
    {
      "uuid": "[uuid]",
      "isEditable": true,
      "url": "https://example.com/hook",
      "headers": {
        "authorization": "XXX..."
      }
    }
    "###);

    let uuid = value.get("uuid").unwrap().as_str().unwrap();
    let (value, code) = server.get_webhook(uuid).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r###"
    {
      "uuid": "[uuid]",
      "isEditable": true,
      "url": "https://example.com/hook",
      "headers": {
        "authorization": "XXX..."
      }
    }
    "###);

    let (_value, code) = server.delete_webhook(uuid).await;
    snapshot!(code, @"204 No Content");

    let (_value, code) = server.get_webhook(uuid).await;
    snapshot!(code, @"404 Not Found");
}

#[actix_web::test]
async fn create_and_patch() {
    let server = Server::new().await;

    let (value, code) =
        server.create_webhook(json!({ "headers": { "authorization": "TOKEN" } })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "The URL for the webhook `[uuid]` is missing.",
      "code": "invalid_webhook_url",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_url"
    }
    "#);

    let (value, code) = server.create_webhook(json!({ "url": "https://example.com/hook" })).await;
    snapshot!(code, @"201 Created");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r#"
    {
      "uuid": "[uuid]",
      "isEditable": true,
      "url": "https://example.com/hook",
      "headers": {}
    }
    "#);

    let uuid = value.get("uuid").unwrap().as_str().unwrap();
    let (value, code) =
        server.patch_webhook(&uuid, json!({ "headers": { "authorization": "TOKEN" } })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r###"
    {
      "uuid": "[uuid]",
      "isEditable": true,
      "url": "https://example.com/hook",
      "headers": {
        "authorization": "XXX..."
      }
    }
    "###);

    let (value, code) =
        server.patch_webhook(&uuid, json!({ "headers": { "authorization2": "TOKEN" } })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r###"
    {
      "uuid": "[uuid]",
      "isEditable": true,
      "url": "https://example.com/hook",
      "headers": {
        "authorization": "XXX...",
        "authorization2": "TOKEN"
      }
    }
    "###);

    let (value, code) =
        server.patch_webhook(&uuid, json!({ "headers": { "authorization": null } })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r#"
    {
      "uuid": "[uuid]",
      "isEditable": true,
      "url": "https://example.com/hook",
      "headers": {
        "authorization2": "TOKEN"
      }
    }
    "#);

    let (value, code) = server.patch_webhook(&uuid, json!({ "url": null })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r#"
    {
      "message": "The URL for the webhook `[uuid]` is missing.",
      "code": "invalid_webhook_url",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_url"
    }
    "#);
}

#[actix_web::test]
async fn invalid_url_and_headers() {
    let server = Server::new().await;

    // Test invalid URL format
    let (value, code) = server.create_webhook(json!({ "url": "not-a-valid-url" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Invalid URL `not-a-valid-url`: relative URL without a base",
      "code": "invalid_webhook_url",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_url"
    }
    "#);

    // Test invalid header name (containing spaces)
    let (value, code) = server
        .create_webhook(json!({
            "url": "https://example.com/hook",
            "headers": { "invalid header name": "value" }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Invalid header name `invalid header name`: invalid HTTP header name",
      "code": "invalid_webhook_headers",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_headers"
    }
    "#);

    // Test invalid header value (containing control characters)
    let (value, code) = server
        .create_webhook(json!({
            "url": "https://example.com/hook",
            "headers": { "authorization": "token\nwith\nnewlines" }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Invalid header value `authorization`: failed to parse header value",
      "code": "invalid_webhook_headers",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_headers"
    }
    "#);
}

#[actix_web::test]
async fn invalid_uuid() {
    let server = Server::new().await;

    // Test get webhook with invalid UUID
    let (value, code) = server.get_webhook("invalid-uuid").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Invalid UUID: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `i` at 1",
      "code": "invalid_webhook_uuid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_uuid"
    }
    "#);

    // Test update webhook with invalid UUID
    let (value, code) =
        server.patch_webhook("invalid-uuid", json!({ "url": "https://example.com/hook" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Invalid UUID: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `i` at 1",
      "code": "invalid_webhook_uuid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_uuid"
    }
    "#);

    // Test delete webhook with invalid UUID
    let (value, code) = server.delete_webhook("invalid-uuid").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Invalid UUID: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `i` at 1",
      "code": "invalid_webhook_uuid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhook_uuid"
    }
    "#);
}

#[actix_web::test]
async fn forbidden_fields() {
    let server = Server::new().await;

    // Test creating webhook with uuid field
    let custom_uuid = Uuid::new_v4();
    let (value, code) = server
        .create_webhook(json!({
            "url": "https://example.com/hook",
            "uuid": custom_uuid.to_string(),
            "headers": { "authorization": "TOKEN" }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Immutable field `uuid`: expected one of `url`, `headers`",
      "code": "immutable_webhook_uuid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_webhook_uuid"
    }
    "#);

    // Test creating webhook with isEditable field
    let (value, code) = server
        .create_webhook(json!({
            "url": "https://example.com/hook2",
            "isEditable": false,
            "headers": { "authorization": "TOKEN" }
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Immutable field `isEditable`: expected one of `url`, `headers`",
      "code": "immutable_webhook_is_editable",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_webhook_is_editable"
    }
    "#);

    // Test patching webhook with uuid field
    let (value, code) = server
        .patch_webhook(
            "uuid-whatever",
            json!({
                "uuid": Uuid::new_v4(),
                "headers": { "new-header": "value" }
            }),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Immutable field `uuid`: expected one of `url`, `headers`",
      "code": "immutable_webhook_uuid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_webhook_uuid"
    }
    "#);

    // Test patching webhook with isEditable field
    let (value, code) = server
        .patch_webhook(
            "uuid-whatever",
            json!({
                "isEditable": false,
                "headers": { "another-header": "value" }
            }),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value, { ".uuid" => "[uuid]" }), @r#"
    {
      "message": "Immutable field `isEditable`: expected one of `url`, `headers`",
      "code": "immutable_webhook_is_editable",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_webhook_is_editable"
    }
    "#);
}

#[actix_web::test]
async fn receive_custom_metadata() {
    let WebhookHandle { server_handle: handle1, url: url1, receiver: mut receiver1 } =
        create_webhook_server().await;
    let WebhookHandle { server_handle: handle2, url: url2, receiver: mut receiver2 } =
        create_webhook_server().await;
    let WebhookHandle { server_handle: handle3, url: url3, receiver: mut receiver3 } =
        create_webhook_server().await;

    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(Url::parse(&url3).unwrap()),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    for url in [url1, url2] {
        let (value, code) = server.create_webhook(json!({ "url": url })).await;
        snapshot!(code, @"201 Created");
        snapshot!(json_string!(value, { ".uuid" => "[uuid]", ".url" => "[ignored]" }), @r#"
        {
          "uuid": "[uuid]",
          "isEditable": true,
          "url": "[ignored]",
          "headers": {}
        }
        "#);
    }
    let index = server.index("tamo");
    let (response, code) = index
        .add_documents_with_custom_metadata(
            json!({ "id": 1, "doggo": "bone" }),
            None,
            Some("test_meta"),
        )
        .await;

    snapshot!(response, @r###"
    {
      "taskUid": 0,
      "indexUid": "tamo",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]",
      "customMetadata": "test_meta"
    }
    "###);
    snapshot!(code, @"202 Accepted");

    let mut count1 = 0;
    let mut count2 = 0;
    let mut count3 = 0;
    while count1 == 0 || count2 == 0 || count3 == 0 {
        tokio::select! {
            msg = receiver1.recv() => {
              if let Some(msg) = msg {
                count1 += 1;
                check_metadata(msg);
              }
           },
            msg = receiver2.recv() => {
              if let Some(msg) = msg {
                count2 += 1;
                check_metadata(msg);
              }
             },
            msg = receiver3.recv() => {
              if let Some(msg) = msg {
                count3 += 1;
                check_metadata(msg);
              }
            },
        }
    }

    assert_eq!(count1, 1);
    assert_eq!(count2, 1);
    assert_eq!(count3, 1);

    handle1.abort();
    handle2.abort();
    handle3.abort();
}

fn check_metadata(msg: Vec<u8>) {
    let msg = String::from_utf8(msg).unwrap();
    let tasks = msg.split('\n');
    for task in tasks {
        if task.is_empty() {
            continue;
        }
        let task: serde_json::Value = serde_json::from_str(task).unwrap();
        snapshot!(common::Value(task), @r###"
        {
          "uid": "[uid]",
          "batchUid": "[batch_uid]",
          "indexUid": "tamo",
          "status": "succeeded",
          "type": "documentAdditionOrUpdate",
          "canceledBy": null,
          "details": {
            "receivedDocuments": 1,
            "indexedDocuments": 1
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]",
          "customMetadata": "test_meta"
        }
        "###);
    }
}
