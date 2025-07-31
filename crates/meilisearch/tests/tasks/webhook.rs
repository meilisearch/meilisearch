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
    let WebhookHandle { server_handle, url, mut receiver } = create_webhook_server().await;

    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(Url::parse(&url).unwrap()),
        task_webhook_authorization_header: Some(String::from("Bearer a-secret-token")),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    let index = server.index("tamo");
    // May be flaky: we're relying on the fact that while the first document addition is processed, the other
    // operations will be received and will be batched together. If it doesn't happen it's not a problem
    // the rest of the test won't assume anything about the number of tasks per batch.
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

    let (webhooks, code) = server.get_webhooks().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(webhooks, { ".results[].url" => "[ignored]" }), @r#"
    {
      "results": [
        {
          "uuid": "00000000-0000-0000-0000-000000000000",
          "isEditable": false,
          "url": "[ignored]",
          "headers": {
            "Authorization": "Bearer a-secret-token"
          }
        }
      ]
    }
    "#);

    server_handle.abort();
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
    snapshot!(webhooks, @r#"
    {
      "results": [
        {
          "uuid": "00000000-0000-0000-0000-000000000000",
          "isEditable": false,
          "url": "http://defined-in-test-cli.com/",
          "headers": {
            "Authorization": "Bearer a-secret-token-defined-in-test-cli"
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
            "authorization": "TOKEN"
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
    "#);
}

#[actix_web::test]
async fn reserved_names() {
    let server = Server::new().await;

    let (value, code) = server
        .set_webhooks(json!({ "webhooks": { Uuid::nil(): { "url": "http://localhost:8080" } } }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Cannot edit webhook `[uuid]`. Webhooks prefixed with an underscore are reserved and may not be modified using the API.",
      "code": "reserved_webhook",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#reserved_webhook"
    }
    "#);

    let (value, code) = server.set_webhooks(json!({ "webhooks": { Uuid::nil(): null } })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Cannot edit webhook `[uuid]`. Webhooks prefixed with an underscore are reserved and may not be modified using the API.",
      "code": "reserved_webhook",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#reserved_webhook"
    }
    "#);
}

#[actix_web::test]
async fn over_limits() {
    let server = Server::new().await;

    // Too many webhooks
    for _ in 0..20 {
        let (_value, code) = server
            .set_webhooks(json!({ "webhooks": { Uuid::new_v4(): { "url": "http://localhost:8080" } } }))
            .await;
        snapshot!(code, @"200 OK");
    }
    let (value, code) = server
        .set_webhooks(json!({ "webhooks": { Uuid::new_v4(): { "url": "http://localhost:8080" } } }))
        .await;
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
    let (value, code) = server.set_webhooks(json!({ "webhooks": null })).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "webhooks": {}
    }
    "#);

    // Test too many headers
    let uuid = Uuid::new_v4();
    for i in 0..200 {
        let header_name = format!("header_{i}");
        let (_value, code) = server
            .set_webhooks(json!({ "webhooks": { uuid: { "url": "http://localhost:8080", "headers": { header_name: "value" } } } }))
            .await;
        snapshot!(code, @"200 OK");
    }
    let (value, code) = server
        .set_webhooks(json!({ "webhooks": { uuid: { "url": "http://localhost:8080", "headers": { "header_201": "value" } } } }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Too many headers for the webhook `[uuid]`. Please limit the number of headers to 200.",
      "code": "invalid_webhooks_headers",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_webhooks_headers"
    }
    "#);
}
