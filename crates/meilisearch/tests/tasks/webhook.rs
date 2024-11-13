//! To test the webhook, we need to spawn a new server with a URL listening for
//! post requests. The webhook handle starts a server and forwards all the
//! received requests into a channel for you to handle.

use std::sync::Arc;

use actix_http::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceResponse};
use actix_web::web::{Bytes, Data};
use actix_web::{post, App, HttpRequest, HttpResponse, HttpServer};
use meili_snap::snapshot;
use meilisearch::Opt;
use tokio::sync::mpsc;
use url::Url;

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
async fn test_basic_webhook() {
    let WebhookHandle { server_handle, url, mut receiver } = create_webhook_server().await;

    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(Url::parse(&url).unwrap()),
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

    server_handle.abort();
}
