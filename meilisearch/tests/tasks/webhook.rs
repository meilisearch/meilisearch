use std::sync::Arc;

use actix_http::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceResponse};
use actix_web::web::{Bytes, Data};
use actix_web::{post, App, HttpResponse, HttpServer};
use meili_snap::snapshot;
use meilisearch::Opt;
use tokio::sync::mpsc;

use crate::common::{default_settings, Server};
use crate::json;

#[post("/")]
async fn forward_body(sender: Data<mpsc::Sender<Vec<u8>>>, body: Bytes) -> HttpResponse {
    println!("Received something");
    let body = body.to_vec();
    sender.send(body).await.unwrap();
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

    let server =
        HttpServer::new(move || create_app(sender.clone())).bind(("localhost", 0)).unwrap();
    let (ip, scheme) = server.addrs_with_scheme()[0];
    let url = format!("{scheme}://{ip}/");

    // TODO: Is it cleaned once the test is over
    let server_handle = tokio::spawn(server.run());

    WebhookHandle { server_handle, url, receiver }
}

#[actix_web::test]
async fn test_basic_webhook() {
    // Request a new server from the pool
    let mut handle = create_webhook_server().await;

    let db_path = tempfile::tempdir().unwrap();
    // let (_handle, mut webhook) = create_webhook_server().await;
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(handle.url.clone()),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    println!("Sending something");
    reqwest::Client::new().post(&handle.url).body("hello").send().await.unwrap();

    // let (_, status) = server.create_index(json!({ "uid": "tamo" })).await;
    // snapshot!(status, @"202 Accepted");

    let payload = handle.receiver.recv().await.unwrap();
    let jsonl = String::from_utf8(payload).unwrap();

    // TODO: kill the server
    // handle.server_handle.;

    snapshot!(jsonl,
        @r###"
    {
      "uid": 0,
      "indexUid": null,
      "status": "succeeded",
      "type": "dumpCreation",
      "canceledBy": null,
      "details": {
        "dumpUid": "[dumpUid]"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}
