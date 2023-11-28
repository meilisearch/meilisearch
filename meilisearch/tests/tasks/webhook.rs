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
async fn forward_body(sender: Data<mpsc::UnboundedSender<Vec<u8>>>, body: Bytes) -> HttpResponse {
    println!("Received something");
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
    let mut log_builder = env_logger::Builder::new();
    log_builder.parse_filters("debug");
    log_builder.init();

    let (sender, receiver) = mpsc::unbounded_channel();
    let sender = Arc::new(sender);

    let server =
        HttpServer::new(move || create_app(sender.clone())).bind(("127.0.0.1", 0)).unwrap();
    let (ip, scheme) = server.addrs_with_scheme()[0];
    let url = format!("{scheme}://{ip}/");
    println!("url is {url}");

    // TODO: Is it cleaned once the test is over
    let server_handle = tokio::spawn(server.run());

    WebhookHandle { server_handle, url, receiver }
}

#[actix_web::test]
async fn test_basic_webhook() {
    // Request a new server from the pool
    let mut handle = create_webhook_server().await;

    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        task_webhook_url: Some(handle.url.clone()),
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    let index = server.index("tamo");
    // TODO: may be flaky, we're relying on the fact that during the time the first document addition succeed, the two other operations will be received.
    for i in 0..3 {
        let (_, _status) = index.add_documents(json!({ "id": i, "doggo": "bone" }), None).await;
    }

    let payload = handle.receiver.recv().await.unwrap();
    let jsonl = String::from_utf8(payload).unwrap();

    snapshot!(jsonl,
        @r###"
    {"uid":0,"indexUid":"tamo","status":"succeeded","type":"documentAdditionOrUpdate","canceledBy":null,"details":{"receivedDocuments":1,"indexedDocuments":1},"error":null,"duration":"PT0.027444S","enqueuedAt":"2023-11-28T14:05:37.767678Z","startedAt":"2023-11-28T14:05:37.769519Z","finishedAt":"2023-11-28T14:05:37.796963Z"}
    "###);

    let payload = handle.receiver.recv().await.unwrap();
    let jsonl = String::from_utf8(payload).unwrap();

    snapshot!(jsonl,
        @r###"
    {"uid":1,"indexUid":"tamo","status":"succeeded","type":"documentAdditionOrUpdate","canceledBy":null,"details":{"receivedDocuments":1,"indexedDocuments":1},"error":null,"duration":"PT0.020221S","enqueuedAt":"2023-11-28T14:05:37.773731Z","startedAt":"2023-11-28T14:05:37.799448Z","finishedAt":"2023-11-28T14:05:37.819669Z"}
    {"uid":2,"indexUid":"tamo","status":"succeeded","type":"documentAdditionOrUpdate","canceledBy":null,"details":{"receivedDocuments":1,"indexedDocuments":1},"error":null,"duration":"PT0.020221S","enqueuedAt":"2023-11-28T14:05:37.780466Z","startedAt":"2023-11-28T14:05:37.799448Z","finishedAt":"2023-11-28T14:05:37.819669Z"}
    "###);
}
