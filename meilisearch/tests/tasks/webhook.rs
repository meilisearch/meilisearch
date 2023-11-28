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
    for i in 0..3 {
        let (_, _status) = index.add_documents(json!({ "id": i, "doggo": "bone" }), None).await;
    }

    let payload = handle.receiver.recv().await.unwrap();
    let jsonl = String::from_utf8(payload).unwrap();

    snapshot!(jsonl,
        @r###"{"uid":0,"enqueuedAt":"2023-11-28T13:43:24.754587Z","startedAt":"2023-11-28T13:43:24.756445Z","finishedAt":"2023-11-28T13:43:24.791527Z","error":null,"canceledBy":null,"details":{"DocumentAdditionOrUpdate":{"received_documents":1,"indexed_documents":1}},"status":"succeeded","kind":{"documentAdditionOrUpdate":{"index_uid":"tamo","primary_key":null,"method":"ReplaceDocuments","content_file":"ca77ac82-4504-4c85-81a5-1a8d68f1a386","documents_count":1,"allow_index_creation":true}}}"###);

    let payload = handle.receiver.recv().await.unwrap();
    let jsonl = String::from_utf8(payload).unwrap();

    snapshot!(jsonl,
        @r###"{"uid":1,"enqueuedAt":"2023-11-28T13:43:24.761498Z","startedAt":"2023-11-28T13:43:24.793989Z","finishedAt":"2023-11-28T13:43:24.814623Z","error":null,"canceledBy":null,"details":{"DocumentAdditionOrUpdate":{"received_documents":1,"indexed_documents":1}},"status":"succeeded","kind":{"documentAdditionOrUpdate":{"index_uid":"tamo","primary_key":null,"method":"ReplaceDocuments","content_file":"c947aefa-7f98-433d-8ce4-5926d8d2ce10","documents_count":1,"allow_index_creation":true}}}{"uid":2,"enqueuedAt":"2023-11-28T13:43:24.76776Z","startedAt":"2023-11-28T13:43:24.793989Z","finishedAt":"2023-11-28T13:43:24.814623Z","error":null,"canceledBy":null,"details":{"DocumentAdditionOrUpdate":{"received_documents":1,"indexed_documents":1}},"status":"succeeded","kind":{"documentAdditionOrUpdate":{"index_uid":"tamo","primary_key":null,"method":"ReplaceDocuments","content_file":"a21d6da6-9322-4827-8c08-f33d2e1b6cae","documents_count":1,"allow_index_creation":true}}}"###);
}
