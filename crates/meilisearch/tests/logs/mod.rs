mod error;

use std::num::NonZeroUsize;
use std::rc::Rc;
use std::str::FromStr;

use actix_web::http::header::ContentType;
use actix_web::web::Data;
use meili_snap::snapshot;
use meilisearch::analytics::Analytics;
use meilisearch::search_queue::SearchQueue;
use meilisearch::{create_app, Opt, SubscriberForSecondLayer};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

use crate::common::{default_settings, Server};
use crate::json;

#[actix_web::test]
async fn basic_test_log_stream_route() {
    let db_path = tempfile::tempdir().unwrap();
    let server = Server::new_with_options(Opt {
        experimental_enable_logs_route: true,
        ..default_settings(db_path.path())
    })
    .await
    .unwrap();

    let (route_layer, route_layer_handle) =
        tracing_subscriber::reload::Layer::new(None.with_filter(
            tracing_subscriber::filter::Targets::new().with_target("", LevelFilter::OFF),
        ));
    let (_stderr_layer, stderr_layer_handle) = tracing_subscriber::reload::Layer::new(
        (Box::new(
            tracing_subscriber::fmt::layer()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE),
        ) as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>)
            .with_filter(tracing_subscriber::filter::Targets::new()),
    );

    let subscriber = tracing_subscriber::registry().with(route_layer).with(
        tracing_subscriber::fmt::layer()
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE)
            .with_filter(tracing_subscriber::filter::LevelFilter::from_str("OFF").unwrap()),
    );
    let search_queue = SearchQueue::new(
        server.service.options.experimental_search_queue_size,
        NonZeroUsize::new(1).unwrap(),
    );

    let app = actix_web::test::init_service(create_app(
        server.service.index_scheduler.clone().into(),
        server.service.auth.clone().into(),
        Data::new(search_queue),
        server.service.options.clone(),
        (route_layer_handle, stderr_layer_handle),
        Data::new(Analytics::no_analytics()),
        true,
    ))
    .await;

    // set the subscriber as the default for the application
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let app = Rc::new(app);

    // First, we start listening on the `/logs/stream` route
    let handle_app = app.clone();
    let handle = tokio::task::spawn_local(async move {
        let req = actix_web::test::TestRequest::post()
            .uri("/logs/stream")
            .insert_header(ContentType::json())
            .set_payload(
                serde_json::to_vec(&json!({
                    "mode": "human",
                    "target": "info",
                }))
                .unwrap(),
            );
        let req = req.to_request();
        let ret = actix_web::test::call_service(&*handle_app, req).await;
        actix_web::test::read_body(ret).await
    });

    // We're going to create an index to get at least one info log saying we processed a batch of task
    let (ret, _code) = server.create_index(json!({ "uid": "tamo" })).await;
    snapshot!(ret, @r###"
    {
      "taskUid": 0,
      "indexUid": "tamo",
      "status": "enqueued",
      "type": "indexCreation",
      "enqueuedAt": "[date]"
    }
    "###);
    server.wait_task(ret.uid()).await.succeeded();

    let req = actix_web::test::TestRequest::delete().uri("/logs/stream");
    let req = req.to_request();
    let ret = actix_web::test::call_service(&*app, req).await;
    let code = ret.status();
    snapshot!(code, @"204 No Content");

    let logs = handle.await.unwrap();
    let logs = String::from_utf8(logs.to_vec()).unwrap();
    assert!(logs.contains("INFO"), "{logs}");
}
