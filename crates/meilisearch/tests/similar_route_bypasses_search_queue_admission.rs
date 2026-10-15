// Copy to crates/meilisearch/tests/similar_route_bypasses_search_queue_admission.rs and run:
//   cargo test -p meilisearch --test similar_route_bypasses_search_queue_admission -- --nocapture

#[path = "common/mod.rs"]
mod common;

use std::num::NonZeroUsize;

use actix_web::http::StatusCode;
use actix_web::test;
use actix_web::test::TestRequest;
use actix_web::web::Data;
use common::server::Server;
use common::Value;
use meilisearch::analytics::Analytics;
use meilisearch::personalization::PersonalizationService;
use meilisearch::search_queue::SearchQueue;
use meilisearch::{create_app, ServicesData, SubscriberForSecondLayer};
use meilisearch_types::milli::progress::Progress;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;

fn json_request(url: &str, body: serde_json::Value) -> actix_http::Request {
    TestRequest::post()
        .uri(url)
        .insert_header(("Content-Type", "application/json"))
        .set_payload(serde_json::to_vec(&body).unwrap())
        .to_request()
}

#[actix_rt::test]
async fn similar_route_must_take_a_search_permit() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Index with a userProvided embedder and three vectors.
    let (task, code) = index
        .update_settings(Value(serde_json::json!({
            "embedders": { "manual": { "source": "userProvided", "dimensions": 3 } }
        })))
        .await;
    assert_eq!(code, StatusCode::ACCEPTED, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index
        .add_documents(
            Value(serde_json::json!([
                { "id": 0, "title": "a", "_vectors": { "manual": [1.0, 0.0, 0.0] } },
                { "id": 1, "title": "b", "_vectors": { "manual": [0.9, 0.1, 0.0] } },
                { "id": 2, "title": "c", "_vectors": { "manual": [0.0, 1.0, 0.0] } },
            ])),
            None,
        )
        .await;
    assert_eq!(code, StatusCode::ACCEPTED, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    // A second app on the same IndexScheduler, with a saturated search queue:
    // capacity 0 and the only permit already held.
    let queue = SearchQueue::new(0, NonZeroUsize::new(1).unwrap());
    let _held_permit = queue
        .try_get_search_permit(&Progress::quiet())
        .await
        .expect("the queue must hand out its single permit");

    let (_route_layer, route_layer_handle) =
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

    let app = test::init_service(create_app(
        ServicesData {
            index_scheduler: server.service.index_scheduler.clone().into(),
            auth: server.service.auth.clone().into(),
            search_queue: Data::new(queue),
            personalization_service: Data::new(PersonalizationService::disabled()),
            logs_route_handle: Data::new(route_layer_handle),
            logs_stderr_handle: Data::new(stderr_layer_handle),
            analytics: Data::new(Analytics::no_analytics()),
        },
        server.service.options.clone(),
        true,
    ))
    .await;

    // Control: a search on the same saturated queue is refused.
    let resp = test::call_service(
        &app,
        json_request(&format!("/indexes/{}/search", index.uid), serde_json::json!({ "q": "" })),
    )
    .await;
    let search_status = resp.status();
    assert_eq!(
        search_status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the saturated SearchQueue must refuse a normal search, got {search_status}"
    );

    // Control: facet search is refused too.
    let resp = test::call_service(
        &app,
        json_request(
            &format!("/indexes/{}/facet-search", index.uid),
            serde_json::json!({ "facetName": "title", "facetQuery": "a" }),
        ),
    )
    .await;
    let facet_search_status = resp.status();
    assert_eq!(
        facet_search_status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the saturated SearchQueue must refuse a facet search, got {facet_search_status}"
    );

    // The similar route must be refused as well.
    let resp = test::call_service(
        &app,
        json_request(
            &format!("/indexes/{}/similar", index.uid),
            serde_json::json!({ "id": 0, "embedder": "manual" }),
        ),
    )
    .await;
    let similar_status = resp.status();
    let similar_body: serde_json::Value =
        serde_json::from_slice(&test::read_body(resp).await).unwrap_or_default();

    assert_eq!(
        similar_status,
        StatusCode::SERVICE_UNAVAILABLE,
        "VULNERABILITY REPRODUCED: `POST /indexes/{{index_uid}}/similar` answered \
         {similar_status} while the SearchQueue was saturated (capacity 0, single permit \
         already held). The control searches were refused with {search_status} and \
         {facet_search_status}, but the similar handler never calls \
         `search_queue.try_get_search_permit` \
         (crates/meilisearch/src/routes/indexes/similar.rs:204-247), so its vector search runs \
         in `spawn_blocking` with no admission control at all. Body: {similar_body}"
    );
}
