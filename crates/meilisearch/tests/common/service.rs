use std::num::NonZeroUsize;
use std::sync::Arc;

use actix_web::body::MessageBody;
use actix_web::dev::ServiceResponse;
use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::test;
use actix_web::test::TestRequest;
use actix_web::web::Data;
use index_scheduler::IndexScheduler;
use meilisearch::analytics::Analytics;
use meilisearch::search_queue::SearchQueue;
use meilisearch::{create_app, Opt, SubscriberForSecondLayer};
use meilisearch_auth::AuthController;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;

use crate::common::encoder::Encoder;
use crate::common::Value;

pub struct Service {
    pub index_scheduler: Arc<IndexScheduler>,
    pub auth: Arc<AuthController>,
    pub options: Opt,
    pub api_key: Option<String>,
}

impl Service {
    pub async fn post(&self, url: impl AsRef<str>, body: Value) -> (Value, StatusCode) {
        self.post_encoded(url, body, Encoder::Plain).await
    }

    pub async fn post_encoded(
        &self,
        url: impl AsRef<str>,
        body: Value,
        encoder: Encoder,
    ) -> (Value, StatusCode) {
        let mut req = test::TestRequest::post().uri(url.as_ref());
        req = self.encode(req, body, encoder);
        self.request(req).await
    }

    /// Send a test post request from a text body.
    pub async fn post_str(
        &self,
        url: impl AsRef<str>,
        body: impl AsRef<str>,
        headers: Vec<(&str, &str)>,
    ) -> (Value, StatusCode) {
        let mut req =
            test::TestRequest::post().uri(url.as_ref()).set_payload(body.as_ref().to_string());
        for header in headers {
            req = req.insert_header(header);
        }
        self.request(req).await
    }

    pub async fn get(&self, url: impl AsRef<str>) -> (Value, StatusCode) {
        let req = test::TestRequest::get().uri(url.as_ref());
        self.request(req).await
    }

    pub async fn put(&self, url: impl AsRef<str>, body: Value) -> (Value, StatusCode) {
        self.put_encoded(url, body, Encoder::Plain).await
    }

    /// Send a test put request from a text body.
    pub async fn put_str(
        &self,
        url: impl AsRef<str>,
        body: impl AsRef<str>,
        headers: Vec<(&str, &str)>,
    ) -> (Value, StatusCode) {
        let mut req =
            test::TestRequest::put().uri(url.as_ref()).set_payload(body.as_ref().to_string());
        for header in headers {
            req = req.insert_header(header);
        }
        self.request(req).await
    }

    pub async fn put_encoded(
        &self,
        url: impl AsRef<str>,
        body: Value,
        encoder: Encoder,
    ) -> (Value, StatusCode) {
        let mut req = test::TestRequest::put().uri(url.as_ref());
        req = self.encode(req, body, encoder);
        self.request(req).await
    }

    pub async fn patch(&self, url: impl AsRef<str>, body: Value) -> (Value, StatusCode) {
        self.patch_encoded(url, body, Encoder::Plain).await
    }

    pub async fn patch_encoded(
        &self,
        url: impl AsRef<str>,
        body: Value,
        encoder: Encoder,
    ) -> (Value, StatusCode) {
        let mut req = test::TestRequest::patch().uri(url.as_ref());
        req = self.encode(req, body, encoder);
        self.request(req).await
    }

    pub async fn delete(&self, url: impl AsRef<str>) -> (Value, StatusCode) {
        let req = test::TestRequest::delete().uri(url.as_ref());
        self.request(req).await
    }

    pub async fn init_web_app(
        &self,
    ) -> impl actix_web::dev::Service<
        actix_http::Request,
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
    > {
        let (_route_layer, route_layer_handle) =
            tracing_subscriber::reload::Layer::new(None.with_filter(
                tracing_subscriber::filter::Targets::new().with_target("", LevelFilter::OFF),
            ));
        let (_stderr_layer, stderr_layer_handle) = tracing_subscriber::reload::Layer::new(
            (Box::new(
                tracing_subscriber::fmt::layer()
                    .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE),
            )
                as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>)
                .with_filter(tracing_subscriber::filter::Targets::new()),
        );
        let search_queue = SearchQueue::new(
            self.options.experimental_search_queue_size,
            NonZeroUsize::new(1).unwrap(),
        );

        actix_web::test::init_service(create_app(
            self.index_scheduler.clone().into(),
            self.auth.clone().into(),
            Data::new(search_queue),
            self.options.clone(),
            (route_layer_handle, stderr_layer_handle),
            Data::new(Analytics::no_analytics()),
            true,
        ))
        .await
    }

    pub async fn request(&self, mut req: test::TestRequest) -> (Value, StatusCode) {
        let app = self.init_web_app().await;

        if let Some(api_key) = &self.api_key {
            req = req.insert_header(("Authorization", ["Bearer ", api_key].concat()));
        }
        let req = req.to_request();
        let res = test::call_service(&app, req).await;
        let status_code = res.status();

        let body = test::read_body(res).await;
        let response = serde_json::from_slice(&body).unwrap_or_default();
        (response, status_code)
    }

    fn encode(&self, req: TestRequest, body: Value, encoder: Encoder) -> TestRequest {
        let bytes = serde_json::to_string(&body).expect("Failed to serialize test data to json");
        let encoded_body = encoder.encode(bytes);
        let header = encoder.header();
        match header {
            Some(header) => req.insert_header(header),
            None => req,
        }
        .set_payload(encoded_body)
        .insert_header(ContentType::json())
    }
}
