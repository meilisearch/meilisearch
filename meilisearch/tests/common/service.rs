use std::sync::Arc;

use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::test;
use actix_web::test::TestRequest;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch::{analytics, create_app, Opt};
use serde_json::Value;

use crate::common::encoder::Encoder;

pub struct Service {
    pub index_scheduler: Arc<IndexScheduler>,
    pub auth: AuthController,
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

    /// Send a test post request from a text body, with a `content-type:application/json` header.
    pub async fn post_str(
        &self,
        url: impl AsRef<str>,
        body: impl AsRef<str>,
    ) -> (Value, StatusCode) {
        let req = test::TestRequest::post()
            .uri(url.as_ref())
            .set_payload(body.as_ref().to_string())
            .insert_header(("content-type", "application/json"));
        self.request(req).await
    }

    pub async fn get(&self, url: impl AsRef<str>) -> (Value, StatusCode) {
        let req = test::TestRequest::get().uri(url.as_ref());
        self.request(req).await
    }

    pub async fn put(&self, url: impl AsRef<str>, body: Value) -> (Value, StatusCode) {
        self.put_encoded(url, body, Encoder::Plain).await
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

    pub async fn request(&self, mut req: test::TestRequest) -> (Value, StatusCode) {
        let app = test::init_service(create_app(
            self.index_scheduler.clone().into(),
            self.auth.clone(),
            self.options.clone(),
            analytics::MockAnalytics::new(&self.options),
            true,
        ))
        .await;

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
