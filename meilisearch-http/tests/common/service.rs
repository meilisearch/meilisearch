use actix_web::{http::StatusCode, test};
use meilisearch_auth::AuthController;
use meilisearch_lib::MeiliSearch;
use serde_json::Value;

use meilisearch_http::{analytics, create_app, Opt};

pub struct Service {
    pub meilisearch: MeiliSearch,
    pub auth: AuthController,
    pub options: Opt,
    pub api_key: Option<String>,
}

impl Service {
    pub async fn post(&self, url: impl AsRef<str>, body: Value) -> (Value, StatusCode) {
        let app = test::init_service(create_app!(
            &self.meilisearch,
            &self.auth,
            true,
            &self.options,
            analytics::MockAnalytics::new(&self.options).0
        ))
        .await;

        let mut req = test::TestRequest::post().uri(url.as_ref()).set_json(&body);
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

    /// Send a test post request from a text body, with a `content-type:application/json` header.
    pub async fn post_str(
        &self,
        url: impl AsRef<str>,
        body: impl AsRef<str>,
    ) -> (Value, StatusCode) {
        let app = test::init_service(create_app!(
            &self.meilisearch,
            &self.auth,
            true,
            &self.options,
            analytics::MockAnalytics::new(&self.options).0
        ))
        .await;

        let mut req = test::TestRequest::post()
            .uri(url.as_ref())
            .set_payload(body.as_ref().to_string())
            .insert_header(("content-type", "application/json"));
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

    pub async fn get(&self, url: impl AsRef<str>) -> (Value, StatusCode) {
        let app = test::init_service(create_app!(
            &self.meilisearch,
            &self.auth,
            true,
            &self.options,
            analytics::MockAnalytics::new(&self.options).0
        ))
        .await;

        let mut req = test::TestRequest::get().uri(url.as_ref());
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

    pub async fn put(&self, url: impl AsRef<str>, body: Value) -> (Value, StatusCode) {
        let app = test::init_service(create_app!(
            &self.meilisearch,
            &self.auth,
            true,
            &self.options,
            analytics::MockAnalytics::new(&self.options).0
        ))
        .await;

        let mut req = test::TestRequest::put().uri(url.as_ref()).set_json(&body);
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

    pub async fn patch(&self, url: impl AsRef<str>, body: Value) -> (Value, StatusCode) {
        let app = test::init_service(create_app!(
            &self.meilisearch,
            &self.auth,
            true,
            &self.options,
            analytics::MockAnalytics::new(&self.options).0
        ))
        .await;

        let mut req = test::TestRequest::patch().uri(url.as_ref()).set_json(&body);
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

    pub async fn delete(&self, url: impl AsRef<str>) -> (Value, StatusCode) {
        let app = test::init_service(create_app!(
            &self.meilisearch,
            &self.auth,
            true,
            &self.options,
            analytics::MockAnalytics::new(&self.options).0
        ))
        .await;

        let mut req = test::TestRequest::delete().uri(url.as_ref());
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
}
