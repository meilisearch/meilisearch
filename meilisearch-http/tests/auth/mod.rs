mod api_keys;
mod authorization;
mod payload;
mod tenant_token;

use actix_web::http::StatusCode;
use serde_json::{json, Value};

use crate::common::Server;

impl Server {
    pub fn use_api_key(&mut self, api_key: impl AsRef<str>) {
        self.service.api_key = Some(api_key.as_ref().to_string());
    }

    /// Fetch and use the default admin key for nexts http requests.
    pub async fn use_admin_key(&mut self, master_key: impl AsRef<str>) {
        self.use_api_key(master_key);
        let (response, code) = self.list_api_keys().await;
        assert_eq!(200, code, "{:?}", response);
        let admin_key = &response["results"][1]["key"];
        self.use_api_key(admin_key.as_str().unwrap());
    }

    pub async fn add_api_key(&self, content: Value) -> (Value, StatusCode) {
        let url = "/keys";
        self.service.post(url, content).await
    }

    pub async fn get_api_key(&self, key: impl AsRef<str>) -> (Value, StatusCode) {
        let url = format!("/keys/{}", key.as_ref());
        self.service.get(url).await
    }

    pub async fn patch_api_key(&self, key: impl AsRef<str>, content: Value) -> (Value, StatusCode) {
        let url = format!("/keys/{}", key.as_ref());
        self.service.patch(url, content).await
    }

    pub async fn list_api_keys(&self) -> (Value, StatusCode) {
        let url = "/keys";
        self.service.get(url).await
    }

    pub async fn delete_api_key(&self, key: impl AsRef<str>) -> (Value, StatusCode) {
        let url = format!("/keys/{}", key.as_ref());
        self.service.delete(url).await
    }

    pub async fn dummy_request(
        &self,
        method: impl AsRef<str>,
        url: impl AsRef<str>,
    ) -> (Value, StatusCode) {
        match method.as_ref() {
            "POST" => self.service.post(url, json!({})).await,
            "PUT" => self.service.put(url, json!({})).await,
            "PATCH" => self.service.patch(url, json!({})).await,
            "GET" => self.service.get(url).await,
            "DELETE" => self.service.delete(url).await,
            _ => unreachable!(),
        }
    }
}
