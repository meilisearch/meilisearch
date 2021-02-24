use std::time::Duration;

use actix_web::http::StatusCode;
use serde_json::{json, Value};
use tokio::time::delay_for;

use super::service::Service;

pub struct Index<'a> {
    pub uid: String,
    pub service: &'a Service,
}

impl Index<'_> {
    pub async fn get(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", self.uid);
        self.service.get(url).await
    }

    pub async fn load_test_set(&self) -> u64 {
        let url = format!("/indexes/{}/documents", self.uid);
        let (response, code) = self.service.post_str(url, include_str!("../assets/test_set.json")).await;
        assert_eq!(code, 200);
        let update_id = response["updateId"].as_i64().unwrap();
        self.wait_update_id(update_id as u64).await;
        update_id as u64
    }

    pub async fn create<'a>(&'a self, primary_key: Option<&str>) -> (Value, StatusCode) {
        let body = json!({
            "uid": self.uid,
            "primaryKey": primary_key,
        });
        self.service.post("/indexes", body).await
    }

    pub async fn update(&self, primary_key: Option<&str>) -> (Value, StatusCode) {
        let body = json!({
            "primaryKey": primary_key,
        });
        let url = format!("/indexes/{}", self.uid);

        self.service.put(url, body).await
    }

    pub async fn delete(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", self.uid);
        self.service.delete(url).await
    }

    pub async fn add_documents(
        &self,
        documents: Value,
        primary_key: Option<&str>,
    ) -> (Value, StatusCode) {
        let url = match primary_key {
            Some(key) => format!("/indexes/{}/documents?primaryKey={}", self.uid, key),
            None => format!("/indexes/{}/documents", self.uid),
        };
        self.service.post(url, documents).await
    }

    pub async fn update_documents(&self, documents: Value, primary_key: Option<&str>) -> (Value, StatusCode) {
        let url = match primary_key {
            Some(key) => format!("/indexes/{}/documents?primaryKey={}", self.uid, key),
            None => format!("/indexes/{}/documents", self.uid),
        };
        self.service.put(url, documents).await
    }

    pub async fn wait_update_id(&self, update_id: u64) -> Value {
        // try 10 times to get status, or panic to not wait forever
        let url = format!("/indexes/{}/updates/{}", self.uid, update_id);
        for _ in 0..10 {
            let (response, status_code) = self.service.get(&url).await;
            assert_eq!(status_code, 200);

            if response["status"] == "processed" || response["status"] == "failed" {
                return response;
            }

            delay_for(Duration::from_secs(1)).await;
        }
        panic!("Timeout waiting for update id");
    }

    pub async fn get_update(&self, udpate_id: u64) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/updates/{}", self.uid, udpate_id);
        self.service.get(url).await
    }

    #[allow(dead_code)]
    pub async fn list_updates(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/updates", self.uid);
        self.service.get(url).await
    }

    pub async fn get_document(&self, id: u64, _options: Option<GetDocumentOptions>) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/{}", self.uid, id);
        self.service.get(url).await
    }

    pub async fn get_all_documents(&self, options: GetAllDocumentsOptions) -> (Value, StatusCode) {
        let mut url = format!("/indexes/{}/documents?", self.uid);
        if let Some(limit) = options.limit {
            url.push_str(&format!("limit={}&", limit));
        }

        if let Some(offset) = options.offset {
            url.push_str(&format!("offset={}&", offset));
        }

        if let Some(attributes_to_retrieve) = options.attributes_to_retrieve {
            url.push_str(&format!("attributesToRetrieve={}&", attributes_to_retrieve.join(",")));
        }

        self.service.get(url).await
    }

    pub async fn delete_document(&self, id: u64) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/{}", self.uid, id);
        self.service.delete(url).await
    }

    pub async fn clear_all_documents(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents", self.uid);
        self.service.delete(url).await
    }

    pub async fn delete_batch(&self, ids: Vec<u64>) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/delete-batch", self.uid);
        self.service.post(url, serde_json::to_value(&ids).unwrap()).await
    }

    pub async fn settings(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", self.uid);
        self.service.get(url).await
    }

    pub async fn update_settings(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", self.uid);
        self.service.post(url, settings).await
    }
}

pub struct GetDocumentOptions;

#[derive(Debug, Default)]
pub struct GetAllDocumentsOptions {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub attributes_to_retrieve: Option<Vec<&'static str>>,
}
