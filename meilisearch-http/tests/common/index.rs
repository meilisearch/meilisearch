use std::{
    fmt::Write,
    panic::{catch_unwind, resume_unwind, UnwindSafe},
    time::Duration,
};

use actix_web::http::StatusCode;
use serde_json::{json, Value};
use tokio::time::sleep;
use urlencoding::encode;

use super::service::Service;

pub struct Index<'a> {
    pub uid: String,
    pub service: &'a Service,
}

#[allow(dead_code)]
impl Index<'_> {
    pub async fn get(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", encode(self.uid.as_ref()));
        self.service.get(url).await
    }

    pub async fn load_test_set(&self) -> u64 {
        let url = format!("/indexes/{}/documents", encode(self.uid.as_ref()));
        let (response, code) = self
            .service
            .post_str(url, include_str!("../assets/test_set.json"))
            .await;
        assert_eq!(code, 202);
        let update_id = response["taskUid"].as_i64().unwrap();
        self.wait_task(update_id as u64).await;
        update_id as u64
    }

    pub async fn create(&self, primary_key: Option<&str>) -> (Value, StatusCode) {
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
        let url = format!("/indexes/{}", encode(self.uid.as_ref()));

        self.service.patch(url, body).await
    }

    pub async fn delete(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", encode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn add_documents(
        &self,
        documents: Value,
        primary_key: Option<&str>,
    ) -> (Value, StatusCode) {
        let url = match primary_key {
            Some(key) => format!(
                "/indexes/{}/documents?primaryKey={}",
                encode(self.uid.as_ref()),
                key
            ),
            None => format!("/indexes/{}/documents", encode(self.uid.as_ref())),
        };
        self.service.post(url, documents).await
    }

    pub async fn update_documents(
        &self,
        documents: Value,
        primary_key: Option<&str>,
    ) -> (Value, StatusCode) {
        let url = match primary_key {
            Some(key) => format!(
                "/indexes/{}/documents?primaryKey={}",
                encode(self.uid.as_ref()),
                key
            ),
            None => format!("/indexes/{}/documents", encode(self.uid.as_ref())),
        };
        self.service.put(url, documents).await
    }

    pub async fn wait_task(&self, update_id: u64) -> Value {
        // try several times to get status, or panic to not wait forever
        let url = format!("/tasks/{}", update_id);
        for _ in 0..100 {
            let (response, status_code) = self.service.get(&url).await;
            assert_eq!(200, status_code, "response: {}", response);

            if response["status"] == "succeeded" || response["status"] == "failed" {
                return response;
            }

            // wait 0.5 second.
            sleep(Duration::from_millis(500)).await;
        }
        panic!("Timeout waiting for update id");
    }

    pub async fn get_task(&self, update_id: u64) -> (Value, StatusCode) {
        let url = format!("/tasks/{}", update_id);
        self.service.get(url).await
    }

    pub async fn list_tasks(&self) -> (Value, StatusCode) {
        let url = format!("/tasks?indexUid={}", self.uid);
        self.service.get(url).await
    }

    pub async fn filtered_tasks(&self, type_: &[&str], status: &[&str]) -> (Value, StatusCode) {
        let mut url = format!("/tasks?indexUid={}", self.uid);
        if !type_.is_empty() {
            let _ = write!(url, "&type={}", type_.join(","));
        }
        if !status.is_empty() {
            let _ = write!(url, "&status={}", status.join(","));
        }
        self.service.get(url).await
    }

    pub async fn get_document(
        &self,
        id: u64,
        options: Option<GetDocumentOptions>,
    ) -> (Value, StatusCode) {
        let mut url = format!("/indexes/{}/documents/{}", encode(self.uid.as_ref()), id);
        if let Some(fields) = options.and_then(|o| o.fields) {
            let _ = write!(url, "?fields={}", fields.join(","));
        }
        self.service.get(url).await
    }

    pub async fn get_all_documents(&self, options: GetAllDocumentsOptions) -> (Value, StatusCode) {
        let mut url = format!("/indexes/{}/documents?", encode(self.uid.as_ref()));
        if let Some(limit) = options.limit {
            let _ = write!(url, "limit={}&", limit);
        }

        if let Some(offset) = options.offset {
            let _ = write!(url, "offset={}&", offset);
        }

        if let Some(attributes_to_retrieve) = options.attributes_to_retrieve {
            let _ = write!(url, "fields={}&", attributes_to_retrieve.join(","));
        }

        self.service.get(url).await
    }

    pub async fn delete_document(&self, id: u64) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/{}", encode(self.uid.as_ref()), id);
        self.service.delete(url).await
    }

    pub async fn clear_all_documents(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents", encode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn delete_batch(&self, ids: Vec<u64>) -> (Value, StatusCode) {
        let url = format!(
            "/indexes/{}/documents/delete-batch",
            encode(self.uid.as_ref())
        );
        self.service
            .post(url, serde_json::to_value(&ids).unwrap())
            .await
    }

    pub async fn settings(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", encode(self.uid.as_ref()));
        self.service.get(url).await
    }

    pub async fn update_settings(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", encode(self.uid.as_ref()));
        self.service.patch(url, settings).await
    }

    pub async fn delete_settings(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", encode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn stats(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/stats", encode(self.uid.as_ref()));
        self.service.get(url).await
    }

    /// Performs both GET and POST search queries
    pub async fn search(
        &self,
        query: Value,
        test: impl Fn(Value, StatusCode) + UnwindSafe + Clone,
    ) {
        let (response, code) = self.search_post(query.clone()).await;
        let t = test.clone();
        if let Err(e) = catch_unwind(move || t(response, code)) {
            eprintln!("Error with post search");
            resume_unwind(e);
        }

        let (response, code) = self.search_get(query).await;
        if let Err(e) = catch_unwind(move || test(response, code)) {
            eprintln!("Error with get search");
            resume_unwind(e);
        }
    }

    pub async fn search_post(&self, query: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/search", encode(self.uid.as_ref()));
        self.service.post(url, query).await
    }

    pub async fn search_get(&self, query: Value) -> (Value, StatusCode) {
        let params = yaup::to_string(&query).unwrap();
        let url = format!("/indexes/{}/search?{}", encode(self.uid.as_ref()), params);
        self.service.get(url).await
    }

    pub async fn update_distinct_attribute(&self, value: Value) -> (Value, StatusCode) {
        let url = format!(
            "/indexes/{}/settings/{}",
            encode(self.uid.as_ref()),
            "distinct-attribute"
        );
        self.service.put(url, value).await
    }

    pub async fn get_distinct_attribute(&self) -> (Value, StatusCode) {
        let url = format!(
            "/indexes/{}/settings/{}",
            encode(self.uid.as_ref()),
            "distinct-attribute"
        );
        self.service.get(url).await
    }
}

pub struct GetDocumentOptions {
    pub fields: Option<Vec<&'static str>>,
}

#[derive(Debug, Default)]
pub struct GetAllDocumentsOptions {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub attributes_to_retrieve: Option<Vec<&'static str>>,
}
