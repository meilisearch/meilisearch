use std::fmt::Write;
use std::panic::{catch_unwind, resume_unwind, UnwindSafe};
use std::time::Duration;

use actix_web::http::StatusCode;
use serde_json::{json, Value};
use tokio::time::sleep;
use urlencoding::encode as urlencode;

use super::encoder::Encoder;
use super::service::Service;

pub struct Index<'a> {
    pub uid: String,
    pub service: &'a Service,
    pub encoder: Encoder,
}

#[allow(dead_code)]
impl Index<'_> {
    pub async fn get(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));
        self.service.get(url).await
    }

    pub async fn load_test_set(&self) -> u64 {
        let url = format!("/indexes/{}/documents", urlencode(self.uid.as_ref()));
        let (response, code) = self
            .service
            .post_str(
                url,
                include_str!("../assets/test_set.json"),
                ("content-type", "application/json"),
            )
            .await;
        assert_eq!(code, 202);
        let update_id = response["taskUid"].as_i64().unwrap();
        self.wait_task(update_id as u64).await;
        update_id as u64
    }

    pub async fn load_test_set_ndjson(&self) -> u64 {
        let url = format!("/indexes/{}/documents", urlencode(self.uid.as_ref()));
        let (response, code) = self
            .service
            .post_str(
                url,
                include_str!("../assets/test_set.ndjson"),
                ("content-type", "application/x-ndjson"),
            )
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
        self.service.post_encoded("/indexes", body, self.encoder).await
    }

    pub async fn update(&self, primary_key: Option<&str>) -> (Value, StatusCode) {
        let body = json!({
            "primaryKey": primary_key,
        });
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));

        self.service.patch_encoded(url, body, self.encoder).await
    }

    pub async fn delete(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn add_documents(
        &self,
        documents: Value,
        primary_key: Option<&str>,
    ) -> (Value, StatusCode) {
        let url = match primary_key {
            Some(key) => {
                format!("/indexes/{}/documents?primaryKey={}", urlencode(self.uid.as_ref()), key)
            }
            None => format!("/indexes/{}/documents", urlencode(self.uid.as_ref())),
        };
        self.service.post_encoded(url, documents, self.encoder).await
    }

    pub async fn update_documents(
        &self,
        documents: Value,
        primary_key: Option<&str>,
    ) -> (Value, StatusCode) {
        let url = match primary_key {
            Some(key) => {
                format!("/indexes/{}/documents?primaryKey={}", urlencode(self.uid.as_ref()), key)
            }
            None => format!("/indexes/{}/documents", urlencode(self.uid.as_ref())),
        };
        self.service.put_encoded(url, documents, self.encoder).await
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
        let url = format!("/tasks?indexUids={}", self.uid);
        self.service.get(url).await
    }

    pub async fn filtered_tasks(
        &self,
        types: &[&str],
        statuses: &[&str],
        canceled_by: &[&str],
    ) -> (Value, StatusCode) {
        let mut url = format!("/tasks?indexUids={}", self.uid);
        if !types.is_empty() {
            let _ = write!(url, "&types={}", types.join(","));
        }
        if !statuses.is_empty() {
            let _ = write!(url, "&statuses={}", statuses.join(","));
        }
        if !canceled_by.is_empty() {
            let _ = write!(url, "&canceledBy={}", canceled_by.join(","));
        }
        self.service.get(url).await
    }

    pub async fn get_document(
        &self,
        id: u64,
        options: Option<GetDocumentOptions>,
    ) -> (Value, StatusCode) {
        let mut url = format!("/indexes/{}/documents/{}", urlencode(self.uid.as_ref()), id);
        if let Some(fields) = options.and_then(|o| o.fields) {
            let _ = write!(url, "?fields={}", fields.join(","));
        }
        self.service.get(url).await
    }

    pub async fn get_all_documents_raw(&self, options: &str) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents{}", urlencode(self.uid.as_ref()), options);
        self.service.get(url).await
    }

    pub async fn get_all_documents(&self, options: GetAllDocumentsOptions) -> (Value, StatusCode) {
        let mut url = format!("/indexes/{}/documents?", urlencode(self.uid.as_ref()));
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
        let url = format!("/indexes/{}/documents/{}", urlencode(self.uid.as_ref()), id);
        self.service.delete(url).await
    }

    pub async fn clear_all_documents(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents", urlencode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn delete_batch(&self, ids: Vec<u64>) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/delete-batch", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, serde_json::to_value(&ids).unwrap(), self.encoder).await
    }

    pub async fn delete_batch_raw(&self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/delete-batch", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, body, self.encoder).await
    }

    pub async fn settings(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", urlencode(self.uid.as_ref()));
        self.service.get(url).await
    }

    pub async fn update_settings(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_displayed_attributes(
        &self,
        settings: Value,
    ) -> (Value, StatusCode) {
        let url =
            format!("/indexes/{}/settings/displayed-attributes", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_searchable_attributes(
        &self,
        settings: Value,
    ) -> (Value, StatusCode) {
        let url =
            format!("/indexes/{}/settings/searchable-attributes", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_filterable_attributes(
        &self,
        settings: Value,
    ) -> (Value, StatusCode) {
        let url =
            format!("/indexes/{}/settings/filterable-attributes", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_sortable_attributes(
        &self,
        settings: Value,
    ) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/sortable-attributes", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_ranking_rules(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/ranking-rules", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_stop_words(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/stop-words", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_synonyms(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/synonyms", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_distinct_attribute(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/distinct-attribute", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_typo_tolerance(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/typo-tolerance", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_faceting(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/faceting", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_pagination(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/pagination", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, settings, self.encoder).await
    }

    pub async fn delete_settings(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", urlencode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn stats(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/stats", urlencode(self.uid.as_ref()));
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
        let query = yaup::to_string(&query).unwrap();
        let (response, code) = self.search_get(&query).await;
        if let Err(e) = catch_unwind(move || test(response, code)) {
            eprintln!("Error with get search");
            resume_unwind(e);
        }
    }

    pub async fn search_post(&self, query: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/search", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, query, self.encoder).await
    }

    pub async fn search_get(&self, query: &str) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/search?{}", urlencode(self.uid.as_ref()), query);
        self.service.get(url).await
    }

    pub async fn update_distinct_attribute(&self, value: Value) -> (Value, StatusCode) {
        let url =
            format!("/indexes/{}/settings/{}", urlencode(self.uid.as_ref()), "distinct-attribute");
        self.service.put_encoded(url, value, self.encoder).await
    }

    pub async fn get_distinct_attribute(&self) -> (Value, StatusCode) {
        let url =
            format!("/indexes/{}/settings/{}", urlencode(self.uid.as_ref()), "distinct-attribute");
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
