use std::fmt::Write;
use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, UnwindSafe};

use actix_web::http::StatusCode;
use serde::Serialize;
use urlencoding::encode as urlencode;

use super::encoder::Encoder;
use super::service::Service;
use super::{Owned, Server, Shared, Value};
use crate::json;

pub struct Index<'a, State = Owned> {
    pub uid: String,
    pub service: &'a Service,
    pub(super) encoder: Encoder,
    pub(super) marker: PhantomData<State>,
}

impl<'a> Index<'a, Owned> {
    pub fn to_shared(&self) -> Index<'a, Shared> {
        Index {
            uid: self.uid.clone(),
            service: self.service,
            encoder: self.encoder,
            marker: PhantomData,
        }
    }

    pub fn with_encoder(&self, encoder: Encoder) -> Index<'a, Owned> {
        Index { uid: self.uid.clone(), service: self.service, encoder, marker: PhantomData }
    }

    pub async fn load_test_set<State>(&self, waiter: &Server<State>) -> u64 {
        let url = format!("/indexes/{}/documents", urlencode(self.uid.as_ref()));
        let (response, code) = self
            .service
            .post_str(
                url,
                include_str!("../assets/test_set.json"),
                vec![("content-type", "application/json")],
            )
            .await;
        assert_eq!(code, 202);
        let update_id = response["taskUid"].as_u64().unwrap();
        waiter.wait_task(update_id).await;
        update_id
    }

    pub async fn load_test_set_ndjson<State>(&self, waiter: &Server<State>) -> u64 {
        let url = format!("/indexes/{}/documents", urlencode(self.uid.as_ref()));
        let (response, code) = self
            .service
            .post_str(
                url,
                include_str!("../assets/test_set.ndjson"),
                vec![("content-type", "application/x-ndjson")],
            )
            .await;
        assert_eq!(code, 202);
        let update_id = response["taskUid"].as_u64().unwrap();
        waiter.wait_task(update_id).await;
        update_id
    }

    pub async fn create(&self, primary_key: Option<&str>) -> (Value, StatusCode) {
        self._create(primary_key).await
    }

    pub async fn update_raw(&self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, body, self.encoder).await
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
        self._add_documents(documents, primary_key, None, false).await
    }

    pub async fn add_documents_with_custom_metadata(
        &self,
        documents: Value,
        primary_key: Option<&str>,
        custom_metadata: Option<&str>,
    ) -> (Value, StatusCode) {
        self._add_documents(documents, primary_key, custom_metadata, false).await
    }

    pub async fn add_documents_with_skip_creation(
        &self,
        documents: Value,
        primary_key: Option<&str>,
        custom_metadata: Option<&str>,
        skip_creation: bool,
    ) -> (Value, StatusCode) {
        self._add_documents(documents, primary_key, custom_metadata, skip_creation).await
    }

    pub async fn raw_add_documents(
        &self,
        payload: &str,
        headers: Vec<(&str, &str)>,
        query_parameter: &str,
    ) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents{}", urlencode(self.uid.as_ref()), query_parameter);
        self.service.post_str(url, payload, headers).await
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

    pub async fn raw_update_documents(
        &self,
        payload: &str,
        content_type: Option<&str>,
        query_parameter: &str,
    ) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents{}", urlencode(self.uid.as_ref()), query_parameter);

        if let Some(content_type) = content_type {
            self.service.put_str(url, payload, vec![("Content-Type", content_type)]).await
        } else {
            self.service.put_str(url, payload, Vec::new()).await
        }
    }

    pub async fn list_tasks(&self) -> (Value, StatusCode) {
        let url = format!("/tasks?indexUids={}", self.uid);
        self.service.get(url).await
    }

    pub async fn list_batches(&self) -> (Value, StatusCode) {
        let url = format!("/batches?indexUids={}", self.uid);
        self.service.get(url).await
    }

    pub async fn delete_document(&self, id: u64) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/{}", urlencode(self.uid.as_ref()), id);
        self.service.delete(url).await
    }

    pub async fn delete_document_by_filter(&self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/delete", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, body, self.encoder).await
    }

    pub async fn clear_all_documents(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents", urlencode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn delete_batch(&self, ids: Vec<u64>) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/delete-batch", urlencode(self.uid.as_ref()));
        self.service
            .post_encoded(url, serde_json::to_value(&ids).unwrap().into(), self.encoder)
            .await
    }

    pub async fn delete_batch_raw(&self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/delete-batch", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, body, self.encoder).await
    }

    pub async fn update_settings(&self, settings: Value) -> (Value, StatusCode) {
        self._update_settings(settings).await
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

    pub async fn update_settings_search_cutoff_ms(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/search-cutoff-ms", urlencode(self.uid.as_ref()));
        self.service.put_encoded(url, settings, self.encoder).await
    }

    pub async fn update_settings_chat(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings/chat", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, settings, self.encoder).await
    }

    pub async fn delete_settings(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", urlencode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn update_distinct_attribute(&self, value: Value) -> (Value, StatusCode) {
        let url =
            format!("/indexes/{}/settings/{}", urlencode(self.uid.as_ref()), "distinct-attribute");
        self.service.put_encoded(url, value, self.encoder).await
    }
}

impl Index<'_, Shared> {
    /// You cannot modify the content of a shared index, thus the delete_document_by_filter call
    /// must fail. If the task successfully enqueue itself, we'll wait for the task to finishes,
    /// and if it succeed the function will panic.
    pub async fn delete_document_by_filter_fail<State>(
        &self,
        body: Value,
        waiter: &Server<State>,
    ) -> (Value, StatusCode) {
        let (mut task, code) = self._delete_document_by_filter(body).await;
        if code.is_success() {
            task = waiter.wait_task(task.uid()).await;
            if task.is_success() {
                panic!(
                    "`delete_document_by_filter_fail` succeeded: {}",
                    serde_json::to_string_pretty(&task).unwrap()
                );
            }
        }
        (task, code)
    }

    pub async fn delete_index_fail<State>(&self, waiter: &Server<State>) -> (Value, StatusCode) {
        let (mut task, code) = self._delete().await;
        if code.is_success() {
            task = waiter.wait_task(task.uid()).await;
            if task.is_success() {
                panic!(
                    "`delete_index_fail` succeeded: {}",
                    serde_json::to_string_pretty(&task).unwrap()
                );
            }
        }
        (task, code)
    }

    pub async fn update_index_fail<State>(
        &self,
        primary_key: Option<&str>,
        waiter: &Server<State>,
    ) -> (Value, StatusCode) {
        let (mut task, code) = self._update(primary_key).await;
        if code.is_success() {
            task = waiter.wait_task(task.uid()).await;
            if task.is_success() {
                panic!(
                    "`update_index_fail` succeeded: {}",
                    serde_json::to_string_pretty(&task).unwrap()
                );
            }
        }
        (task, code)
    }

    pub async fn update_raw_index_fail<State>(
        &self,
        body: Value,
        waiter: &Server<State>,
    ) -> (Value, StatusCode) {
        let (mut task, code) = self._update_raw(body).await;
        if code.is_success() {
            task = waiter.wait_task(task.uid()).await;
            if task.is_success() {
                panic!(
                    "`update_raw_index_fail` succeeded: {}",
                    serde_json::to_string_pretty(&task).unwrap()
                );
            }
        }
        (task, code)
    }
}

#[allow(dead_code)]
impl<State> Index<'_, State> {
    pub async fn get(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));
        self.service.get(url).await
    }

    /// add_documents is not allowed on shared index but we need to use it to initialize
    /// a bunch of very common indexes in `common/mod.rs`.
    pub(super) async fn _add_documents(
        &self,
        documents: Value,
        primary_key: Option<&str>,
        custom_metadata: Option<&str>,
        skip_creation: bool,
    ) -> (Value, StatusCode) {
        let url = match (primary_key, custom_metadata) {
            (Some(key), Some(meta)) => {
                format!(
                    "/indexes/{}/documents?primaryKey={key}&customMetadata={meta}&skipCreation={skip_creation}",
                    urlencode(self.uid.as_ref()),
                )
            }
            (None, Some(meta)) => {
                format!(
                    "/indexes/{}/documents?&customMetadata={meta}&skipCreation={skip_creation}",
                    urlencode(self.uid.as_ref()),
                )
            }
            (Some(key), None) => {
                format!(
                    "/indexes/{}/documents?&primaryKey={key}&skipCreation={skip_creation}",
                    urlencode(self.uid.as_ref()),
                )
            }
            (None, None) => format!(
                "/indexes/{}/documents?skipCreation={skip_creation}",
                urlencode(self.uid.as_ref())
            ),
        };
        self.service.post_encoded(url, documents, self.encoder).await
    }

    pub(super) async fn _update_settings(&self, settings: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, settings, self.encoder).await
    }

    pub(super) async fn _delete_document_by_filter(&self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/delete", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, body, self.encoder).await
    }

    pub(super) async fn _create(&self, primary_key: Option<&str>) -> (Value, StatusCode) {
        let body = json!({
            "uid": self.uid,
            "primaryKey": primary_key,
        });
        self.service.post_encoded("/indexes", body, self.encoder).await
    }

    pub(super) async fn _update(&self, primary_key: Option<&str>) -> (Value, StatusCode) {
        let body = json!({
            "primaryKey": primary_key,
        });
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, body, self.encoder).await
    }

    pub(super) async fn _update_raw(&self, body: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));
        self.service.patch_encoded(url, body, self.encoder).await
    }

    pub(super) async fn _delete(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencode(self.uid.as_ref()));
        self.service.delete(url).await
    }

    pub async fn get_task(&self, update_id: u64) -> (Value, StatusCode) {
        let url = format!("/tasks/{}", update_id);
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

    pub async fn fields(&self, params: &ListFieldsPayload<'_>) -> (Value, StatusCode) {
        self.service
            .post(
                format!("/indexes/{}/fields", urlencode(self.uid.as_str())),
                serde_json::to_value(params).unwrap().into(),
            )
            .await
    }

    pub async fn get_batch(&self, batch_id: u32) -> (Value, StatusCode) {
        let url = format!("/batches/{}", batch_id);
        self.service.get(url).await
    }

    pub async fn filtered_batches(
        &self,
        types: &[&str],
        statuses: &[&str],
        canceled_by: &[&str],
    ) -> (Value, StatusCode) {
        let mut url = format!("/batches?indexUids={}", self.uid);
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

    pub async fn get_document(&self, id: u64, options: Option<Value>) -> (Value, StatusCode) {
        let mut url = format!("/indexes/{}/documents/{}", urlencode(self.uid.as_ref()), id);
        if let Some(options) = options {
            write!(url, "{}", yaup::to_string(&options).unwrap()).unwrap();
        }
        self.service.get(url).await
    }

    pub async fn fetch_documents(&self, payload: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents/fetch", urlencode(self.uid.as_ref()));
        self.service.post(url, payload).await
    }

    pub async fn get_all_documents_raw(&self, options: &str) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/documents{}", urlencode(self.uid.as_ref()), options);
        self.service.get(url).await
    }

    pub async fn get_all_documents(&self, options: GetAllDocumentsOptions) -> (Value, StatusCode) {
        let url = format!(
            "/indexes/{}/documents{}",
            urlencode(self.uid.as_ref()),
            yaup::to_string(&options).unwrap()
        );

        self.service.get(url).await
    }

    pub async fn settings(&self) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/settings", urlencode(self.uid.as_ref()));
        self.service.get(url).await
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
        let post = self.search_post(query.clone()).await;

        let query = yaup::to_string(&query).unwrap();
        let get = self.search_get(&query).await;

        insta::allow_duplicates! {
            let (response, code) = post;
            let t = test.clone();
            if let Err(e) = catch_unwind(move || t(response, code)) {
                eprintln!("Error with post search");
                resume_unwind(e);
            }

            let (response, code) = get;
            if let Err(e) = catch_unwind(move || test(response, code)) {
                eprintln!("Error with get search");
                resume_unwind(e);
            }
        }
    }

    pub async fn search_post(&self, query: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/search", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, query, self.encoder).await
    }

    pub async fn search_with_headers(
        &self,
        query: Value,
        headers: Vec<(&str, &str)>,
    ) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/search", urlencode(self.uid.as_ref()));
        let body = serde_json::to_string(&query).unwrap();
        let mut all_headers = vec![("content-type", "application/json")];
        all_headers.extend(headers);
        self.service.post_str(url, body, all_headers).await
    }

    pub async fn search_get(&self, query: &str) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/search{}", urlencode(self.uid.as_ref()), query);
        self.service.get(url).await
    }

    /// Performs both GET and POST similar queries
    pub async fn similar(
        &self,
        query: Value,
        test: impl Fn(Value, StatusCode) + UnwindSafe + Clone,
    ) {
        let post = self.similar_post(query.clone()).await;

        let query = yaup::to_string(&query).unwrap();
        let get = self.similar_get(&query).await;

        insta::allow_duplicates! {
            let (response, code) = post;
            let t = test.clone();
            if let Err(e) = catch_unwind(move || t(response, code)) {
                eprintln!("Error with post search");
                resume_unwind(e);
            }

            let (response, code) = get;
            if let Err(e) = catch_unwind(move || test(response, code)) {
                eprintln!("Error with get search");
                resume_unwind(e);
            }
        }
    }

    pub async fn similar_post(&self, query: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/similar", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, query, self.encoder).await
    }

    pub async fn similar_get(&self, query: &str) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/similar{}", urlencode(self.uid.as_ref()), query);
        self.service.get(url).await
    }

    pub async fn facet_search(&self, query: Value) -> (Value, StatusCode) {
        let url = format!("/indexes/{}/facet-search", urlencode(self.uid.as_ref()));
        self.service.post_encoded(url, query, self.encoder).await
    }

    pub async fn get_distinct_attribute(&self) -> (Value, StatusCode) {
        let url =
            format!("/indexes/{}/settings/{}", urlencode(self.uid.as_ref()), "distinct-attribute");
        self.service.get(url).await
    }
}

#[derive(Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAllDocumentsOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<&'static str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<&'static str>>,
    pub retrieve_vectors: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFieldsPayload<'a> {
    pub offset: usize,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<ListFieldsFilterPayload<'a>>,
}

impl Default for ListFieldsPayload<'_> {
    fn default() -> Self {
        Self { offset: 0, limit: 20, filter: None }
    }
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListFieldsFilterPayload<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub starts_with: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regex: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub glob: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub displayed: Option<bool>,
}
