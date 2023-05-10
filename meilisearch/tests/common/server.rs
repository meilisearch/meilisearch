#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use actix_http::body::MessageBody;
use actix_web::dev::ServiceResponse;
use actix_web::http::StatusCode;
use byte_unit::{Byte, ByteUnit};
use clap::Parser;
use meilisearch::option::{IndexerOpts, MaxMemory, Opt};
use meilisearch::{analytics, create_app, setup_meilisearch};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::time::sleep;

use super::index::Index;
use super::service::Service;
use super::task::Task;
use crate::common::encoder::Encoder;

pub struct Server {
    pub service: Service,
    // hold ownership to the tempdir while we use the server instance.
    _dir: Option<TempDir>,
}

pub static TEST_TEMP_DIR: Lazy<TempDir> = Lazy::new(|| TempDir::new().unwrap());

impl Server {
    pub async fn new() -> Self {
        let dir = TempDir::new().unwrap();

        if cfg!(windows) {
            std::env::set_var("TMP", TEST_TEMP_DIR.path());
        } else {
            std::env::set_var("TMPDIR", TEST_TEMP_DIR.path());
        }

        let options = default_settings(dir.path());

        let (index_scheduler, auth) = setup_meilisearch(&options).unwrap();
        let service = Service { index_scheduler, auth, options, api_key: None };

        Server { service, _dir: Some(dir) }
    }

    pub async fn new_auth_with_options(mut options: Opt, dir: TempDir) -> Self {
        if cfg!(windows) {
            std::env::set_var("TMP", TEST_TEMP_DIR.path());
        } else {
            std::env::set_var("TMPDIR", TEST_TEMP_DIR.path());
        }

        options.master_key = Some("MASTER_KEY".to_string());

        let (index_scheduler, auth) = setup_meilisearch(&options).unwrap();
        let service = Service { index_scheduler, auth, options, api_key: None };

        Server { service, _dir: Some(dir) }
    }

    pub async fn new_auth() -> Self {
        let dir = TempDir::new().unwrap();
        let options = default_settings(dir.path());
        Self::new_auth_with_options(options, dir).await
    }

    pub async fn new_with_options(options: Opt) -> Result<Self, anyhow::Error> {
        let (index_scheduler, auth) = setup_meilisearch(&options)?;
        let service = Service { index_scheduler, auth, options, api_key: None };

        Ok(Server { service, _dir: None })
    }

    pub async fn init_web_app(
        &self,
    ) -> impl actix_web::dev::Service<
        actix_http::Request,
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
    > {
        actix_web::test::init_service(create_app(
            self.service.index_scheduler.clone().into(),
            self.service.auth.clone().into(),
            self.service.options.clone(),
            analytics::MockAnalytics::new(&self.service.options),
            true,
        ))
        .await
    }

    /// Returns a view to an index. There is no guarantee that the index exists.
    pub fn index(&self, uid: impl AsRef<str>) -> Index<'_> {
        self.index_with_encoder(uid, Encoder::Plain)
    }

    pub async fn create_index(&self, body: Value) -> Task<'_> {
        let (value, code) = self.service.post("/indexes", body).await;
        Task::new(&self.service, code, value)
    }

    pub fn index_with_encoder(&self, uid: impl AsRef<str>, encoder: Encoder) -> Index<'_> {
        Index { uid: uid.as_ref().to_string(), service: &self.service, encoder }
    }

    pub async fn multi_search(&self, queries: Value) -> (Value, StatusCode) {
        self.service.post("/multi-search", queries).await
    }

    pub async fn list_indexes_raw(&self, parameters: &str) -> Task<'_> {
        let (value, code) = self.service.get(format!("/indexes{parameters}")).await;
        Task::new(&self.service, code, value)
    }

    pub async fn list_indexes(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> (Value, StatusCode) {
        let (offset, limit) = (
            offset.map(|offset| format!("offset={offset}")),
            limit.map(|limit| format!("limit={limit}")),
        );
        let query_parameter = offset
            .as_ref()
            .zip(limit.as_ref())
            .map(|(offset, limit)| format!("{offset}&{limit}"))
            .or_else(|| offset.xor(limit));
        if let Some(query_parameter) = query_parameter {
            self.service.get(format!("/indexes?{query_parameter}")).await
        } else {
            self.service.get("/indexes").await
        }
    }

    pub async fn version(&self) -> (Value, StatusCode) {
        self.service.get("/version").await
    }

    pub async fn stats(&self) -> (Value, StatusCode) {
        self.service.get("/stats").await
    }

    pub async fn tasks(&self) -> Task<'_> {
        let (value, code) = self.service.get("/tasks").await;
        Task::new(&self.service, code, value)
    }

    pub async fn tasks_filter(&self, filter: &str) -> Task<'_> {
        let (value, code) = self.service.get(format!("/tasks?{}", filter)).await;
        Task::new(&self.service, code, value)
    }

    pub async fn get_dump_status(&self, uid: &str) -> (Value, StatusCode) {
        self.service.get(format!("/dumps/{}/status", uid)).await
    }

    pub async fn create_dump(&self) -> Task<'_> {
        let (value, code) = self.service.post("/dumps", json!(null)).await;
        Task::new(&self.service, code, value)
    }

    pub async fn index_swap(&self, value: Value) -> Task<'_> {
        let (value, code) = self.service.post("/swap-indexes", value).await;
        Task::new(&self.service, code, value)
    }

    pub async fn cancel_tasks(&self, value: &str) -> Task<'_> {
        let (value, code) =
            self.service.post(format!("/tasks/cancel?{}", value), json!(null)).await;
        Task::new(&self.service, code, value)
    }

    pub async fn delete_tasks(&self, value: &str) -> Task<'_> {
        let (value, code) = self.service.delete(format!("/tasks?{}", value)).await;
        Task::new(&self.service, code, value)
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

    pub async fn get_task(&self, update_id: u64) -> Task<'_> {
        let url = format!("/tasks/{}", update_id);
        let (value, code) = self.service.get(url).await;
        Task::new(&self.service, code, value)
    }
}

pub fn default_settings(dir: impl AsRef<Path>) -> Opt {
    Opt {
        db_path: dir.as_ref().join("db"),
        dump_dir: dir.as_ref().join("dumps"),
        env: "development".to_owned(),
        #[cfg(all(not(debug_assertions), feature = "analytics"))]
        no_analytics: true,
        max_index_size: Byte::from_unit(100.0, ByteUnit::MiB).unwrap(),
        max_task_db_size: Byte::from_unit(1.0, ByteUnit::GiB).unwrap(),
        http_payload_size_limit: Byte::from_unit(10.0, ByteUnit::MiB).unwrap(),
        snapshot_dir: ".".into(),
        indexer_options: IndexerOpts {
            // memory has to be unlimited because several meilisearch are running in test context.
            max_indexing_memory: MaxMemory::unlimited(),
            skip_index_budget: true,
            ..Parser::parse_from(None as Option<&str>)
        },
        experimental_enable_metrics: true,
        ..Parser::parse_from(None as Option<&str>)
    }
}
