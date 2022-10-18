#![allow(dead_code)]

use actix_http::body::MessageBody;
use actix_web::dev::ServiceResponse;
use clap::Parser;
use std::path::Path;
use std::sync::Arc;

use actix_web::http::StatusCode;
use byte_unit::{Byte, ByteUnit};
use once_cell::sync::Lazy;
use serde_json::Value;
use tempfile::TempDir;

use meilisearch_http::option::{IndexerOpts, MaxMemory, Opt};
use meilisearch_http::{analytics, create_app, setup_meilisearch};
use crate::common::encoder::Encoder;

use super::index::Index;
use super::service::Service;

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
        let service = Service {
            index_scheduler: Arc::new(index_scheduler),
            auth,
            options,
            api_key: None,
        };

        Server {
            service,
            _dir: Some(dir),
        }
    }

    pub async fn new_auth_with_options(mut options: Opt, dir: TempDir) -> Self {
        if cfg!(windows) {
            std::env::set_var("TMP", TEST_TEMP_DIR.path());
        } else {
            std::env::set_var("TMPDIR", TEST_TEMP_DIR.path());
        }

        options.master_key = Some("MASTER_KEY".to_string());

        let (index_scheduler, auth) = setup_meilisearch(&options).unwrap();
        let service = Service {
            index_scheduler: Arc::new(index_scheduler),
            auth,
            options,
            api_key: None,
        };

        Server {
            service,
            _dir: Some(dir),
        }
    }

    pub async fn new_auth() -> Self {
        let dir = TempDir::new().unwrap();
        let options = default_settings(dir.path());
        Self::new_auth_with_options(options, dir).await
    }

    pub async fn new_with_options(options: Opt) -> Result<Self, anyhow::Error> {
        let (index_scheduler, auth) = setup_meilisearch(&options)?;
        let service = Service {
            index_scheduler: Arc::new(index_scheduler),
            auth,
            options,
            api_key: None,
        };

        Ok(Server {
            service,
            _dir: None,
        })
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
            self.service.auth.clone(),
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

    pub fn index_with_encoder(&self, uid: impl AsRef<str>, encoder: Encoder) -> Index<'_> {
        Index {
            uid: uid.as_ref().to_string(),
            service: &self.service,
            encoder,
        }
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
            self.service
                .get(format!("/indexes?{query_parameter}"))
                .await
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

    pub async fn tasks(&self) -> (Value, StatusCode) {
        self.service.get("/tasks").await
    }

    pub async fn get_dump_status(&self, uid: &str) -> (Value, StatusCode) {
        self.service.get(format!("/dumps/{}/status", uid)).await
    }
}

pub fn default_settings(dir: impl AsRef<Path>) -> Opt {
    Opt {
        db_path: dir.as_ref().join("db"),
        dumps_dir: dir.as_ref().join("dump"),
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
            ..Parser::parse_from(None as Option<&str>)
        },
        #[cfg(feature = "metrics")]
        enable_metrics_route: true,
        ..Parser::parse_from(None as Option<&str>)
    }
}
