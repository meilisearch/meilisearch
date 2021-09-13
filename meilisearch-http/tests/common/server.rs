use std::path::Path;

use actix_web::http::StatusCode;
use byte_unit::{Byte, ByteUnit};
use serde_json::Value;
use tempdir::TempDir;
use urlencoding::encode;

use meilisearch_http::data::Data;
use meilisearch_http::option::{IndexerOpts, MaxMemory, Opt};

use super::index::Index;
use super::service::Service;

pub struct Server {
    pub service: Service,
    // hold ownership to the tempdir while we use the server instance.
    _dir: Option<tempdir::TempDir>,
}

impl Server {
    pub async fn new() -> Self {
        let dir = TempDir::new("meilisearch").unwrap();

        let opt = default_settings(dir.path());

        let data = Data::new(opt).unwrap();
        let service = Service(data);

        Server {
            service,
            _dir: Some(dir),
        }
    }

    pub async fn new_with_options(opt: Opt) -> Self {
        let data = Data::new(opt).unwrap();
        let service = Service(data);

        Server {
            service,
            _dir: None,
        }
    }

    /// Returns a view to an index. There is no guarantee that the index exists.
    pub fn index(&self, uid: impl AsRef<str>) -> Index<'_> {
        Index {
            uid: encode(uid.as_ref()).to_string(),
            service: &self.service,
        }
    }

    pub async fn list_indexes(&self) -> (Value, StatusCode) {
        self.service.get("/indexes").await
    }

    pub async fn version(&self) -> (Value, StatusCode) {
        self.service.get("/version").await
    }

    pub async fn stats(&self) -> (Value, StatusCode) {
        self.service.get("/stats").await
    }
}

pub fn default_settings(dir: impl AsRef<Path>) -> Opt {
    Opt {
        db_path: dir.as_ref().join("db"),
        dumps_dir: dir.as_ref().join("dump"),
        http_addr: "127.0.0.1:7700".to_owned(),
        master_key: None,
        env: "development".to_owned(),
        #[cfg(all(not(debug_assertions), feature = "analytics"))]
        no_analytics: true,
        max_index_size: Byte::from_unit(4.0, ByteUnit::GiB).unwrap(),
        max_udb_size: Byte::from_unit(4.0, ByteUnit::GiB).unwrap(),
        http_payload_size_limit: Byte::from_unit(10.0, ByteUnit::MiB).unwrap(),
        ssl_cert_path: None,
        ssl_key_path: None,
        ssl_auth_path: None,
        ssl_ocsp_path: None,
        ssl_require_auth: false,
        ssl_resumption: false,
        ssl_tickets: false,
        import_snapshot: None,
        ignore_missing_snapshot: false,
        ignore_snapshot_if_db_exists: false,
        snapshot_dir: ".".into(),
        schedule_snapshot: false,
        snapshot_interval_sec: 0,
        import_dump: None,
        indexer_options: IndexerOpts {
            // memory has to be unlimited because several meilisearch are running in test context.
            max_memory: MaxMemory::unlimited(),
            ..Default::default()
        },
        log_level: "off".into(),
    }
}
