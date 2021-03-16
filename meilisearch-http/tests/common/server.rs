use actix_web::http::StatusCode;
use byte_unit::{Byte, ByteUnit};
use serde_json::Value;
use tempdir::TempDir;
use urlencoding::encode;

use meilisearch_http::data::Data;
use meilisearch_http::option::{IndexerOpts, Opt};

use super::index::Index;
use super::service::Service;

pub struct Server {
    pub service: Service,
    // hod ownership to the tempdir while we use the server instance.
    _dir: tempdir::TempDir,
}

impl Server {
    pub async fn new() -> Self {
        let dir = TempDir::new("meilisearch").unwrap();

        let opt = Opt {
            db_path: dir.path().join("db"),
            dumps_dir: dir.path().join("dump"),
            dump_batch_size: 16,
            http_addr: "127.0.0.1:7700".to_owned(),
            master_key: None,
            env: "development".to_owned(),
            no_analytics: true,
            max_mdb_size: Byte::from_unit(4.0, ByteUnit::GiB).unwrap(),
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
            snapshot_interval_sec: None,
            import_dump: None,
            indexer_options: IndexerOpts::default(),
            #[cfg(all(not(debug_assertions), feature = "sentry"))]
            sentry_dsn: String::from(""),
            #[cfg(all(not(debug_assertions), feature = "sentry"))]
            no_sentry: true,
        };

        let data = Data::new(opt).unwrap();
        let service = Service(data);

        Server { service, _dir: dir }
    }

    /// Returns a view to an index. There is no guarantee that the index exists.
    pub fn index<'a>(&'a self, uid: impl AsRef<str>) -> Index<'a> {
        Index {
            uid: encode(uid.as_ref()),
            service: &self.service,
        }
    }

    pub async fn list_indexes(&self) -> (Value, StatusCode) {
        self.service.get("/indexes").await
    }

    pub async fn version(&self) -> (Value, StatusCode) {
        self.service.get("/version").await
    }
}
