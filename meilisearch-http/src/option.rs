use std::fs;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;

use byte_unit::Byte;
use clap::Parser;
use meilisearch_lib::{
    export_to_env_if_not_present,
    options::{IndexerOpts, SchedulerConfig},
};
use rustls::{
    server::{
        AllowAnyAnonymousOrAuthenticatedClient, AllowAnyAuthenticatedClient,
        ServerSessionMemoryCache,
    },
    RootCertStore,
};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::{Deserialize, Serialize};

const POSSIBLE_ENV: [&str; 2] = ["development", "production"];

const MEILI_DB_PATH: &str = "MEILI_DB_PATH";
const MEILI_HTTP_ADDR: &str = "MEILI_HTTP_ADDR";
const MEILI_MASTER_KEY: &str = "MEILI_MASTER_KEY";
const MEILI_ENV: &str = "MEILI_ENV";
#[cfg(all(not(debug_assertions), feature = "analytics"))]
const MEILI_NO_ANALYTICS: &str = "MEILI_NO_ANALYTICS";
const MEILI_MAX_INDEX_SIZE: &str = "MEILI_MAX_INDEX_SIZE";
const MEILI_MAX_TASK_DB_SIZE: &str = "MEILI_MAX_TASK_DB_SIZE";
const MEILI_HTTP_PAYLOAD_SIZE_LIMIT: &str = "MEILI_HTTP_PAYLOAD_SIZE_LIMIT";
const MEILI_SSL_CERT_PATH: &str = "MEILI_SSL_CERT_PATH";
const MEILI_SSL_KEY_PATH: &str = "MEILI_SSL_KEY_PATH";
const MEILI_SSL_AUTH_PATH: &str = "MEILI_SSL_AUTH_PATH";
const MEILI_SSL_OCSP_PATH: &str = "MEILI_SSL_OCSP_PATH";
const MEILI_SSL_REQUIRE_AUTH: &str = "MEILI_SSL_REQUIRE_AUTH";
const MEILI_SSL_RESUMPTION: &str = "MEILI_SSL_RESUMPTION";
const MEILI_SSL_TICKETS: &str = "MEILI_SSL_TICKETS";
const MEILI_SNAPSHOT_DIR: &str = "MEILI_SNAPSHOT_DIR";
const MEILI_SCHEDULE_SNAPSHOT: &str = "MEILI_SCHEDULE_SNAPSHOT";
const MEILI_SNAPSHOT_INTERVAL_SEC: &str = "MEILI_SNAPSHOT_INTERVAL_SEC";
const MEILI_DUMPS_DIR: &str = "MEILI_DUMPS_DIR";
const MEILI_LOG_LEVEL: &str = "MEILI_LOG_LEVEL";
#[cfg(feature = "metrics")]
const MEILI_ENABLE_METRICS_ROUTE: &str = "MEILI_ENABLE_METRICS_ROUTE";

const DEFAULT_DB_PATH: &str = "./data.ms";
const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:7700";
const DEFAULT_ENV: &str = "development";
const DEFAULT_MAX_INDEX_SIZE: &str = "100 GiB";
const DEFAULT_MAX_TASK_DB_SIZE: &str = "100 GiB";
const DEFAULT_HTTP_PAYLOAD_SIZE_LIMIT: &str = "100 MB";
const DEFAULT_SNAPSHOT_DIR: &str = "snapshots/";
const DEFAULT_SNAPSHOT_INTERVAL_SEC: u64 = 86400;
const DEFAULT_DUMPS_DIR: &str = "dumps/";
const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Debug, Clone, Parser, Serialize, Deserialize)]
#[clap(version)]
pub struct Opt {
    /// The destination where the database must be created.
    #[clap(long, env = MEILI_DB_PATH, default_value_os_t = default_db_path())]
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// The address on which the http server will listen.
    #[clap(long, env = MEILI_HTTP_ADDR, default_value_t = default_http_addr())]
    #[serde(default = "default_http_addr")]
    pub http_addr: String,

    /// The master key allowing you to do everything on the server.
    #[serde(skip_serializing)]
    #[clap(long, env = MEILI_MASTER_KEY)]
    pub master_key: Option<String>,

    /// This environment variable must be set to `production` if you are running in production.
    /// If the server is running in development mode more logs will be displayed,
    /// and the master key can be avoided which implies that there is no security on the updates routes.
    /// This is useful to debug when integrating the engine with another service.
    #[clap(long, env = MEILI_ENV, default_value_t = default_env(), possible_values = &POSSIBLE_ENV)]
    #[serde(default = "default_env")]
    pub env: String,

    /// Do not send analytics to Meili.
    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    #[serde(skip_serializing, default)] // we can't send true
    #[clap(long, env = MEILI_NO_ANALYTICS)]
    pub no_analytics: bool,

    /// The maximum size, in bytes, of the main lmdb database directory
    #[clap(long, env = MEILI_MAX_INDEX_SIZE, default_value_t = default_max_index_size())]
    #[serde(default = "default_max_index_size")]
    pub max_index_size: Byte,

    /// The maximum size, in bytes, of the update lmdb database directory
    #[clap(long, env = MEILI_MAX_TASK_DB_SIZE, default_value_t = default_max_task_db_size())]
    #[serde(default = "default_max_task_db_size")]
    pub max_task_db_size: Byte,

    /// The maximum size, in bytes, of accepted JSON payloads
    #[clap(long, env = MEILI_HTTP_PAYLOAD_SIZE_LIMIT, default_value_t = default_http_payload_size_limit())]
    #[serde(default = "default_http_payload_size_limit")]
    pub http_payload_size_limit: Byte,

    /// Read server certificates from CERTFILE.
    /// This should contain PEM-format certificates
    /// in the right order (the first certificate should
    /// certify KEYFILE, the last should be a root CA).
    #[serde(skip_serializing)]
    #[clap(long, env = MEILI_SSL_CERT_PATH, parse(from_os_str))]
    pub ssl_cert_path: Option<PathBuf>,

    /// Read private key from KEYFILE.  This should be a RSA
    /// private key or PKCS8-encoded private key, in PEM format.
    #[serde(skip_serializing)]
    #[clap(long, env = MEILI_SSL_KEY_PATH, parse(from_os_str))]
    pub ssl_key_path: Option<PathBuf>,

    /// Enable client authentication, and accept certificates
    /// signed by those roots provided in CERTFILE.
    #[clap(long, env = MEILI_SSL_AUTH_PATH, parse(from_os_str))]
    #[serde(skip_serializing)]
    pub ssl_auth_path: Option<PathBuf>,

    /// Read DER-encoded OCSP response from OCSPFILE and staple to certificate.
    /// Optional
    #[serde(skip_serializing)]
    #[clap(long, env = MEILI_SSL_OCSP_PATH, parse(from_os_str))]
    pub ssl_ocsp_path: Option<PathBuf>,

    /// Send a fatal alert if the client does not complete client authentication.
    #[serde(skip_serializing, default)]
    #[clap(long, env = MEILI_SSL_REQUIRE_AUTH)]
    pub ssl_require_auth: bool,

    /// SSL support session resumption
    #[serde(skip_serializing, default)]
    #[clap(long, env = MEILI_SSL_RESUMPTION)]
    pub ssl_resumption: bool,

    /// SSL support tickets.
    #[serde(skip_serializing, default)]
    #[clap(long, env = MEILI_SSL_TICKETS)]
    pub ssl_tickets: bool,

    /// Defines the path of the snapshot file to import.
    /// This option will, by default, stop the process if a database already exist or if no snapshot exists at
    /// the given path. If this option is not specified no snapshot is imported.
    #[clap(long, env = "MEILI_IMPORT_SNAPSHOT")]
    pub import_snapshot: Option<PathBuf>,

    /// The engine will ignore a missing snapshot and not return an error in such case.
    #[clap(
        long,
        env = "MEILI_IGNORE_MISSING_SNAPSHOT",
        requires = "import-snapshot"
    )]
    #[serde(default)]
    pub ignore_missing_snapshot: bool,

    /// The engine will skip snapshot importation and not return an error in such case.
    #[clap(
        long,
        env = "MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS",
        requires = "import-snapshot"
    )]
    #[serde(default)]
    pub ignore_snapshot_if_db_exists: bool,

    /// Defines the directory path where meilisearch will create snapshot each snapshot_time_gap.
    #[clap(long, env = MEILI_SNAPSHOT_DIR, default_value_os_t = default_snapshot_dir())]
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: PathBuf,

    /// Activate snapshot scheduling.
    #[clap(long, env = MEILI_SCHEDULE_SNAPSHOT)]
    #[serde(default)]
    pub schedule_snapshot: bool,

    /// Defines time interval, in seconds, between each snapshot creation.
    #[clap(long, env = MEILI_SNAPSHOT_INTERVAL_SEC, default_value_t = default_snapshot_interval_sec())]
    #[serde(default = "default_snapshot_interval_sec")]
    // 24h
    pub snapshot_interval_sec: u64,

    /// Import a dump from the specified path, must be a `.dump` file.
    #[clap(long, env = "MEILI_IMPORT_DUMP", conflicts_with = "import-snapshot")]
    pub import_dump: Option<PathBuf>,

    /// If the dump doesn't exists, load or create the database specified by `db-path` instead.
    #[clap(long, env = "MEILI_IGNORE_MISSING_DUMP", requires = "import-dump")]
    #[serde(default)]
    pub ignore_missing_dump: bool,

    /// Ignore the dump if a database already exists, and load that database instead.
    #[clap(long, env = "MEILI_IGNORE_DUMP_IF_DB_EXISTS", requires = "import-dump")]
    #[serde(default)]
    pub ignore_dump_if_db_exists: bool,

    /// Folder where dumps are created when the dump route is called.
    #[clap(long, env = MEILI_DUMPS_DIR, default_value_os_t = default_dumps_dir())]
    #[serde(default = "default_dumps_dir")]
    pub dumps_dir: PathBuf,

    /// Set the log level
    #[clap(long, env = MEILI_LOG_LEVEL, default_value_t = default_log_level())]
    #[serde(default = "default_log_level")]
    pub log_level: String,

    /// Enables Prometheus metrics and /metrics route.
    #[cfg(feature = "metrics")]
    #[clap(long, env = MEILI_ENABLE_METRICS_ROUTE)]
    #[serde(default)]
    pub enable_metrics_route: bool,

    #[serde(flatten)]
    #[clap(flatten)]
    pub indexer_options: IndexerOpts,

    #[serde(flatten)]
    #[clap(flatten)]
    pub scheduler_options: SchedulerConfig,

    /// The path to a configuration file that should be used to setup the engine.
    /// Format must be TOML.
    #[serde(skip_serializing)]
    #[clap(long)]
    config_file_path: Option<PathBuf>,
}

impl Opt {
    /// Wether analytics should be enabled or not.
    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    pub fn analytics(&self) -> bool {
        !self.no_analytics
    }

    pub fn build() -> Self {
        let mut opts = Opt::parse();
        if let Some(config_file_path) = opts.config_file_path.as_ref() {
            eprintln!("loading config file : {:?}", config_file_path);
            match std::fs::read(config_file_path) {
                Ok(config) => {
                    let opt_from_config =
                        toml::from_slice::<Opt>(&config).expect("can't read file");
                    opt_from_config.export_to_env();
                    opts = Opt::parse();
                }
                Err(err) => eprintln!("can't read {:?} : {}", config_file_path, err),
            }
        }

        opts
    }

    fn export_to_env(self) {
        export_to_env_if_not_present(MEILI_DB_PATH, self.db_path);
        export_to_env_if_not_present(MEILI_HTTP_ADDR, self.http_addr);
        if let Some(master_key) = self.master_key {
            export_to_env_if_not_present(MEILI_MASTER_KEY, master_key);
        }
        export_to_env_if_not_present(MEILI_ENV, self.env);
        #[cfg(all(not(debug_assertions), feature = "analytics"))]
        {
            export_to_env_if_not_present(MEILI_NO_ANALYTICS, self.no_analytics.to_string());
        }
        export_to_env_if_not_present(MEILI_MAX_INDEX_SIZE, self.max_index_size.to_string());
        export_to_env_if_not_present(MEILI_MAX_TASK_DB_SIZE, self.max_task_db_size.to_string());
        export_to_env_if_not_present(
            MEILI_HTTP_PAYLOAD_SIZE_LIMIT,
            self.http_payload_size_limit.to_string(),
        );
        if let Some(ssl_cert_path) = self.ssl_cert_path {
            export_to_env_if_not_present(MEILI_SSL_CERT_PATH, ssl_cert_path);
        }
        if let Some(ssl_key_path) = self.ssl_key_path {
            export_to_env_if_not_present(MEILI_SSL_KEY_PATH, ssl_key_path);
        }
        if let Some(ssl_auth_path) = self.ssl_auth_path {
            export_to_env_if_not_present(MEILI_SSL_AUTH_PATH, ssl_auth_path);
        }
        if let Some(ssl_ocsp_path) = self.ssl_ocsp_path {
            export_to_env_if_not_present(MEILI_SSL_OCSP_PATH, ssl_ocsp_path);
        }
        export_to_env_if_not_present(MEILI_SSL_REQUIRE_AUTH, self.ssl_require_auth.to_string());
        export_to_env_if_not_present(MEILI_SSL_RESUMPTION, self.ssl_resumption.to_string());
        export_to_env_if_not_present(MEILI_SSL_TICKETS, self.ssl_tickets.to_string());
        export_to_env_if_not_present(MEILI_SNAPSHOT_DIR, self.snapshot_dir);
        export_to_env_if_not_present(MEILI_SCHEDULE_SNAPSHOT, self.schedule_snapshot.to_string());
        export_to_env_if_not_present(
            MEILI_SNAPSHOT_INTERVAL_SEC,
            self.snapshot_interval_sec.to_string(),
        );
        export_to_env_if_not_present(MEILI_DUMPS_DIR, self.dumps_dir);
        export_to_env_if_not_present(MEILI_LOG_LEVEL, self.log_level);
        #[cfg(feature = "metrics")]
        {
            export_to_env_if_not_present(
                MEILI_ENABLE_METRICS_ROUTE,
                self.enable_metrics_route.to_string(),
            );
        }
        self.indexer_options.export_to_env();
        self.scheduler_options.export_to_env();
    }

    pub fn get_ssl_config(&self) -> anyhow::Result<Option<rustls::ServerConfig>> {
        if let (Some(cert_path), Some(key_path)) = (&self.ssl_cert_path, &self.ssl_key_path) {
            let config = rustls::ServerConfig::builder().with_safe_defaults();

            let config = match &self.ssl_auth_path {
                Some(auth_path) => {
                    let roots = load_certs(auth_path.to_path_buf())?;
                    let mut client_auth_roots = RootCertStore::empty();
                    for root in roots {
                        client_auth_roots.add(&root).unwrap();
                    }
                    if self.ssl_require_auth {
                        let verifier = AllowAnyAuthenticatedClient::new(client_auth_roots);
                        config.with_client_cert_verifier(verifier)
                    } else {
                        let verifier =
                            AllowAnyAnonymousOrAuthenticatedClient::new(client_auth_roots);
                        config.with_client_cert_verifier(verifier)
                    }
                }
                None => config.with_no_client_auth(),
            };

            let certs = load_certs(cert_path.to_path_buf())?;
            let privkey = load_private_key(key_path.to_path_buf())?;
            let ocsp = load_ocsp(&self.ssl_ocsp_path)?;
            let mut config = config
                .with_single_cert_with_ocsp_and_sct(certs, privkey, ocsp, vec![])
                .map_err(|_| anyhow::anyhow!("bad certificates/private key"))?;

            config.key_log = Arc::new(rustls::KeyLogFile::new());

            if self.ssl_resumption {
                config.session_storage = ServerSessionMemoryCache::new(256);
            }

            if self.ssl_tickets {
                config.ticketer = rustls::Ticketer::new().unwrap();
            }

            Ok(Some(config))
        } else {
            Ok(None)
        }
    }
}

fn load_certs(filename: PathBuf) -> anyhow::Result<Vec<rustls::Certificate>> {
    let certfile =
        fs::File::open(filename).map_err(|_| anyhow::anyhow!("cannot open certificate file"))?;
    let mut reader = BufReader::new(certfile);
    certs(&mut reader)
        .map(|certs| certs.into_iter().map(rustls::Certificate).collect())
        .map_err(|_| anyhow::anyhow!("cannot read certificate file"))
}

fn load_private_key(filename: PathBuf) -> anyhow::Result<rustls::PrivateKey> {
    let rsa_keys = {
        let keyfile = fs::File::open(filename.clone())
            .map_err(|_| anyhow::anyhow!("cannot open private key file"))?;
        let mut reader = BufReader::new(keyfile);
        rsa_private_keys(&mut reader)
            .map_err(|_| anyhow::anyhow!("file contains invalid rsa private key"))?
    };

    let pkcs8_keys = {
        let keyfile = fs::File::open(filename)
            .map_err(|_| anyhow::anyhow!("cannot open private key file"))?;
        let mut reader = BufReader::new(keyfile);
        pkcs8_private_keys(&mut reader).map_err(|_| {
            anyhow::anyhow!(
                "file contains invalid pkcs8 private key (encrypted keys not supported)"
            )
        })?
    };

    // prefer to load pkcs8 keys
    if !pkcs8_keys.is_empty() {
        Ok(rustls::PrivateKey(pkcs8_keys[0].clone()))
    } else {
        assert!(!rsa_keys.is_empty());
        Ok(rustls::PrivateKey(rsa_keys[0].clone()))
    }
}

fn load_ocsp(filename: &Option<PathBuf>) -> anyhow::Result<Vec<u8>> {
    let mut ret = Vec::new();

    if let Some(ref name) = filename {
        fs::File::open(name)
            .map_err(|_| anyhow::anyhow!("cannot open ocsp file"))?
            .read_to_end(&mut ret)
            .map_err(|_| anyhow::anyhow!("cannot read oscp file"))?;
    }

    Ok(ret)
}

fn default_db_path() -> PathBuf {
    PathBuf::from(DEFAULT_DB_PATH)
}

fn default_http_addr() -> String {
    DEFAULT_HTTP_ADDR.to_string()
}

fn default_env() -> String {
    DEFAULT_ENV.to_string()
}

fn default_max_index_size() -> Byte {
    Byte::from_str(DEFAULT_MAX_INDEX_SIZE).unwrap()
}

fn default_max_task_db_size() -> Byte {
    Byte::from_str(DEFAULT_MAX_TASK_DB_SIZE).unwrap()
}

fn default_http_payload_size_limit() -> Byte {
    Byte::from_str(DEFAULT_HTTP_PAYLOAD_SIZE_LIMIT).unwrap()
}

fn default_snapshot_dir() -> PathBuf {
    PathBuf::from(DEFAULT_SNAPSHOT_DIR)
}

fn default_snapshot_interval_sec() -> u64 {
    DEFAULT_SNAPSHOT_INTERVAL_SEC
}

fn default_dumps_dir() -> PathBuf {
    PathBuf::from(DEFAULT_DUMPS_DIR)
}

fn default_log_level() -> String {
    DEFAULT_LOG_LEVEL.to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_opt() {
        assert!(Opt::try_parse_from(Some("")).is_ok());
    }
}
