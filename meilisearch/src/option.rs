use std::convert::TryFrom;
use std::env::VarError;
use std::ffi::OsStr;
use std::io::{BufReader, Read};
use std::num::ParseIntError;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::{env, fmt, fs};

use byte_unit::{Byte, ByteError};
use clap::Parser;
use meilisearch_types::milli::update::IndexerConfig;
use rustls::server::{
    AllowAnyAnonymousOrAuthenticatedClient, AllowAnyAuthenticatedClient, ServerSessionMemoryCache,
};
use rustls::RootCertStore;
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::{Deserialize, Serialize};
use sysinfo::{RefreshKind, System, SystemExt};

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
const MEILI_IMPORT_SNAPSHOT: &str = "MEILI_IMPORT_SNAPSHOT";
const MEILI_IGNORE_MISSING_SNAPSHOT: &str = "MEILI_IGNORE_MISSING_SNAPSHOT";
const MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS: &str = "MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS";
const MEILI_SNAPSHOT_DIR: &str = "MEILI_SNAPSHOT_DIR";
const MEILI_SCHEDULE_SNAPSHOT: &str = "MEILI_SCHEDULE_SNAPSHOT";
const MEILI_SNAPSHOT_INTERVAL_SEC: &str = "MEILI_SNAPSHOT_INTERVAL_SEC";
const MEILI_IMPORT_DUMP: &str = "MEILI_IMPORT_DUMP";
const MEILI_IGNORE_MISSING_DUMP: &str = "MEILI_IGNORE_MISSING_DUMP";
const MEILI_IGNORE_DUMP_IF_DB_EXISTS: &str = "MEILI_IGNORE_DUMP_IF_DB_EXISTS";
const MEILI_DUMP_DIR: &str = "MEILI_DUMP_DIR";
const MEILI_LOG_LEVEL: &str = "MEILI_LOG_LEVEL";
#[cfg(feature = "metrics")]
const MEILI_ENABLE_METRICS_ROUTE: &str = "MEILI_ENABLE_METRICS_ROUTE";

const DEFAULT_CONFIG_FILE_PATH: &str = "./config.toml";
const DEFAULT_DB_PATH: &str = "./data.ms";
const DEFAULT_HTTP_ADDR: &str = "localhost:7700";
const DEFAULT_ENV: &str = "development";
const DEFAULT_MAX_INDEX_SIZE: &str = "100 GiB";
const DEFAULT_MAX_TASK_DB_SIZE: &str = "100 GiB";
const DEFAULT_HTTP_PAYLOAD_SIZE_LIMIT: &str = "100 MB";
const DEFAULT_SNAPSHOT_DIR: &str = "snapshots/";
const DEFAULT_SNAPSHOT_INTERVAL_SEC: u64 = 86400;
const DEFAULT_DUMP_DIR: &str = "dumps/";
const DEFAULT_LOG_LEVEL: &str = "INFO";

const MEILI_MAX_INDEXING_MEMORY: &str = "MEILI_MAX_INDEXING_MEMORY";
const MEILI_MAX_INDEXING_THREADS: &str = "MEILI_MAX_INDEXING_THREADS";
const DISABLE_AUTO_BATCHING: &str = "DISABLE_AUTO_BATCHING";
const DEFAULT_LOG_EVERY_N: usize = 100000;

#[derive(Debug, Clone, Parser, Deserialize)]
#[clap(version, next_display_order = None)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Opt {
    /// Designates the location where database files will be created and retrieved.
    #[clap(long, env = MEILI_DB_PATH, default_value_os_t = default_db_path())]
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// Sets the HTTP address and port Meilisearch will use.
    #[clap(long, env = MEILI_HTTP_ADDR, default_value_t = default_http_addr())]
    #[serde(default = "default_http_addr")]
    pub http_addr: String,

    /// Sets the instance's master key, automatically protecting all routes except `GET /health`.
    #[clap(long, env = MEILI_MASTER_KEY)]
    pub master_key: Option<String>,

    /// Configures the instance's environment. Value must be either `production` or `development`.
    #[clap(long, env = MEILI_ENV, default_value_t = default_env(), value_parser = POSSIBLE_ENV)]
    #[serde(default = "default_env")]
    pub env: String,

    /// Deactivates Meilisearch's built-in telemetry when provided.
    ///
    /// Meilisearch automatically collects data from all instances that do not opt out using this flag.
    /// All gathered data is used solely for the purpose of improving Meilisearch, and can be deleted
    /// at any time.
    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    #[serde(default)] // we can't send true
    #[clap(long, env = MEILI_NO_ANALYTICS)]
    pub no_analytics: bool,

    /// Sets the maximum size of the index. Value must be given in bytes or explicitly stating a base unit (for instance: 107374182400, '107.7Gb', or '107374 Mb').
    #[clap(long, env = MEILI_MAX_INDEX_SIZE, default_value_t = default_max_index_size())]
    #[serde(default = "default_max_index_size")]
    pub max_index_size: Byte,

    /// Sets the maximum size of the task database. Value must be given in bytes or explicitly stating a
    /// base unit (for instance: 107374182400, '107.7Gb', or '107374 Mb').
    #[clap(long, env = MEILI_MAX_TASK_DB_SIZE, default_value_t = default_max_task_db_size())]
    #[serde(default = "default_max_task_db_size")]
    pub max_task_db_size: Byte,

    /// Sets the maximum size of accepted payloads. Value must be given in bytes or explicitly stating a
    /// base unit (for instance: 107374182400, '107.7Gb', or '107374 Mb').
    #[clap(long, env = MEILI_HTTP_PAYLOAD_SIZE_LIMIT, default_value_t = default_http_payload_size_limit())]
    #[serde(default = "default_http_payload_size_limit")]
    pub http_payload_size_limit: Byte,

    /// Sets the server's SSL certificates.
    #[clap(long, env = MEILI_SSL_CERT_PATH, value_parser)]
    pub ssl_cert_path: Option<PathBuf>,

    /// Sets the server's SSL key files.
    #[clap(long, env = MEILI_SSL_KEY_PATH, value_parser)]
    pub ssl_key_path: Option<PathBuf>,

    /// Enables client authentication in the specified path.
    #[clap(long, env = MEILI_SSL_AUTH_PATH, value_parser)]
    pub ssl_auth_path: Option<PathBuf>,

    /// Sets the server's OCSP file. *Optional*
    ///
    /// Reads DER-encoded OCSP response from OCSPFILE and staple to certificate.
    #[clap(long, env = MEILI_SSL_OCSP_PATH, value_parser)]
    pub ssl_ocsp_path: Option<PathBuf>,

    /// Makes SSL authentication mandatory.
    #[serde(default)]
    #[clap(long, env = MEILI_SSL_REQUIRE_AUTH)]
    pub ssl_require_auth: bool,

    /// Activates SSL session resumption.
    #[serde(default)]
    #[clap(long, env = MEILI_SSL_RESUMPTION)]
    pub ssl_resumption: bool,

    /// Activates SSL tickets.
    #[serde(default)]
    #[clap(long, env = MEILI_SSL_TICKETS)]
    pub ssl_tickets: bool,

    /// Launches Meilisearch after importing a previously-generated snapshot at the given filepath.
    #[clap(long, env = MEILI_IMPORT_SNAPSHOT)]
    pub import_snapshot: Option<PathBuf>,

    /// Prevents a Meilisearch instance from throwing an error when `--import-snapshot`
    /// does not point to a valid snapshot file.
    ///
    /// This command will throw an error if `--import-snapshot` is not defined.
    #[clap(
        long,
        env = MEILI_IGNORE_MISSING_SNAPSHOT,
        requires = "import_snapshot"
    )]
    #[serde(default)]
    pub ignore_missing_snapshot: bool,

    /// Prevents a Meilisearch instance with an existing database from throwing an
    /// error when using `--import-snapshot`. Instead, the snapshot will be ignored
    /// and Meilisearch will launch using the existing database.
    ///
    /// This command will throw an error if `--import-snapshot` is not defined.
    #[clap(
        long,
        env = MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS,
        requires = "import_snapshot"
    )]
    #[serde(default)]
    pub ignore_snapshot_if_db_exists: bool,

    /// Sets the directory where Meilisearch will store snapshots.
    #[clap(long, env = MEILI_SNAPSHOT_DIR, default_value_os_t = default_snapshot_dir())]
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: PathBuf,

    /// Activates scheduled snapshots when provided. Snapshots are disabled by default.
    #[clap(long, env = MEILI_SCHEDULE_SNAPSHOT)]
    #[serde(default)]
    pub schedule_snapshot: bool,

    /// Defines the interval between each snapshot. Value must be given in seconds.
    #[clap(long, env = MEILI_SNAPSHOT_INTERVAL_SEC, default_value_t = default_snapshot_interval_sec())]
    #[serde(default = "default_snapshot_interval_sec")]
    pub snapshot_interval_sec: u64,

    /// Imports the dump file located at the specified path. Path must point to a `.dump` file.
    /// If a database already exists, Meilisearch will throw an error and abort launch.
    #[clap(long, env = MEILI_IMPORT_DUMP, conflicts_with = "import_snapshot")]
    pub import_dump: Option<PathBuf>,

    /// Prevents Meilisearch from throwing an error when `--import-dump` does not point to
    /// a valid dump file. Instead, Meilisearch will start normally without importing any dump.
    ///
    /// This option will trigger an error if `--import-dump` is not defined.
    #[clap(long, env = MEILI_IGNORE_MISSING_DUMP, requires = "import_dump")]
    #[serde(default)]
    pub ignore_missing_dump: bool,

    /// Prevents a Meilisearch instance with an existing database from throwing an error
    /// when using `--import-dump`. Instead, the dump will be ignored and Meilisearch will
    /// launch using the existing database.
    ///
    /// This option will trigger an error if `--import-dump` is not defined.
    #[clap(long, env = MEILI_IGNORE_DUMP_IF_DB_EXISTS, requires = "import_dump")]
    #[serde(default)]
    pub ignore_dump_if_db_exists: bool,

    /// Sets the directory where Meilisearch will create dump files.
    #[clap(long, env = MEILI_DUMP_DIR, default_value_os_t = default_dump_dir())]
    #[serde(default = "default_dump_dir")]
    pub dump_dir: PathBuf,

    /// Defines how much detail should be present in Meilisearch's logs.
    ///
    /// Meilisearch currently supports five log levels, listed in order of increasing verbosity: ERROR, WARN, INFO, DEBUG, TRACE.
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

    /// Set the path to a configuration file that should be used to setup the engine.
    /// Format must be TOML.
    #[clap(long)]
    pub config_file_path: Option<PathBuf>,
}

impl Opt {
    /// Whether analytics should be enabled or not.
    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    pub fn analytics(&self) -> bool {
        !self.no_analytics
    }

    /// Build a new Opt from config file, env vars and cli args.
    pub fn try_build() -> anyhow::Result<(Self, Option<PathBuf>)> {
        // Parse the args to get the config_file_path.
        let mut opts = Opt::parse();
        let mut config_read_from = None;
        let user_specified_config_file_path = opts
            .config_file_path
            .clone()
            .or_else(|| env::var("MEILI_CONFIG_FILE_PATH").map(PathBuf::from).ok());
        let config_file_path = user_specified_config_file_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE_PATH));

        match std::fs::read(&config_file_path) {
            Ok(config) => {
                // If the file is successfully read, we deserialize it with `toml`.
                let opt_from_config = toml::from_slice::<Opt>(&config)?;
                // Return an error if config file contains 'config_file_path'
                // Using that key in the config file doesn't make sense bc it creates a logical loop (config file referencing itself)
                if opt_from_config.config_file_path.is_some() {
                    anyhow::bail!("`config_file_path` is not supported in the configuration file")
                }
                // We inject the values from the toml in the corresponding env vars if needs be. Doing so, we respect the priority toml < env vars < cli args.
                opt_from_config.export_to_env();
                // Once injected we parse the cli args once again to take the new env vars into scope.
                opts = Opt::parse();
                config_read_from = Some(config_file_path);
            }
            Err(e) => {
                if let Some(path) = user_specified_config_file_path {
                    // If we have an error while reading the file defined by the user.
                    anyhow::bail!(
                        "unable to open or read the {:?} configuration file: {}.",
                        path,
                        e,
                    )
                }
            }
        }

        Ok((opts, config_read_from))
    }

    /// Exports the opts values to their corresponding env vars if they are not set.
    fn export_to_env(self) {
        let Opt {
            db_path,
            http_addr,
            master_key,
            env,
            max_index_size,
            max_task_db_size,
            http_payload_size_limit,
            ssl_cert_path,
            ssl_key_path,
            ssl_auth_path,
            ssl_ocsp_path,
            ssl_require_auth,
            ssl_resumption,
            ssl_tickets,
            snapshot_dir,
            schedule_snapshot,
            snapshot_interval_sec,
            dump_dir,
            log_level,
            indexer_options,
            scheduler_options,
            import_snapshot: _,
            ignore_missing_snapshot: _,
            ignore_snapshot_if_db_exists: _,
            import_dump: _,
            ignore_missing_dump: _,
            ignore_dump_if_db_exists: _,
            config_file_path: _,
            #[cfg(all(not(debug_assertions), feature = "analytics"))]
            no_analytics,
            #[cfg(feature = "metrics")]
            enable_metrics_route,
        } = self;
        export_to_env_if_not_present(MEILI_DB_PATH, db_path);
        export_to_env_if_not_present(MEILI_HTTP_ADDR, http_addr);
        if let Some(master_key) = master_key {
            export_to_env_if_not_present(MEILI_MASTER_KEY, master_key);
        }
        export_to_env_if_not_present(MEILI_ENV, env);
        #[cfg(all(not(debug_assertions), feature = "analytics"))]
        {
            export_to_env_if_not_present(MEILI_NO_ANALYTICS, no_analytics.to_string());
        }
        export_to_env_if_not_present(MEILI_MAX_INDEX_SIZE, max_index_size.to_string());
        export_to_env_if_not_present(MEILI_MAX_TASK_DB_SIZE, max_task_db_size.to_string());
        export_to_env_if_not_present(
            MEILI_HTTP_PAYLOAD_SIZE_LIMIT,
            http_payload_size_limit.to_string(),
        );
        if let Some(ssl_cert_path) = ssl_cert_path {
            export_to_env_if_not_present(MEILI_SSL_CERT_PATH, ssl_cert_path);
        }
        if let Some(ssl_key_path) = ssl_key_path {
            export_to_env_if_not_present(MEILI_SSL_KEY_PATH, ssl_key_path);
        }
        if let Some(ssl_auth_path) = ssl_auth_path {
            export_to_env_if_not_present(MEILI_SSL_AUTH_PATH, ssl_auth_path);
        }
        if let Some(ssl_ocsp_path) = ssl_ocsp_path {
            export_to_env_if_not_present(MEILI_SSL_OCSP_PATH, ssl_ocsp_path);
        }
        export_to_env_if_not_present(MEILI_SSL_REQUIRE_AUTH, ssl_require_auth.to_string());
        export_to_env_if_not_present(MEILI_SSL_RESUMPTION, ssl_resumption.to_string());
        export_to_env_if_not_present(MEILI_SSL_TICKETS, ssl_tickets.to_string());
        export_to_env_if_not_present(MEILI_SNAPSHOT_DIR, snapshot_dir);
        export_to_env_if_not_present(MEILI_SCHEDULE_SNAPSHOT, schedule_snapshot.to_string());
        export_to_env_if_not_present(
            MEILI_SNAPSHOT_INTERVAL_SEC,
            snapshot_interval_sec.to_string(),
        );
        export_to_env_if_not_present(MEILI_DUMP_DIR, dump_dir);
        export_to_env_if_not_present(MEILI_LOG_LEVEL, log_level);
        #[cfg(feature = "metrics")]
        {
            export_to_env_if_not_present(
                MEILI_ENABLE_METRICS_ROUTE,
                enable_metrics_route.to_string(),
            );
        }
        indexer_options.export_to_env();
        scheduler_options.export_to_env();
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

#[derive(Debug, Clone, Parser, Deserialize)]
pub struct IndexerOpts {
    /// Sets the amount of documents to skip before printing
    /// a log regarding the indexing advancement.
    #[serde(default = "default_log_every_n")]
    #[clap(long, default_value_t = default_log_every_n(), hide = true)] // 100k
    pub log_every_n: usize,

    /// Grenad max number of chunks in bytes.
    #[clap(long, hide = true)]
    pub max_nb_chunks: Option<usize>,

    /// Sets the maximum amount of RAM Meilisearch can use when indexing. By default, Meilisearch
    /// uses no more than two thirds of available memory.
    #[clap(long, env = MEILI_MAX_INDEXING_MEMORY, default_value_t)]
    #[serde(default)]
    pub max_indexing_memory: MaxMemory,

    /// Sets the maximum number of threads Meilisearch can use during indexation. By default, the
    /// indexer avoids using more than half of a machine's total processing units. This ensures
    /// Meilisearch is always ready to perform searches, even while you are updating an index.
    #[clap(long, env = MEILI_MAX_INDEXING_THREADS, default_value_t)]
    #[serde(default)]
    pub max_indexing_threads: MaxThreads,
}

impl IndexerOpts {
    /// Exports the values to their corresponding env vars if they are not set.
    pub fn export_to_env(self) {
        let IndexerOpts {
            max_indexing_memory,
            max_indexing_threads,
            log_every_n: _,
            max_nb_chunks: _,
        } = self;
        if let Some(max_indexing_memory) = max_indexing_memory.0 {
            export_to_env_if_not_present(
                MEILI_MAX_INDEXING_MEMORY,
                max_indexing_memory.to_string(),
            );
        }
        export_to_env_if_not_present(
            MEILI_MAX_INDEXING_THREADS,
            max_indexing_threads.0.to_string(),
        );
    }
}

#[derive(Debug, Clone, Parser, Default, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SchedulerConfig {
    /// Deactivates auto-batching when provided.
    #[clap(long, env = DISABLE_AUTO_BATCHING)]
    #[serde(default)]
    pub disable_auto_batching: bool,
}

impl SchedulerConfig {
    pub fn export_to_env(self) {
        let SchedulerConfig { disable_auto_batching } = self;
        export_to_env_if_not_present(DISABLE_AUTO_BATCHING, disable_auto_batching.to_string());
    }
}

impl TryFrom<&IndexerOpts> for IndexerConfig {
    type Error = anyhow::Error;

    fn try_from(other: &IndexerOpts) -> Result<Self, Self::Error> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|index| format!("indexing-thread:{index}"))
            .num_threads(*other.max_indexing_threads)
            .build()?;

        Ok(Self {
            log_every_n: Some(other.log_every_n),
            max_nb_chunks: other.max_nb_chunks,
            max_memory: other.max_indexing_memory.map(|b| b.get_bytes() as usize),
            thread_pool: Some(thread_pool),
            max_positions_per_attributes: None,
            ..Default::default()
        })
    }
}

impl Default for IndexerOpts {
    fn default() -> Self {
        Self {
            log_every_n: 100_000,
            max_nb_chunks: None,
            max_indexing_memory: MaxMemory::default(),
            max_indexing_threads: MaxThreads::default(),
        }
    }
}

/// A type used to detect the max memory available and use 2/3 of it.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct MaxMemory(Option<Byte>);

impl FromStr for MaxMemory {
    type Err = ByteError;

    fn from_str(s: &str) -> Result<MaxMemory, ByteError> {
        Byte::from_str(s).map(Some).map(MaxMemory)
    }
}

impl Default for MaxMemory {
    fn default() -> MaxMemory {
        MaxMemory(total_memory_bytes().map(|bytes| bytes * 2 / 3).map(Byte::from_bytes))
    }
}

impl fmt::Display for MaxMemory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Some(memory) => write!(f, "{}", memory.get_appropriate_unit(true)),
            None => f.write_str("unknown"),
        }
    }
}

impl Deref for MaxMemory {
    type Target = Option<Byte>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MaxMemory {
    pub fn unlimited() -> Self {
        Self(None)
    }
}

/// Returns the total amount of bytes available or `None` if this system isn't supported.
fn total_memory_bytes() -> Option<u64> {
    if System::IS_SUPPORTED {
        let memory_kind = RefreshKind::new().with_memory();
        let mut system = System::new_with_specifics(memory_kind);
        system.refresh_memory();
        Some(system.total_memory())
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct MaxThreads(usize);

impl FromStr for MaxThreads {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        usize::from_str(s).map(Self)
    }
}

impl Default for MaxThreads {
    fn default() -> Self {
        MaxThreads(num_cpus::get() / 2)
    }
}

impl fmt::Display for MaxThreads {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for MaxThreads {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
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

/// Checks if the key is defined in the environment variables.
/// If not, inserts it with the given value.
pub fn export_to_env_if_not_present<T>(key: &str, value: T)
where
    T: AsRef<OsStr>,
{
    if let Err(VarError::NotPresent) = std::env::var(key) {
        std::env::set_var(key, value);
    }
}

/// Functions used to get default value for `Opt` fields, needs to be function because of serde's default attribute.

fn default_db_path() -> PathBuf {
    PathBuf::from(DEFAULT_DB_PATH)
}

pub fn default_http_addr() -> String {
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

fn default_dump_dir() -> PathBuf {
    PathBuf::from(DEFAULT_DUMP_DIR)
}

fn default_log_level() -> String {
    DEFAULT_LOG_LEVEL.to_string()
}

fn default_log_every_n() -> usize {
    DEFAULT_LOG_EVERY_N
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_valid_opt() {
        assert!(Opt::try_parse_from(Some("")).is_ok());
    }

    #[test]
    #[ignore]
    fn test_meilli_config_file_path_valid() {
        temp_env::with_vars(
            vec![("MEILI_CONFIG_FILE_PATH", Some("../config.toml"))], // Relative path in meilisearch package
            || {
                assert!(Opt::try_build().is_ok());
            },
        );
    }

    #[test]
    #[ignore]
    fn test_meilli_config_file_path_invalid() {
        temp_env::with_vars(vec![("MEILI_CONFIG_FILE_PATH", Some("../configgg.toml"))], || {
            let possible_error_messages = [
                    "unable to open or read the \"../configgg.toml\" configuration file: No such file or directory (os error 2).",
                    "unable to open or read the \"../configgg.toml\" configuration file: The system cannot find the file specified. (os error 2).", // Windows
                ];
            let error_message = Opt::try_build().unwrap_err().to_string();
            assert!(
                possible_error_messages.contains(&error_message.as_str()),
                "Expected onf of {:?}, got {:?}.",
                possible_error_messages,
                error_message
            );
        });
    }
}
