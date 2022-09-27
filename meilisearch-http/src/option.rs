use std::convert::TryFrom;
use std::io::{BufReader, Read};
use std::num::ParseIntError;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, fs};

use byte_unit::{Byte, ByteError};
use clap::Parser;
use index_scheduler::milli::update::IndexerConfig;
use rustls::{
    server::{
        AllowAnyAnonymousOrAuthenticatedClient, AllowAnyAuthenticatedClient,
        ServerSessionMemoryCache,
    },
    RootCertStore,
};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use serde::Serialize;
use sysinfo::{RefreshKind, System, SystemExt};

const POSSIBLE_ENV: [&str; 2] = ["development", "production"];

#[derive(Debug, Clone, Parser, Serialize)]
#[clap(version)]
pub struct Opt {
    /// The destination where the database must be created.
    #[clap(long, env = "MEILI_DB_PATH", default_value = "./data.ms")]
    pub db_path: PathBuf,

    /// The address on which the http server will listen.
    #[clap(long, env = "MEILI_HTTP_ADDR", default_value = "127.0.0.1:7700")]
    pub http_addr: String,

    /// The master key allowing you to do everything on the server.
    #[serde(skip)]
    #[clap(long, env = "MEILI_MASTER_KEY")]
    pub master_key: Option<String>,

    /// This environment variable must be set to `production` if you are running in production.
    /// If the server is running in development mode more logs will be displayed,
    /// and the master key can be avoided which implies that there is no security on the updates routes.
    /// This is useful to debug when integrating the engine with another service.
    #[clap(long, env = "MEILI_ENV", default_value = "development", possible_values = &POSSIBLE_ENV)]
    pub env: String,

    /// Do not send analytics to Meili.
    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    #[serde(skip)] // we can't send true
    #[clap(long, env = "MEILI_NO_ANALYTICS")]
    pub no_analytics: bool,

    /// The maximum size, in bytes, of the main lmdb database directory
    #[clap(long, env = "MEILI_MAX_INDEX_SIZE", default_value = "100 GiB")]
    pub max_index_size: Byte,

    /// The maximum size, in bytes, of the update lmdb database directory
    #[clap(long, env = "MEILI_MAX_TASK_DB_SIZE", default_value = "100 GiB")]
    pub max_task_db_size: Byte,

    /// The maximum size, in bytes, of accepted JSON payloads
    #[clap(long, env = "MEILI_HTTP_PAYLOAD_SIZE_LIMIT", default_value = "100 MB")]
    pub http_payload_size_limit: Byte,

    /// Read server certificates from CERTFILE.
    /// This should contain PEM-format certificates
    /// in the right order (the first certificate should
    /// certify KEYFILE, the last should be a root CA).
    #[serde(skip)]
    #[clap(long, env = "MEILI_SSL_CERT_PATH", parse(from_os_str))]
    pub ssl_cert_path: Option<PathBuf>,

    /// Read private key from KEYFILE.  This should be a RSA
    /// private key or PKCS8-encoded private key, in PEM format.
    #[serde(skip)]
    #[clap(long, env = "MEILI_SSL_KEY_PATH", parse(from_os_str))]
    pub ssl_key_path: Option<PathBuf>,

    /// Enable client authentication, and accept certificates
    /// signed by those roots provided in CERTFILE.
    #[clap(long, env = "MEILI_SSL_AUTH_PATH", parse(from_os_str))]
    #[serde(skip)]
    pub ssl_auth_path: Option<PathBuf>,

    /// Read DER-encoded OCSP response from OCSPFILE and staple to certificate.
    /// Optional
    #[serde(skip)]
    #[clap(long, env = "MEILI_SSL_OCSP_PATH", parse(from_os_str))]
    pub ssl_ocsp_path: Option<PathBuf>,

    /// Send a fatal alert if the client does not complete client authentication.
    #[serde(skip)]
    #[clap(long, env = "MEILI_SSL_REQUIRE_AUTH")]
    pub ssl_require_auth: bool,

    /// SSL support session resumption
    #[serde(skip)]
    #[clap(long, env = "MEILI_SSL_RESUMPTION")]
    pub ssl_resumption: bool,

    /// SSL support tickets.
    #[serde(skip)]
    #[clap(long, env = "MEILI_SSL_TICKETS")]
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
    pub ignore_missing_snapshot: bool,

    /// The engine will skip snapshot importation and not return an error in such case.
    #[clap(
        long,
        env = "MEILI_IGNORE_SNAPSHOT_IF_DB_EXISTS",
        requires = "import-snapshot"
    )]
    pub ignore_snapshot_if_db_exists: bool,

    /// Defines the directory path where meilisearch will create snapshot each snapshot_time_gap.
    #[clap(long, env = "MEILI_SNAPSHOT_DIR", default_value = "snapshots/")]
    pub snapshot_dir: PathBuf,

    /// Activate snapshot scheduling.
    #[clap(long, env = "MEILI_SCHEDULE_SNAPSHOT")]
    pub schedule_snapshot: bool,

    /// Defines time interval, in seconds, between each snapshot creation.
    #[clap(long, env = "MEILI_SNAPSHOT_INTERVAL_SEC", default_value = "86400")] // 24h
    pub snapshot_interval_sec: u64,

    /// Import a dump from the specified path, must be a `.dump` file.
    #[clap(long, env = "MEILI_IMPORT_DUMP", conflicts_with = "import-snapshot")]
    pub import_dump: Option<PathBuf>,

    /// If the dump doesn't exists, load or create the database specified by `db-path` instead.
    #[clap(long, env = "MEILI_IGNORE_MISSING_DUMP", requires = "import-dump")]
    pub ignore_missing_dump: bool,

    /// Ignore the dump if a database already exists, and load that database instead.
    #[clap(long, env = "MEILI_IGNORE_DUMP_IF_DB_EXISTS", requires = "import-dump")]
    pub ignore_dump_if_db_exists: bool,

    /// Folder where dumps are created when the dump route is called.
    #[clap(long, env = "MEILI_DUMPS_DIR", default_value = "dumps/")]
    pub dumps_dir: PathBuf,

    /// Set the log level
    #[clap(long, env = "MEILI_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Enables Prometheus metrics and /metrics route.
    #[cfg(feature = "metrics")]
    #[clap(long, env = "MEILI_ENABLE_METRICS_ROUTE")]
    pub enable_metrics_route: bool,

    #[serde(flatten)]
    #[clap(flatten)]
    pub indexer_options: IndexerOpts,

    #[serde(flatten)]
    #[clap(flatten)]
    pub scheduler_options: SchedulerConfig,
}

impl Opt {
    /// Wether analytics should be enabled or not.
    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    pub fn analytics(&self) -> bool {
        !self.no_analytics
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

#[derive(Debug, Clone, Parser, Serialize)]
pub struct IndexerOpts {
    /// The amount of documents to skip before printing
    /// a log regarding the indexing advancement.
    #[serde(skip)]
    #[clap(long, default_value = "100000", hide = true)] // 100k
    pub log_every_n: usize,

    /// Grenad max number of chunks in bytes.
    #[serde(skip)]
    #[clap(long, hide = true)]
    pub max_nb_chunks: Option<usize>,

    /// The maximum amount of memory the indexer will use. It defaults to 2/3
    /// of the available memory. It is recommended to use something like 80%-90%
    /// of the available memory, no more.
    ///
    /// In case the engine is unable to retrieve the available memory the engine will
    /// try to use the memory it needs but without real limit, this can lead to
    /// Out-Of-Memory issues and it is recommended to specify the amount of memory to use.
    #[clap(long, env = "MEILI_MAX_INDEXING_MEMORY", default_value_t)]
    pub max_indexing_memory: MaxMemory,

    /// The maximum number of threads the indexer will use.
    /// If the number set is higher than the real number of cores available in the machine,
    /// it will use the maximum number of available cores.
    ///
    /// It defaults to half of the available threads.
    #[clap(long, env = "MEILI_MAX_INDEXING_THREADS", default_value_t)]
    pub max_indexing_threads: MaxThreads,
}

#[derive(Debug, Clone, Parser, Default, Serialize)]
pub struct SchedulerConfig {
    /// The engine will disable task auto-batching,
    /// and will sequencialy compute each task one by one.
    #[clap(long, env = "DISABLE_AUTO_BATCHING")]
    pub disable_auto_batching: bool,
}

impl TryFrom<&IndexerOpts> for IndexerConfig {
    type Error = anyhow::Error;

    fn try_from(other: &IndexerOpts) -> Result<Self, Self::Error> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
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
#[derive(Debug, Clone, Copy, Serialize)]
pub struct MaxMemory(Option<Byte>);

impl FromStr for MaxMemory {
    type Err = ByteError;

    fn from_str(s: &str) -> Result<MaxMemory, ByteError> {
        Byte::from_str(s).map(Some).map(MaxMemory)
    }
}

impl Default for MaxMemory {
    fn default() -> MaxMemory {
        MaxMemory(
            total_memory_bytes()
                .map(|bytes| bytes * 2 / 3)
                .map(Byte::from_bytes),
        )
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
        Some(system.total_memory() * 1024) // KiB into bytes
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_valid_opt() {
        assert!(Opt::try_parse_from(Some("")).is_ok());
    }
}
