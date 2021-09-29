use std::fs;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;

use byte_unit::Byte;
use meilisearch_lib::options::IndexerOpts;
use rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls::{
    AllowAnyAnonymousOrAuthenticatedClient, AllowAnyAuthenticatedClient, NoClientAuth,
    RootCertStore,
};
use structopt::StructOpt;

const POSSIBLE_ENV: [&str; 2] = ["development", "production"];

#[derive(Debug, Clone, StructOpt)]
pub struct Opt {
    /// The destination where the database must be created.
    #[structopt(long, env = "MEILI_DB_PATH", default_value = "./data.ms")]
    pub db_path: PathBuf,

    /// The address on which the http server will listen.
    #[structopt(long, env = "MEILI_HTTP_ADDR", default_value = "127.0.0.1:7700")]
    pub http_addr: String,

    /// The master key allowing you to do everything on the server.
    #[structopt(long, env = "MEILI_MASTER_KEY")]
    pub master_key: Option<String>,

    /// This environment variable must be set to `production` if you are running in production.
    /// If the server is running in development mode more logs will be displayed,
    /// and the master key can be avoided which implies that there is no security on the updates routes.
    /// This is useful to debug when integrating the engine with another service.
    #[structopt(long, env = "MEILI_ENV", default_value = "development", possible_values = &POSSIBLE_ENV)]
    pub env: String,

    /// Do not send analytics to Meili.
    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    #[structopt(long, env = "MEILI_NO_ANALYTICS")]
    pub no_analytics: bool,

    /// The maximum size, in bytes, of the main lmdb database directory
    #[structopt(long, env = "MEILI_MAX_INDEX_SIZE", default_value = "100 GiB")]
    pub max_index_size: Byte,

    /// The maximum size, in bytes, of the update lmdb database directory
    #[structopt(long, env = "MEILI_MAX_UDB_SIZE", default_value = "100 GiB")]
    pub max_udb_size: Byte,

    /// The maximum size, in bytes, of accepted JSON payloads
    #[structopt(long, env = "MEILI_HTTP_PAYLOAD_SIZE_LIMIT", default_value = "100 MB")]
    pub http_payload_size_limit: Byte,

    /// Read server certificates from CERTFILE.
    /// This should contain PEM-format certificates
    /// in the right order (the first certificate should
    /// certify KEYFILE, the last should be a root CA).
    #[structopt(long, env = "MEILI_SSL_CERT_PATH", parse(from_os_str))]
    pub ssl_cert_path: Option<PathBuf>,

    /// Read private key from KEYFILE.  This should be a RSA
    /// private key or PKCS8-encoded private key, in PEM format.
    #[structopt(long, env = "MEILI_SSL_KEY_PATH", parse(from_os_str))]
    pub ssl_key_path: Option<PathBuf>,

    /// Enable client authentication, and accept certificates
    /// signed by those roots provided in CERTFILE.
    #[structopt(long, env = "MEILI_SSL_AUTH_PATH", parse(from_os_str))]
    pub ssl_auth_path: Option<PathBuf>,

    /// Read DER-encoded OCSP response from OCSPFILE and staple to certificate.
    /// Optional
    #[structopt(long, env = "MEILI_SSL_OCSP_PATH", parse(from_os_str))]
    pub ssl_ocsp_path: Option<PathBuf>,

    /// Send a fatal alert if the client does not complete client authentication.
    #[structopt(long, env = "MEILI_SSL_REQUIRE_AUTH")]
    pub ssl_require_auth: bool,

    /// SSL support session resumption
    #[structopt(long, env = "MEILI_SSL_RESUMPTION")]
    pub ssl_resumption: bool,

    /// SSL support tickets.
    #[structopt(long, env = "MEILI_SSL_TICKETS")]
    pub ssl_tickets: bool,

    /// Defines the path of the snapshot file to import.
    /// This option will, by default, stop the process if a database already exist or if no snapshot exists at
    /// the given path. If this option is not specified no snapshot is imported.
    #[structopt(long)]
    pub import_snapshot: Option<PathBuf>,

    /// The engine will ignore a missing snapshot and not return an error in such case.
    #[structopt(long, requires = "import-snapshot")]
    pub ignore_missing_snapshot: bool,

    /// The engine will skip snapshot importation and not return an error in such case.
    #[structopt(long, requires = "import-snapshot")]
    pub ignore_snapshot_if_db_exists: bool,

    /// Defines the directory path where meilisearch will create snapshot each snapshot_time_gap.
    #[structopt(long, env = "MEILI_SNAPSHOT_DIR", default_value = "snapshots/")]
    pub snapshot_dir: PathBuf,

    /// Activate snapshot scheduling.
    #[structopt(long, env = "MEILI_SCHEDULE_SNAPSHOT")]
    pub schedule_snapshot: bool,

    /// Defines time interval, in seconds, between each snapshot creation.
    #[structopt(long, env = "MEILI_SNAPSHOT_INTERVAL_SEC", default_value = "86400")] // 24h
    pub snapshot_interval_sec: u64,

    /// Folder where dumps are created when the dump route is called.
    #[structopt(long, env = "MEILI_DUMPS_DIR", default_value = "dumps/")]
    pub dumps_dir: PathBuf,

    /// Import a dump from the specified path, must be a `.dump` file.
    #[structopt(long, conflicts_with = "import-snapshot")]
    pub import_dump: Option<PathBuf>,

    /// Set the log level
    #[structopt(long, env = "MEILI_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[structopt(skip)]
    pub indexer_options: IndexerOpts,
}

impl Opt {
    pub fn get_ssl_config(&self) -> anyhow::Result<Option<rustls::ServerConfig>> {
        if let (Some(cert_path), Some(key_path)) = (&self.ssl_cert_path, &self.ssl_key_path) {
            let client_auth = match &self.ssl_auth_path {
                Some(auth_path) => {
                    let roots = load_certs(auth_path.to_path_buf())?;
                    let mut client_auth_roots = RootCertStore::empty();
                    for root in roots {
                        client_auth_roots.add(&root).unwrap();
                    }
                    if self.ssl_require_auth {
                        AllowAnyAuthenticatedClient::new(client_auth_roots)
                    } else {
                        AllowAnyAnonymousOrAuthenticatedClient::new(client_auth_roots)
                    }
                }
                None => NoClientAuth::new(),
            };

            let mut config = rustls::ServerConfig::new(client_auth);
            config.key_log = Arc::new(rustls::KeyLogFile::new());

            let certs = load_certs(cert_path.to_path_buf())?;
            let privkey = load_private_key(key_path.to_path_buf())?;
            let ocsp = load_ocsp(&self.ssl_ocsp_path)?;
            config
                .set_single_cert_with_ocsp_and_sct(certs, privkey, ocsp, vec![])
                .map_err(|_| anyhow::anyhow!("bad certificates/private key"))?;

            if self.ssl_resumption {
                config.set_persistence(rustls::ServerSessionMemoryCache::new(256));
            }

            if self.ssl_tickets {
                config.ticketer = rustls::Ticketer::new();
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
    certs(&mut reader).map_err(|_| anyhow::anyhow!("cannot read certificate file"))
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
        Ok(pkcs8_keys[0].clone())
    } else {
        assert!(!rsa_keys.is_empty());
        Ok(rsa_keys[0].clone())
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
