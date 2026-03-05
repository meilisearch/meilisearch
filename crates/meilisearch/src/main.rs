use std::env;
use std::io::{stderr, LineWriter, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::available_parallelism;
use std::time::Duration;

use actix_web::http::KeepAlive;
use actix_web::web::Data;
use actix_web::HttpServer;
use index_scheduler::IndexScheduler;
use is_terminal::IsTerminal;
use meilisearch::analytics::Analytics;
use meilisearch::option::LogMode;
use meilisearch::personalization::PersonalizationService;
use meilisearch::search_queue::SearchQueue;
use meilisearch::{
    analytics, create_app, setup_meilisearch, LogRouteHandle, LogRouteType, LogStderrHandle,
    LogStderrType, Opt, ServicesData, SubscriberForSecondLayer,
};
use meilisearch_auth::{generate_master_key, AuthController, MASTER_KEY_MIN_SIZE};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::Layer;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn default_log_route_layer() -> LogRouteType {
    None.with_filter(tracing_subscriber::filter::Targets::new().with_target("", LevelFilter::OFF))
}

fn default_log_stderr_layer(opt: &Opt) -> LogStderrType {
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(|| LineWriter::new(std::io::stderr()))
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE);

    let layer = match opt.experimental_logs_mode {
        LogMode::Human => Box::new(layer)
            as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
        LogMode::Json => Box::new(layer.json())
            as Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
    };

    layer.with_filter(
        tracing_subscriber::filter::Targets::new()
            .with_target("", LevelFilter::from_str(&opt.log_level.to_string()).unwrap()),
    )
}

/// does all the setup before meilisearch is launched
fn setup(opt: &Opt) -> anyhow::Result<(LogRouteHandle, LogStderrHandle)> {
    let (route_layer, route_layer_handle) =
        tracing_subscriber::reload::Layer::new(default_log_route_layer());
    let route_layer: tracing_subscriber::reload::Layer<_, _> = route_layer;

    let (stderr_layer, stderr_layer_handle) =
        tracing_subscriber::reload::Layer::new(default_log_stderr_layer(opt));
    let route_layer: tracing_subscriber::reload::Layer<_, _> = route_layer;

    let subscriber = tracing_subscriber::registry().with(route_layer).with(stderr_layer);

    // set the subscriber as the default for the application
    tracing::subscriber::set_global_default(subscriber).unwrap();

    Ok((route_layer_handle, stderr_layer_handle))
}

fn on_panic(info: &std::panic::PanicHookInfo) {
    let info = info.to_string().replace('\n', " ");
    tracing::error!(%info);
}

// Cluster mode requires a multi-threaded tokio runtime because the Raft LMDB
// store uses `tokio::task::block_in_place()`. The default `#[actix_web::main]`
// creates a current_thread runtime which doesn't support block_in_place.
#[cfg(feature = "cluster")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_inner().await
}

#[cfg(not(feature = "cluster"))]
#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    main_inner().await
}

async fn main_inner() -> anyhow::Result<()> {
    // won't panic inside of tokio::main
    let runtime = tokio::runtime::Handle::current();

    try_main(runtime).await.inspect_err(|error| {
        tracing::error!(%error);
        let mut current = error.source();
        let mut depth = 0;
        while let Some(source) = current {
            tracing::info!(%source, depth, "Error caused by");
            current = source.source();
            depth += 1;
        }
    })
}

async fn try_main(runtime: tokio::runtime::Handle) -> anyhow::Result<()> {
    let (opt, config_read_from) = Opt::try_build()?;

    std::panic::set_hook(Box::new(on_panic));

    anyhow::ensure!(
        !(cfg!(windows) && opt.experimental_reduce_indexing_memory_usage),
        "The `experimental-reduce-indexing-memory-usage` flag is not supported on Windows"
    );

    let log_handle = setup(&opt)?;

    match (opt.env.as_ref(), &opt.master_key) {
        ("production", Some(master_key)) if master_key.len() < MASTER_KEY_MIN_SIZE => {
            anyhow::bail!(
                "The master key must be at least {MASTER_KEY_MIN_SIZE} bytes in a production environment. The provided key is only {} bytes.

{}",
                master_key.len(),
                generated_master_key_message(),
            )
        }
        ("production", None) => {
            anyhow::bail!(
                "You must provide a master key to secure your instance in a production environment. It can be specified via the MEILI_MASTER_KEY environment variable or the --master-key launch option.

{}",
                generated_master_key_message()
            )
        }
        // No error; continue
        _ => (),
    }

    // --cluster-status: query a running node and exit immediately
    if let Some(ref url) = opt.cluster_status_url {
        let status_url = format!("{}/cluster/status", url.trim_end_matches('/'));
        let client = http_client::reqwest::ClientBuilder::new()
            .prepare(|b| b.timeout(Duration::from_secs(5)))
            .danger_build_no_ip_policy()
            .unwrap_or_else(|_| http_client::reqwest::DangerousClient::new());
        let mut req = client.get(&status_url);
        if let Some(ref key) = opt.master_key {
            req = req.header("Authorization", format!("Bearer {key}"));
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_else(|e| format!("(error reading body: {e})"));
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                    println!("{}", serde_json::to_string_pretty(&json).unwrap_or(body));
                } else {
                    println!("{body}");
                }
                std::process::exit(if status.is_success() { 0 } else { 1 });
            }
            Err(e) => {
                eprintln!("Failed to connect to {status_url}: {e}");
                std::process::exit(1);
            }
        }
    }

    // --cluster-leave: tell a running node to leave its cluster and exit
    if let Some(ref url) = opt.cluster_leave_url {
        let leave_url = format!("{}/cluster/status/leave", url.trim_end_matches('/'));
        let client = http_client::reqwest::ClientBuilder::new()
            .prepare(|b| b.timeout(Duration::from_secs(15)))
            .danger_build_no_ip_policy()
            .unwrap_or_else(|_| http_client::reqwest::DangerousClient::new());
        let mut req = client.post(&leave_url);
        if let Some(ref key) = opt.master_key {
            req = req.header("Authorization", format!("Bearer {key}"));
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_else(|e| format!("(error reading body: {e})"));
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                    println!("{}", serde_json::to_string_pretty(&json).unwrap_or(body));
                } else {
                    println!("{body}");
                }
                std::process::exit(if status.is_success() { 0 } else { 1 });
            }
            Err(e) => {
                eprintln!("Failed to connect to {leave_url}: {e}");
                std::process::exit(1);
            }
        }
    }

    // --cluster-reset: wipe persisted cluster state and exit
    if opt.cluster_reset {
        let cluster_path = opt.db_path.join("cluster");
        if cluster_path.exists() {
            std::fs::remove_dir_all(&cluster_path)
                .expect("failed to remove cluster directory");
            eprintln!("Cluster state wiped: {}", cluster_path.display());
            eprintln!("Re-create with --cluster-create or re-join with --cluster-join.");
        } else {
            eprintln!("No cluster state found at {}", cluster_path.display());
        }
        std::process::exit(0);
    }

    // --cluster-show-secret: print the derived cluster secret and exit
    if opt.cluster_show_secret {
        if let Some(ref explicit) = opt.cluster_secret {
            println!("{explicit}");
        } else if let Some(ref mk) = opt.master_key {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(b"meili-cluster-secret:");
            hasher.update(mk.as_bytes());
            let derived: String =
                hasher.finalize().iter().map(|b| format!("{b:02x}")).collect();
            println!("{derived}");
        } else {
            eprintln!("Error: --cluster-show-secret requires --master-key or --cluster-secret");
            std::process::exit(1);
        }
        std::process::exit(0);
    }

    // For cluster-join mode, the join handshake (including snapshot reception) MUST
    // happen BEFORE setup_meilisearch() creates the IndexScheduler. The snapshot
    // replaces files at db_path, and LMDB environments must be opened AFTER those
    // files are in place. Otherwise the IndexScheduler opens empty LMDB envs that
    // never see the snapshot data.
    #[cfg(feature = "cluster")]
    let pre_joined_node: Option<meilisearch_cluster::ClusterNode> = {
        if let Some(ref join_addr) = opt.cluster_join {
            let bootstrap_addr: std::net::SocketAddr = join_addr
                .parse()
                .or_else(|_| {
                    // If not a numeric IP:port, try DNS resolution (for Docker/Kubernetes hostnames)
                    use std::net::ToSocketAddrs;
                    join_addr
                        .to_socket_addrs()
                        .map_err(|e| e.to_string())
                        .and_then(|mut addrs| {
                            addrs.next().ok_or_else(|| {
                                format!("DNS resolution for '{join_addr}' returned no addresses")
                            })
                        })
                })
                .expect("invalid --cluster-join address (expected host:port or hostname:port)");

            let bind_addr: std::net::SocketAddr = opt
                .cluster_bind
                .as_deref()
                .unwrap_or("0.0.0.0:7701")
                .parse()
                .expect("invalid --cluster-bind address");

            let secret = opt.cluster_secret.clone().or_else(|| {
                opt.master_key.as_ref().map(|mk| {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(b"meili-cluster-secret:");
                    hasher.update(mk.as_bytes());
                    hasher.finalize().iter().map(|b| format!("{b:02x}")).collect::<String>()
                })
            }).expect("--cluster-secret or --master-key is required when joining a cluster");

            let cluster_config = meilisearch_cluster::ClusterConfig {
                heartbeat_ms: opt.cluster_heartbeat_ms,
                election_timeout_min_ms: opt.cluster_election_timeout_min_ms,
                election_timeout_max_ms: opt.cluster_election_timeout_max_ms,
                accept_timeout_ms: opt.cluster_accept_timeout_ms,
                max_message_size_mb: opt.cluster_max_message_size_mb,
                raft_db_size_mb: opt.cluster_raft_db_size_mb,
                max_transfer_failures: opt.cluster_max_transfer_failures,
                max_replication_lag: opt.cluster_max_replication_lag,
                write_timeout_secs: opt.cluster_write_timeout_secs,
                snapshot_max_compaction_age_s: opt.cluster_snapshot_max_compaction_age_s,
                tls: opt.cluster_tls,
            };

            // Derive the advertise addresses from --cluster-addr (if set).
            // In Docker/K8s, --cluster-addr provides the hostname (e.g., "node2")
            // and we combine it with the respective ports for QUIC and HTTP.
            let quic_advertise = if let Some(ref cluster_addr) = opt.cluster_addr {
                format!("{}:{}", cluster_addr, bind_addr.port())
            } else {
                bind_addr.to_string()
            };
            if quic_advertise.starts_with("0.0.0.0:") || quic_advertise.starts_with("[::]:") {
                anyhow::bail!(
                    "--cluster-bind is {bind_addr}, which is not routable by peers. \
                     Set --cluster-addr to the hostname or IP other nodes can reach."
                );
            }
            let http_url = if let Some(ref cluster_addr) = opt.cluster_addr {
                let http_port = opt.http_addr.rsplit(':').next().unwrap_or("7700");
                format!("http://{}:{}", cluster_addr, http_port)
            } else {
                format!("http://{}", opt.http_addr)
            };
            let node = meilisearch_cluster::ClusterNode::join(
                opt.cluster_node_id,
                bind_addr,
                quic_advertise,
                http_url,
                bootstrap_addr,
                secret,
                &opt.db_path,
                &cluster_config,
                env!("CARGO_PKG_VERSION"),
                Vec::new(), // compile_features filled in later via store_node_features
            )
            .await
            .expect("failed to join Raft cluster");

            Some(node)
        } else {
            None
        }
    };

    let (index_scheduler, auth_controller) = setup_meilisearch(&opt, runtime)?;

    let analytics =
        analytics::Analytics::new(&opt, index_scheduler.clone(), auth_controller.clone()).await;

    print_launch_resume(&opt, analytics.clone(), config_read_from);

    #[cfg(feature = "cluster")]
    let pre_joined: Option<Box<dyn std::any::Any + Send>> =
        pre_joined_node.map(|n| Box::new(n) as Box<dyn std::any::Any + Send>);
    #[cfg(not(feature = "cluster"))]
    let pre_joined: Option<Box<dyn std::any::Any + Send>> = None;

    run_http(index_scheduler, auth_controller, opt, log_handle, Arc::new(analytics), pre_joined).await?;

    Ok(())
}

async fn run_http(
    index_scheduler: Arc<IndexScheduler>,
    auth_controller: Arc<AuthController>,
    opt: Opt,
    logs: (LogRouteHandle, LogStderrHandle),
    analytics: Arc<Analytics>,
    #[allow(unused)] pre_joined: Option<Box<dyn std::any::Any + Send>>,
) -> anyhow::Result<()> {
    let enable_dashboard = &opt.env == "development";
    let opt_clone = opt.clone();

    #[cfg_attr(not(feature = "cluster"), allow(unused_mut))]
    let mut cluster_state = meilisearch::cluster::ClusterState::from_opts(&opt);

    // Phase 3: Raft cluster initialization (behind "cluster" feature flag)
    // Must happen BEFORE Data::from wrapping since we need Arc<IndexScheduler> access.
    #[cfg(feature = "cluster")]
    {
        /// Detect active compile-time features for cluster capability negotiation.
        fn detect_compile_features() -> Vec<String> {
            let mut features = Vec::new();
            if cfg!(feature = "enterprise") {
                features.push("enterprise".to_string());
            }
            if cfg!(feature = "mini-dashboard") {
                features.push("mini-dashboard".to_string());
            }
            if cfg!(feature = "chinese") {
                features.push("chinese".to_string());
            }
            if cfg!(feature = "chinese-pinyin") {
                features.push("chinese-pinyin".to_string());
            }
            if cfg!(feature = "hebrew") {
                features.push("hebrew".to_string());
            }
            if cfg!(feature = "japanese") {
                features.push("japanese".to_string());
            }
            if cfg!(feature = "korean") {
                features.push("korean".to_string());
            }
            if cfg!(feature = "thai") {
                features.push("thai".to_string());
            }
            if cfg!(feature = "greek") {
                features.push("greek".to_string());
            }
            if cfg!(feature = "khmer") {
                features.push("khmer".to_string());
            }
            if cfg!(feature = "vietnamese") {
                features.push("vietnamese".to_string());
            }
            if cfg!(feature = "swedish-recomposition") {
                features.push("swedish-recomposition".to_string());
            }
            if cfg!(feature = "german") {
                features.push("german".to_string());
            }
            if cfg!(feature = "turkish") {
                features.push("turkish".to_string());
            }
            // cluster feature is always present in this code path
            features.push("cluster".to_string());
            features
        }

        let compile_features = detect_compile_features();

        /// Adapter that implements `TaskProposer` by proposing tasks through Raft consensus.
        /// Task IDs are NOT pre-assigned — each node auto-assigns from its own LMDB.
        /// Raft's deterministic log order guarantees all nodes assign the same IDs.
        struct RaftTaskProposer {
            raft_node: Arc<meilisearch_cluster::ClusterNode>,
            runtime: tokio::runtime::Handle,
        }

        impl index_scheduler::TaskProposer for RaftTaskProposer {
            fn propose_task(
                &self,
                kind: &meilisearch_types::tasks::KindWithContent,
                content_file: Option<(uuid::Uuid, std::path::PathBuf)>,
            ) -> std::result::Result<
                index_scheduler::TaskId,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                // Send content file to followers via DML channel BEFORE proposing.
                if let Some((uuid, ref path)) = content_file {
                    tokio::task::block_in_place(|| {
                        self.runtime
                            .block_on(self.raft_node.send_file_to_followers(uuid, path))
                    })?;
                }

                // Use JSON instead of bincode because KindWithContent contains
                // Settings with #[serde(skip_serializing_if)] attributes that are
                // incompatible with bincode's positional format.
                let kind_bytes = serde_json::to_vec(kind)?;
                let request =
                    meilisearch_cluster::types::RaftRequest::TaskEnqueued { kind_bytes };
                // Block on the async Raft client_write from sync context.
                // Uses block_in_place since we're on a tokio worker thread (actix-web).
                let resp = tokio::task::block_in_place(|| {
                    self.runtime.block_on(self.raft_node.client_write(request))
                })
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    match e {
                        meilisearch_cluster::ClusterWriteError::NoLeader
                        | meilisearch_cluster::ClusterWriteError::NotLeader { .. } => {
                            Box::new(index_scheduler::Error::ClusterNoLeader)
                        }
                        meilisearch_cluster::ClusterWriteError::QuorumUnavailable => {
                            Box::new(index_scheduler::Error::ClusterQuorumUnavailable)
                        }
                        meilisearch_cluster::ClusterWriteError::Other(e) => {
                            Box::<dyn std::error::Error + Send + Sync>::from(e.to_string())
                        }
                    }
                })?;
                match resp.data {
                    meilisearch_cluster::types::RaftResponse::TaskRegistered { task_uid } => {
                        Ok(task_uid)
                    }
                    _ => Err("unexpected Raft response: expected TaskRegistered".into()),
                }
            }
        }

        /// Adapter that implements `TaskApplier` by calling IndexScheduler::register_from_raft.
        struct IndexSchedulerApplier {
            index_scheduler: Arc<IndexScheduler>,
        }

        impl meilisearch_cluster::task_applier::TaskApplier for IndexSchedulerApplier {
            fn apply_task(
                &self,
                kind_bytes: &[u8],
                raft_log_index: u64,
            ) -> std::result::Result<u32, Box<dyn std::error::Error + Send + Sync>> {
                let kind: meilisearch_types::tasks::KindWithContent =
                    serde_json::from_slice(kind_bytes)?;
                let task = self.index_scheduler.register_from_raft(kind, raft_log_index)?;
                Ok(task.uid)
            }

            fn missing_content_uuid(
                &self,
                kind_bytes: &[u8],
            ) -> Option<uuid::Uuid> {
                let kind: meilisearch_types::tasks::KindWithContent =
                    serde_json::from_slice(kind_bytes).ok()?;
                match kind {
                    meilisearch_types::tasks::KindWithContent::DocumentAdditionOrUpdate {
                        content_file,
                        ..
                    } => {
                        if self.index_scheduler.content_file_exists(content_file) {
                            None
                        } else {
                            Some(content_file)
                        }
                    }
                    _ => None,
                }
            }
        }

        /// Adapter that implements `ContentFileFetcher` by calling
        /// ClusterNode::request_content_file_from_leader.
        struct ClusterContentFileFetcher {
            raft_node: Arc<meilisearch_cluster::ClusterNode>,
        }

        impl meilisearch_cluster::task_applier::ContentFileFetcher for ClusterContentFileFetcher {
            fn fetch_content_file(
                &self,
                uuid: uuid::Uuid,
            ) -> meilisearch_cluster::task_applier::BoxFuture<
                '_,
                std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>,
            > {
                Box::pin(async move {
                    self.raft_node
                        .request_content_file_from_leader(uuid)
                        .await
                        .map_err(|e| {
                            let msg = e.to_string();
                            Box::<dyn std::error::Error + Send + Sync>::from(msg)
                        })
                })
            }
        }

        /// Convert `ClusterWriteError` to a boxed `AuthControllerError` so the
        /// auth controller can downcast it to the proper 503 variant.
        fn cluster_write_err_to_auth(
            e: meilisearch_cluster::ClusterWriteError,
        ) -> Box<dyn std::error::Error + Send + Sync> {
            use meilisearch_auth::error::AuthControllerError;
            match e {
                meilisearch_cluster::ClusterWriteError::NoLeader
                | meilisearch_cluster::ClusterWriteError::NotLeader { .. } => {
                    Box::new(AuthControllerError::ClusterNoLeader)
                }
                meilisearch_cluster::ClusterWriteError::QuorumUnavailable => {
                    Box::new(AuthControllerError::ClusterQuorumUnavailable)
                }
                meilisearch_cluster::ClusterWriteError::Other(e) => {
                    Box::<dyn std::error::Error + Send + Sync>::from(e.to_string())
                }
            }
        }

        /// Adapter that implements `KeyProposer` by proposing key operations through Raft.
        struct RaftKeyProposer {
            raft_node: Arc<meilisearch_cluster::ClusterNode>,
            runtime: tokio::runtime::Handle,
        }

        impl meilisearch_auth::KeyProposer for RaftKeyProposer {
            fn propose_key_put(
                &self,
                key: &meilisearch_types::keys::Key,
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let key_bytes = serde_json::to_vec(key)?;
                let request = meilisearch_cluster::types::RaftRequest::ApiKeyPut { key_bytes };
                tokio::task::block_in_place(|| {
                    self.runtime.block_on(self.raft_node.client_write(request))
                })
                .map_err(cluster_write_err_to_auth)?;
                Ok(())
            }

            fn propose_key_delete(
                &self,
                uid: uuid::Uuid,
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let uid_bytes = bincode::serialize(&uid)?;
                let request = meilisearch_cluster::types::RaftRequest::ApiKeyDelete { uid_bytes };
                tokio::task::block_in_place(|| {
                    self.runtime.block_on(self.raft_node.client_write(request))
                })
                .map_err(cluster_write_err_to_auth)?;
                Ok(())
            }
        }

        /// Adapter that implements `AuthApplier` by calling AuthController directly.
        struct AuthControllerApplier {
            auth_controller: Arc<AuthController>,
        }

        impl meilisearch_cluster::task_applier::AuthApplier for AuthControllerApplier {
            fn apply_key_put(
                &self,
                key_bytes: &[u8],
                raft_log_index: u64,
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let key: meilisearch_types::keys::Key = serde_json::from_slice(key_bytes)?;
                self.auth_controller.raw_insert_key(key, raft_log_index)?;
                Ok(())
            }

            fn apply_key_delete(
                &self,
                uid_bytes: &[u8],
                raft_log_index: u64,
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let uid: uuid::Uuid = bincode::deserialize(uid_bytes)?;
                self.auth_controller.raw_delete_key(uid, raft_log_index)?;
                Ok(())
            }

            fn snapshot_keys(
                &self,
            ) -> std::result::Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
                let keys = self.auth_controller.list_keys()?;
                keys.into_iter()
                    .map(|k| Ok(serde_json::to_vec(&k)?))
                    .collect()
            }

            fn install_snapshot_keys(
                &self,
                key_bytes_list: &[Vec<u8>],
                last_applied_log_index: u64,
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                self.auth_controller
                    .install_snapshot_keys(key_bytes_list, last_applied_log_index)?;
                Ok(())
            }
        }

        /// Adapter that implements `FeatureApplier` by calling IndexScheduler.
        struct IndexSchedulerFeatureApplier {
            index_scheduler: Arc<IndexScheduler>,
        }

        impl meilisearch_cluster::task_applier::FeatureApplier for IndexSchedulerFeatureApplier {
            fn apply_features(
                &self,
                features_json: &[u8],
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let features: meilisearch_types::features::RuntimeTogglableFeatures =
                    serde_json::from_slice(features_json)?;
                self.index_scheduler.put_runtime_features(features)?;
                Ok(())
            }

            fn snapshot_features(
                &self,
            ) -> std::result::Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>
            {
                let features = self.index_scheduler.features().runtime_features();
                if features == meilisearch_types::features::RuntimeTogglableFeatures::default() {
                    return Ok(None);
                }
                let json = serde_json::to_vec(&features)?;
                Ok(Some(json))
            }
        }

        /// Adapter that implements `LogLevelApplier` by modifying the stderr tracing layer.
        struct StderrLogLevelApplier {
            logs: meilisearch::LogStderrHandle,
        }

        impl meilisearch_cluster::task_applier::LogLevelApplier for StderrLogLevelApplier {
            fn apply_log_level(
                &self,
                target: &str,
            ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                use std::str::FromStr;
                let targets = tracing_subscriber::filter::Targets::from_str(target)?;
                self.logs
                    .modify(|layer| {
                        *layer.filter_mut() = targets;
                    })
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    })?;
                tracing::info!(target = target, "Log level updated via Raft replication");
                Ok(())
            }
        }

        /// Adapter that implements `SnapshotProvider` by delegating to
        /// `IndexScheduler::prepare_cluster_snapshot()`.
        struct IndexSchedulerSnapshotProvider {
            index_scheduler: Arc<IndexScheduler>,
        }

        impl meilisearch_cluster::snapshot::SnapshotProvider for IndexSchedulerSnapshotProvider {
            fn prepare_snapshot(&self, max_compaction_age_s: Option<u64>) -> anyhow::Result<tempfile::TempDir> {
                use meilisearch_types::heed::CompactionOption;
                let compaction = match max_compaction_age_s {
                    None => CompactionOption::Disabled,
                    Some(0) => CompactionOption::Enabled,
                    Some(max_age) => {
                        let last = self.index_scheduler.last_compaction_at();
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        if now.saturating_sub(last) > max_age {
                            tracing::info!(
                                last_compaction_secs_ago = now.saturating_sub(last),
                                max_age,
                                "Last compaction is stale, compacting before snapshot"
                            );
                            CompactionOption::Enabled
                        } else {
                            tracing::info!(
                                last_compaction_secs_ago = now.saturating_sub(last),
                                max_age,
                                "Recent compaction found, skipping pre-snapshot compaction"
                            );
                            CompactionOption::Disabled
                        }
                    }
                };
                self.index_scheduler
                    .prepare_cluster_snapshot(compaction)
                    .map_err(|e| anyhow::anyhow!("{e}"))
            }
        }

        /// Wire all appliers, proposers, background tasks, and return the Arc-wrapped node.
        ///
        /// This is the common wiring shared by create, join, and restart paths.
        fn wire_cluster_node(
            node: meilisearch_cluster::ClusterNode,
            index_scheduler: &Arc<IndexScheduler>,
            auth_controller: &Arc<AuthController>,
            logs: &(meilisearch::LogRouteHandle, meilisearch::LogStderrHandle),
            opt: &meilisearch::Opt,
        ) -> Arc<meilisearch_cluster::ClusterNode> {
            // Wire appliers: state machine → application layers
            node.state_machine.set_task_applier(Arc::new(IndexSchedulerApplier {
                index_scheduler: index_scheduler.clone(),
            }));
            node.state_machine.set_auth_applier(Arc::new(AuthControllerApplier {
                auth_controller: auth_controller.clone(),
            }));
            node.state_machine.set_feature_applier(Arc::new(IndexSchedulerFeatureApplier {
                index_scheduler: index_scheduler.clone(),
            }));
            node.state_machine
                .set_log_level_applier(Arc::new(StderrLogLevelApplier { logs: logs.1.clone() }));

            // Wire proposers: application layers → Raft
            let node = Arc::new(node);

            // Wire content file fetcher for missing file recovery during Raft replay
            node.state_machine.set_content_file_fetcher(Arc::new(ClusterContentFileFetcher {
                raft_node: node.clone(),
            }));

            index_scheduler.set_task_proposer(Arc::new(RaftTaskProposer {
                raft_node: node.clone(),
                runtime: tokio::runtime::Handle::current(),
            }));
            auth_controller.set_key_proposer(Arc::new(RaftKeyProposer {
                raft_node: node.clone(),
                runtime: tokio::runtime::Handle::current(),
            }));

            // Wire snapshot provider for consistent LMDB-based snapshot transfers
            node.set_snapshot_provider(Arc::new(IndexSchedulerSnapshotProvider {
                index_scheduler: index_scheduler.clone(),
            }));

            // Background tasks
            node.spawn_leader_watcher(
                index_scheduler.is_leader_flag(),
                index_scheduler.wake_up_signal(),
            );
            node.set_update_file_path(opt.db_path.join("update_files"));
            node.spawn_accept_loop();
            node.spawn_idle_peer_cleanup();
            node.spawn_lag_eviction();
            node.spawn_retained_file_cleanup();

            node
        }

        let cluster_config = meilisearch_cluster::ClusterConfig {
            heartbeat_ms: opt.cluster_heartbeat_ms,
            election_timeout_min_ms: opt.cluster_election_timeout_min_ms,
            election_timeout_max_ms: opt.cluster_election_timeout_max_ms,
            accept_timeout_ms: opt.cluster_accept_timeout_ms,
            max_message_size_mb: opt.cluster_max_message_size_mb,
            raft_db_size_mb: opt.cluster_raft_db_size_mb,
            max_transfer_failures: opt.cluster_max_transfer_failures,
            max_replication_lag: opt.cluster_max_replication_lag,
            write_timeout_secs: opt.cluster_write_timeout_secs,
            snapshot_max_compaction_age_s: opt.cluster_snapshot_max_compaction_age_s,
            tls: opt.cluster_tls,
        };

        if opt.cluster_create {
            let bind_addr: std::net::SocketAddr = opt
                .cluster_bind
                .as_deref()
                .unwrap_or("0.0.0.0:7701")
                .parse()
                .expect("invalid --cluster-bind address");

            // Determine the cluster secret: use explicit --cluster-secret if set,
            // otherwise derive deterministically from --master-key (if set).
            // If neither is set, create() will generate a random key.
            let (derived_secret, secret_source) = if let Some(ref explicit) = opt.cluster_secret {
                (Some(explicit.clone()), "explicit --cluster-secret")
            } else if let Some(ref mk) = opt.master_key {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(b"meili-cluster-secret:");
                hasher.update(mk.as_bytes());
                let derived = hasher.finalize().iter().map(|b| format!("{b:02x}")).collect::<String>();
                (Some(derived), "derived from --master-key")
            } else {
                (None, "randomly generated")
            };

            let quic_advertise = if let Some(ref cluster_addr) = opt.cluster_addr {
                format!("{}:{}", cluster_addr, bind_addr.port())
            } else {
                bind_addr.to_string()
            };
            if quic_advertise.starts_with("0.0.0.0:") || quic_advertise.starts_with("[::]:") {
                anyhow::bail!(
                    "--cluster-bind is {bind_addr}, which is not routable by peers. \
                     Set --cluster-addr to the hostname or IP other nodes can reach."
                );
            }
            let http_url = if let Some(ref cluster_addr) = opt.cluster_addr {
                let http_port = opt.http_addr.rsplit(':').next().unwrap_or("7700");
                format!("http://{}:{}", cluster_addr, http_port)
            } else {
                format!("http://{}", opt.http_addr)
            };
            let quic_advertise_display = quic_advertise.clone();
            let (node, cluster_key) = meilisearch_cluster::ClusterNode::create(
                opt.cluster_node_id,
                bind_addr,
                quic_advertise,
                http_url,
                &opt.db_path,
                &cluster_config,
                derived_secret,
            )
            .await
            .expect("failed to create Raft cluster");

            let http_url_display = format!("http://{}", opt.http_addr);
            eprintln!("=== CLUSTER CREATED ===");
            eprintln!("Cluster Key:  {cluster_key}");
            eprintln!("Key source:   {secret_source}");
            eprintln!("HTTP Address: {http_url_display}");
            eprintln!("QUIC Address: {quic_advertise_display}");
            eprintln!("Node ID:      {}", opt.cluster_node_id);
            eprintln!();
            if secret_source == "derived from --master-key" {
                eprintln!("Join command:");
                eprintln!("  meilisearch --cluster-join {quic_advertise_display} \\");
                eprintln!("    --master-key <same-master-key> \\");
                eprintln!("    --cluster-bind <host:port>");
                eprintln!();
                eprintln!("  (Nodes sharing the same --master-key auto-derive the cluster secret)");
                eprintln!("  (Node ID is auto-assigned by the leader)");
            } else {
                eprintln!("Join command:");
                eprintln!("  meilisearch --cluster-join {quic_advertise_display} \\");
                eprintln!("    --cluster-secret {cluster_key} \\");
                eprintln!("    --cluster-bind <host:port>");
                eprintln!();
                eprintln!("  (Node ID is auto-assigned by the leader)");
            }
            eprintln!("=======================");

            // Store this node's compile-time features for capability intersection
            node.store_node_features(opt.cluster_node_id, &compile_features);
            node.store_node_version(opt.cluster_node_id, env!("CARGO_PKG_VERSION"));
            node.store_node_protocols(opt.cluster_node_id, meilisearch_cluster::SUPPORTED_PROTOCOLS);

            let node = wire_cluster_node(
                node,
                &index_scheduler,
                &auth_controller,
                &logs,
                &opt,
            );
            cluster_state.set_raft_node(node);
            let advertise = if let Some(ref ca) = opt.cluster_addr {
                format!("{}:{}", ca, bind_addr.port())
            } else {
                bind_addr.to_string()
            };
            cluster_state.set_join_info(advertise, secret_source.to_string());
        } else if opt.cluster_join.is_some() {
            // The join handshake (including snapshot reception) already happened in
            // try_main() BEFORE setup_meilisearch(), so the IndexScheduler opened
            // the snapshot's LMDB files. Now just wire up the pre-joined node.
            let node: meilisearch_cluster::ClusterNode = *pre_joined
                .expect("pre-joined node must be set for --cluster-join")
                .downcast::<meilisearch_cluster::ClusterNode>()
                .expect("pre-joined node type mismatch");

            let actual_node_id = node.node_id;
            node.store_node_features(actual_node_id, &compile_features);
            node.store_node_version(actual_node_id, env!("CARGO_PKG_VERSION"));
            node.store_node_protocols(actual_node_id, meilisearch_cluster::SUPPORTED_PROTOCOLS);

            let bind_addr = opt
                .cluster_bind
                .as_deref()
                .unwrap_or("0.0.0.0:7701");
            let advertise = if let Some(ref ca) = opt.cluster_addr {
                let port = bind_addr.rsplit(':').next().unwrap_or("7701");
                format!("{}:{}", ca, port)
            } else {
                bind_addr.to_string()
            };

            let node = wire_cluster_node(
                node,
                &index_scheduler,
                &auth_controller,
                &logs,
                &opt,
            );
            cluster_state.set_raft_node(node);
            let join_secret_source = if opt.cluster_secret.is_some() {
                "explicit --cluster-secret"
            } else {
                "derived from --master-key"
            };
            cluster_state.set_join_info(advertise, join_secret_source.to_string());
        } else if meilisearch_cluster::has_persisted_cluster(&opt.db_path) {
            // Auto-restart: existing cluster LMDB found, no --cluster-create or --cluster-join.
            // Validate Raft log compatibility before attempting restart
            meilisearch_cluster::validate_raft_log_compatibility(
                &opt.db_path,
                cluster_config.raft_db_size_mb,
            )
            .expect(
                "Cluster Raft log incompatible with this binary — \
                 run with --cluster-reset to wipe cluster state",
            );

            let config = meilisearch_cluster::load_node_config(&opt.db_path)
                .expect("failed to load persisted cluster config")
                .expect(
                    "cluster LMDB exists but no node config found — \
                     was this cluster created before restart support was added?",
                );

            eprintln!("=== CLUSTER RESTART ===");
            eprintln!("Node ID:   {}", config.node_id);
            eprintln!("QUIC Bind: {}", config.bind_addr);
            eprintln!("=======================");

            let node = meilisearch_cluster::ClusterNode::restart(
                config.node_id,
                config.bind_addr,
                config.secret,
                &opt.db_path,
                &cluster_config,
            )
            .await
            .expect("failed to restart Raft cluster node");

            // Update this node's compile-time features (may have changed since last run)
            node.store_node_features(config.node_id, &compile_features);
            node.store_node_version(config.node_id, env!("CARGO_PKG_VERSION"));
            node.store_node_protocols(config.node_id, meilisearch_cluster::SUPPORTED_PROTOCOLS);

            let node = wire_cluster_node(
                node,
                &index_scheduler,
                &auth_controller,
                &logs,
                &opt,
            );
            cluster_state.set_raft_node(node);
            let advertise = if let Some(ref ca) = opt.cluster_addr {
                format!("{}:{}", ca, config.bind_addr.port())
            } else {
                config.bind_addr.to_string()
            };
            cluster_state.set_join_info(
                advertise,
                "persisted from previous run".to_string(),
            );
        }
    }

    let index_scheduler = Data::from(index_scheduler);
    let auth = Data::from(auth_controller);
    let analytics = Data::from(analytics);
    // Create personalization service with API key from options
    let personalization_service = Data::new(
        opt.experimental_personalization_api_key
            .clone()
            .map(|api_key| {
                PersonalizationService::cohere(api_key, index_scheduler.ip_policy().clone())
            })
            .unwrap_or_else(PersonalizationService::disabled),
    );
    let search_queue = SearchQueue::new(
        opt.experimental_search_queue_size,
        available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .checked_mul(opt.experimental_nb_searches_per_core)
            .unwrap_or(NonZeroUsize::MAX),
    )
    .with_time_to_abort(Duration::from_secs(
        usize::from(opt.experimental_drop_search_after) as u64
    ));
    let search_queue = Data::new(search_queue);
    let (logs_route_handle, logs_stderr_handle) = logs;
    let logs_route_handle = Data::new(logs_route_handle);
    let logs_stderr_handle = Data::new(logs_stderr_handle);

    // Capture the Raft node handle and leave_notify for graceful shutdown.
    #[cfg(feature = "cluster")]
    let shutdown_raft_node = cluster_state.raft_node.clone();
    #[cfg(not(feature = "cluster"))]
    let shutdown_raft_node: Option<()> = None;

    let leave_notify = cluster_state.leave_notify.clone();

    // Spawn shutdown handler that responds to both Ctrl+C and the HTTP leave endpoint.
    // Both paths use the same graceful shutdown logic to avoid process::exit() which
    // skips destructors and can corrupt LMDB mid-transaction.
    tokio::spawn(async move {
        // Use a loop so we can retry after a failed leave (e.g. last node in cluster).
        // On Ctrl+C or successful leave, we exit. On recoverable failure, we go back
        // to waiting for the next signal.
        loop {
            let is_ctrl_c;
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Ctrl+C received, initiating shutdown...");
                    is_ctrl_c = true;
                }
                _ = leave_notify.notified() => {
                    tracing::info!("Leave signal received from HTTP endpoint, initiating shutdown...");
                    is_ctrl_c = false;
                }
            }

            #[cfg(feature = "cluster")]
            if let Some(ref node) = shutdown_raft_node {
                if is_ctrl_c {
                    // Ctrl+C: just shut down without leaving the cluster
                    tracing::info!("Shutting down Raft engine (not leaving cluster)...");
                    let _ = node.shutdown().await;
                } else {
                    // HTTP leave: attempt graceful leave
                    tracing::info!("Attempting graceful leave (5s timeout)...");
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        node.leave(),
                    )
                    .await
                    {
                        Ok(Ok(())) => tracing::info!("Graceful leave completed"),
                        Ok(Err(e)) => {
                            let msg = e.to_string();
                            if msg.contains("last node") {
                                tracing::warn!("Cannot leave: last node in cluster. Node continues running.");
                                continue; // Go back to waiting for next signal
                            }
                            tracing::warn!("Graceful leave failed: {e}, forcing shutdown");
                            let _ = node.shutdown().await;
                        }
                        Err(_) => {
                            tracing::warn!("Graceful leave timed out, forcing shutdown");
                            let _ = node.shutdown().await;
                        }
                    }
                }
            }
            #[cfg(not(feature = "cluster"))]
            let _ = (&shutdown_raft_node, is_ctrl_c);
            std::process::exit(0);
        }
    });

    let cluster_state = Data::new(cluster_state);

    let services = ServicesData {
        index_scheduler,
        auth,
        search_queue,
        personalization_service,
        logs_route_handle,
        logs_stderr_handle,
        analytics,
        cluster_state,
    };

    let http_server =
        HttpServer::new(move || create_app(services.clone(), opt.clone(), enable_dashboard))
            // Disable signals allows the server to terminate immediately when a user enter CTRL-C
            .disable_signals()
            .keep_alive(KeepAlive::Os);

    if let Some(config) = opt_clone.get_ssl_config()? {
        http_server.bind_rustls_0_23(opt_clone.http_addr, config)?.run().await?;
    } else {
        http_server.bind(&opt_clone.http_addr)?.run().await?;
    }
    Ok(())
}

pub fn print_launch_resume(opt: &Opt, analytics: Analytics, config_read_from: Option<PathBuf>) {
    let build_info = build_info::BuildInfo::from_build();

    let protocol =
        if opt.ssl_cert_path.is_some() && opt.ssl_key_path.is_some() { "https" } else { "http" };
    let ascii_name = r#"
888b     d888          d8b 888 d8b                                            888
8888b   d8888          Y8P 888 Y8P                                            888
88888b.d88888              888                                                888
888Y88888P888  .d88b.  888 888 888 .d8888b   .d88b.   8888b.  888d888 .d8888b 88888b.
888 Y888P 888 d8P  Y8b 888 888 888 88K      d8P  Y8b     "88b 888P"  d88P"    888 "88b
888  Y8P  888 88888888 888 888 888 "Y8888b. 88888888 .d888888 888    888      888  888
888   "   888 Y8b.     888 888 888      X88 Y8b.     888  888 888    Y88b.    888  888
888       888  "Y8888  888 888 888  88888P'  "Y8888  "Y888888 888     "Y8888P 888  888
"#;

    eprintln!("{}", ascii_name);

    eprintln!(
        "Config file path:\t{:?}",
        config_read_from
            .map(|config_file_path| config_file_path.display().to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    eprintln!("Database path:\t\t{:?}", opt.db_path);
    eprintln!("Server listening on:\t\"{}://{}\"", protocol, opt.http_addr);
    eprintln!("Environment:\t\t{:?}", opt.env);
    eprintln!("Commit SHA:\t\t{:?}", build_info.commit_sha1.unwrap_or("unknown"));
    eprintln!(
        "Commit date:\t\t{:?}",
        build_info
            .commit_timestamp
            .and_then(|commit_timestamp| commit_timestamp
                .format(&time::format_description::well_known::Rfc3339)
                .ok())
            .unwrap_or("unknown".into())
    );
    eprintln!("Package version:\t{:?}", env!("CARGO_PKG_VERSION").to_string());
    if let Some(prototype) = build_info.describe.and_then(|describe| describe.as_prototype()) {
        eprintln!("Prototype:\t\t{:?}", prototype);
    }

    {
        if !opt.no_analytics {
            eprintln!(
                "
Thank you for using Meilisearch!

\nWe collect anonymized analytics to improve our product and your experience. To learn more, including how to turn off analytics, visit our dedicated documentation page: https://www.meilisearch.com/docs/learn/what_is_meilisearch/telemetry

Anonymous telemetry:\t\"Enabled\""
            );
        } else {
            eprintln!("Anonymous telemetry:\t\"Disabled\"");
        }
    }

    if let Some(instance_uid) = analytics.instance_uid() {
        eprintln!("Instance UID:\t\t\"{}\"", instance_uid);
    }

    eprintln!();

    match (opt.env.as_ref(), &opt.master_key) {
        ("production", Some(_)) => {
            eprintln!("A master key has been set. Requests to Meilisearch won't be authorized unless you provide an authentication key.");
        }
        ("development", Some(master_key)) => {
            eprintln!("A master key has been set. Requests to Meilisearch won't be authorized unless you provide an authentication key.");

            if master_key.len() < MASTER_KEY_MIN_SIZE {
                print_master_key_too_short_warning()
            }
        }
        ("development", None) => print_missing_master_key_warning(),
        // unreachable because Opt::try_build above would have failed already if any other value had been produced
        _ => unreachable!(),
    }

    eprintln!();
    eprintln!("Check out Meilisearch Cloud!\thttps://www.meilisearch.com/cloud?utm_campaign=oss&utm_source=engine&utm_medium=cli");
    eprintln!("Documentation:\t\t\thttps://www.meilisearch.com/docs");
    eprintln!("Source code:\t\t\thttps://github.com/meilisearch/meilisearch");
    eprintln!("Discord:\t\t\thttps://discord.meilisearch.com");
    eprintln!();
}

const WARNING_BG_COLOR: Option<Color> = Some(Color::Ansi256(178));
const WARNING_FG_COLOR: Option<Color> = Some(Color::Ansi256(0));

fn print_master_key_too_short_warning() {
    let choice = if stderr().is_terminal() { ColorChoice::Auto } else { ColorChoice::Never };
    let mut stderr = StandardStream::stderr(choice);
    stderr
        .set_color(
            ColorSpec::new().set_bg(WARNING_BG_COLOR).set_fg(WARNING_FG_COLOR).set_bold(true),
        )
        .unwrap();
    writeln!(stderr, "\n").unwrap();
    writeln!(
        stderr,
        " Meilisearch started with a master key considered unsafe for use in a production environment.

 A master key of at least {MASTER_KEY_MIN_SIZE} bytes will be required when switching to a production environment."
    )
    .unwrap();
    stderr.reset().unwrap();
    writeln!(stderr).unwrap();

    eprintln!("\n{}", generated_master_key_message());
    eprintln!(
        "\nRestart Meilisearch with the argument above to use this new and secure master key."
    )
}

fn print_missing_master_key_warning() {
    let choice = if stderr().is_terminal() { ColorChoice::Auto } else { ColorChoice::Never };
    let mut stderr = StandardStream::stderr(choice);
    stderr
        .set_color(
            ColorSpec::new().set_bg(WARNING_BG_COLOR).set_fg(WARNING_FG_COLOR).set_bold(true),
        )
        .unwrap();
    writeln!(stderr, "\n").unwrap();
    writeln!(
    stderr,
    " No master key was found. The server will accept unidentified requests.

 A master key of at least {MASTER_KEY_MIN_SIZE} bytes will be required when switching to a production environment."
)
.unwrap();
    stderr.reset().unwrap();
    writeln!(stderr).unwrap();

    eprintln!("\n{}", generated_master_key_message());
    eprintln!(
        "\nRestart Meilisearch with the argument above to use this new and secure master key."
    )
}

fn generated_master_key_message() -> String {
    format!(
        "We generated a new secure master key for you (you can safely use this token):

>> --master-key {} <<",
        generate_master_key()
    )
}
