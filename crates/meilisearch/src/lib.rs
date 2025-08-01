#![allow(rustdoc::private_intra_doc_links)]
#[macro_use]
pub mod error;
pub mod analytics;
#[macro_use]
pub mod extractors;
pub mod metrics;
pub mod middleware;
pub mod option;
#[cfg(test)]
mod option_test;
pub mod routes;
pub mod search;
pub mod search_queue;

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use actix_cors::Cors;
use actix_http::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceResponse};
use actix_web::error::JsonPayloadError;
use actix_web::http::header::{CONTENT_TYPE, USER_AGENT};
use actix_web::web::Data;
use actix_web::{web, HttpRequest};
use analytics::Analytics;
use anyhow::bail;
use error::PayloadError;
use extractors::payload::PayloadConfig;
use index_scheduler::versioning::Versioning;
use index_scheduler::{IndexScheduler, IndexSchedulerOptions};
use meilisearch_auth::{open_auth_store_env, AuthController};
use meilisearch_types::milli::constants::VERSION_MAJOR;
use meilisearch_types::milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use meilisearch_types::milli::progress::{EmbedderStats, Progress};
use meilisearch_types::milli::update::{
    default_thread_pool_and_threads, IndexDocumentsConfig, IndexDocumentsMethod, IndexerConfig,
};
use meilisearch_types::settings::apply_settings_to_builder;
use meilisearch_types::tasks::KindWithContent;
use meilisearch_types::versioning::{
    create_current_version_file, get_version, VersionFileError, VERSION_MINOR, VERSION_PATCH,
};
use meilisearch_types::{compression, heed, milli, VERSION_FILE_NAME};
pub use option::Opt;
use option::ScheduleSnapshot;
use search_queue::SearchQueue;
use tracing::{error, info_span};
use tracing_subscriber::filter::Targets;

use crate::error::MeilisearchHttpError;

/// Default number of simultaneously opened indexes.
///
/// This value is used when dynamic computation of how many indexes can be opened at once was skipped (e.g., in tests).
///
/// Lower for Windows that dedicates a smaller virtual address space to processes.
///
/// The value was chosen this way:
///
/// - Windows provides a small virtual address space of about 10TiB to processes.
/// - The chosen value allows for indexes to use the default map size of 2TiB safely.
#[cfg(windows)]
const DEFAULT_INDEX_COUNT: usize = 4;

/// Default number of simultaneously opened indexes.
///
/// This value is used when dynamic computation of how many indexes can be opened at once was skipped (e.g., in tests).
///
/// The higher, the better for avoiding reopening indexes.
///
/// The value was chosen this way:
///
/// - Opening an index consumes a file descriptor.
/// - The default on many unices is about 256 file descriptors for a process.
/// - 100 is a little bit less than half this value.
/// - The chosen value allows for indexes to use the default map size of 2TiB safely.
#[cfg(not(windows))]
const DEFAULT_INDEX_COUNT: usize = 20;

/// Check if a db is empty. It does not provide any information on the
/// validity of the data in it.
/// We consider a database as non empty when it's a non empty directory.
fn is_empty_db(db_path: impl AsRef<Path>) -> bool {
    let db_path = db_path.as_ref();

    if !db_path.exists() {
        true
    // if we encounter an error or if the db is a file we consider the db non empty
    } else if let Ok(dir) = db_path.read_dir() {
        dir.count() == 0
    } else {
        true
    }
}

/// The handle used to update the logs at runtime. Must be accessible from the `main.rs` and the `route/logs.rs`.
pub type LogRouteHandle =
    tracing_subscriber::reload::Handle<LogRouteType, tracing_subscriber::Registry>;

pub type LogRouteType = tracing_subscriber::filter::Filtered<
    Option<Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>>,
    Targets,
    tracing_subscriber::Registry,
>;

pub type SubscriberForSecondLayer = tracing_subscriber::layer::Layered<
    tracing_subscriber::reload::Layer<LogRouteType, tracing_subscriber::Registry>,
    tracing_subscriber::Registry,
>;

pub type LogStderrHandle =
    tracing_subscriber::reload::Handle<LogStderrType, SubscriberForSecondLayer>;

pub type LogStderrType = tracing_subscriber::filter::Filtered<
    Box<dyn tracing_subscriber::Layer<SubscriberForSecondLayer> + Send + Sync>,
    Targets,
    SubscriberForSecondLayer,
>;

pub fn create_app(
    index_scheduler: Data<IndexScheduler>,
    auth_controller: Data<AuthController>,
    search_queue: Data<SearchQueue>,
    opt: Opt,
    logs: (LogRouteHandle, LogStderrHandle),
    analytics: Data<Analytics>,
    enable_dashboard: bool,
) -> actix_web::App<
    impl ServiceFactory<
        actix_web::dev::ServiceRequest,
        Config = (),
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
        InitError = (),
    >,
> {
    let app = actix_web::App::new()
        .configure(|s| {
            configure_data(
                s,
                index_scheduler.clone(),
                auth_controller.clone(),
                search_queue.clone(),
                &opt,
                logs,
                analytics.clone(),
            )
        })
        .configure(routes::configure)
        .configure(|s| dashboard(s, enable_dashboard));

    let app = app.wrap(middleware::RouteMetrics);
    app.wrap(
        Cors::default()
            .send_wildcard()
            .allow_any_header()
            .allow_any_origin()
            .allow_any_method()
            .max_age(86_400), // 24h
    )
    .wrap(tracing_actix_web::TracingLogger::<AwebTracingLogger>::new())
    .wrap(actix_web::middleware::Compress::default())
    .wrap(actix_web::middleware::NormalizePath::new(actix_web::middleware::TrailingSlash::Trim))
}

struct AwebTracingLogger;

impl tracing_actix_web::RootSpanBuilder for AwebTracingLogger {
    fn on_request_start(request: &actix_web::dev::ServiceRequest) -> tracing::Span {
        use tracing::field::Empty;

        let conn_info = request.connection_info();
        let headers = request.headers();
        let user_agent = headers
            .get(USER_AGENT)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
            .unwrap_or_default();
        info_span!("HTTP request", method = %request.method(), host = conn_info.host(), route = %request.path(), query_parameters = %request.query_string(), %user_agent, status_code = Empty, error = Empty)
    }

    fn on_request_end<B: MessageBody>(
        span: tracing::Span,
        outcome: &Result<ServiceResponse<B>, actix_web::Error>,
    ) {
        match &outcome {
            Ok(response) => {
                let code: i32 = response.response().status().as_u16().into();
                span.record("status_code", code);

                if let Some(error) = response.response().error() {
                    // use the status code already constructed for the outgoing HTTP response
                    span.record("error", tracing::field::display(error.as_response_error()));
                }
            }
            Err(error) => {
                let code: i32 = error.error_response().status().as_u16().into();
                span.record("status_code", code);
                span.record("error", tracing::field::display(error.as_response_error()));
            }
        };
    }
}

enum OnFailure {
    RemoveDb,
    KeepDb,
}

pub fn setup_meilisearch(opt: &Opt) -> anyhow::Result<(Arc<IndexScheduler>, Arc<AuthController>)> {
    let index_scheduler_opt = IndexSchedulerOptions {
        version_file_path: opt.db_path.join(VERSION_FILE_NAME),
        auth_path: opt.db_path.join("auth"),
        tasks_path: opt.db_path.join("tasks"),
        update_file_path: opt.db_path.join("update_files"),
        indexes_path: opt.db_path.join("indexes"),
        snapshots_path: opt.snapshot_dir.clone(),
        dumps_path: opt.dump_dir.clone(),
        webhook_url: opt.task_webhook_url.as_ref().map(|url| url.to_string()),
        webhook_authorization_header: opt.task_webhook_authorization_header.clone(),
        task_db_size: opt.max_task_db_size.as_u64() as usize,
        index_base_map_size: opt.max_index_size.as_u64() as usize,
        enable_mdb_writemap: opt.experimental_reduce_indexing_memory_usage,
        indexer_config: Arc::new((&opt.indexer_options).try_into()?),
        autobatching_enabled: true,
        cleanup_enabled: !opt.experimental_replication_parameters,
        max_number_of_tasks: 1_000_000,
        max_number_of_batched_tasks: opt.experimental_max_number_of_batched_tasks,
        batched_tasks_size_limit: opt.experimental_limit_batched_tasks_total_size.into(),
        index_growth_amount: byte_unit::Byte::from_str("10GiB").unwrap().as_u64() as usize,
        index_count: DEFAULT_INDEX_COUNT,
        instance_features: opt.to_instance_features(),
        auto_upgrade: opt.experimental_dumpless_upgrade,
        embedding_cache_cap: opt.experimental_embedding_cache_entries,
        experimental_no_snapshot_compaction: opt.experimental_no_snapshot_compaction,
    };
    let binary_version = (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);

    let empty_db = is_empty_db(&opt.db_path);
    let (index_scheduler, auth_controller) = if let Some(ref snapshot_path) = opt.import_snapshot {
        let snapshot_path_exists = snapshot_path.exists();
        // the db is empty and the snapshot exists, import it
        if empty_db && snapshot_path_exists {
            match compression::from_tar_gz(snapshot_path, &opt.db_path) {
                Ok(()) => open_or_create_database_unchecked(
                    opt,
                    index_scheduler_opt,
                    OnFailure::RemoveDb,
                    binary_version, // the db is empty
                )?,
                Err(e) => {
                    std::fs::remove_dir_all(&opt.db_path)?;
                    return Err(e);
                }
            }
        // the db already exists and we should not ignore the snapshot => throw an error
        } else if !empty_db && !opt.ignore_snapshot_if_db_exists {
            bail!(
                "database already exists at {:?}, try to delete it or rename it",
                opt.db_path.canonicalize().unwrap_or_else(|_| opt.db_path.to_owned())
            )
        // the snapshot doesn't exist and we can't ignore it => throw an error
        } else if !snapshot_path_exists && !opt.ignore_missing_snapshot {
            bail!("snapshot doesn't exist at {}", snapshot_path.display())
        // the snapshot and the db exist, and we can ignore the snapshot because of the ignore_snapshot_if_db_exists flag
        } else {
            open_or_create_database(opt, index_scheduler_opt, empty_db, binary_version)?
        }
    } else if let Some(ref path) = opt.import_dump {
        let src_path_exists = path.exists();
        // the db is empty and the dump exists, import it
        if empty_db && src_path_exists {
            let (mut index_scheduler, mut auth_controller) = open_or_create_database_unchecked(
                opt,
                index_scheduler_opt,
                OnFailure::RemoveDb,
                binary_version, // the db is empty
            )?;
            match import_dump(&opt.db_path, path, &mut index_scheduler, &mut auth_controller) {
                Ok(()) => (index_scheduler, auth_controller),
                Err(e) => {
                    std::fs::remove_dir_all(&opt.db_path)?;
                    return Err(e);
                }
            }
        // the db already exists and we should not ignore the dump option => throw an error
        } else if !empty_db && !opt.ignore_dump_if_db_exists {
            bail!(
                "database already exists at {:?}, try to delete it or rename it",
                opt.db_path.canonicalize().unwrap_or_else(|_| opt.db_path.to_owned())
            )
        // the dump doesn't exist and we can't ignore it => throw an error
        } else if !src_path_exists && !opt.ignore_missing_dump {
            bail!("dump doesn't exist at {:?}", path)
        // the dump and the db exist and we can ignore the dump because of the ignore_dump_if_db_exists flag
        // or, the dump is missing but we can ignore that because of the ignore_missing_dump flag
        } else {
            open_or_create_database(opt, index_scheduler_opt, empty_db, binary_version)?
        }
    } else {
        open_or_create_database(opt, index_scheduler_opt, empty_db, binary_version)?
    };

    // We create a loop in a thread that registers snapshotCreation tasks
    let index_scheduler = Arc::new(index_scheduler);
    let auth_controller = Arc::new(auth_controller);
    if let ScheduleSnapshot::Enabled(snapshot_delay) = opt.schedule_snapshot {
        let snapshot_delay = Duration::from_secs(snapshot_delay);
        let index_scheduler = index_scheduler.clone();
        thread::Builder::new()
            .name(String::from("register-snapshot-tasks"))
            .spawn(move || loop {
                thread::sleep(snapshot_delay);
                if let Err(e) =
                    index_scheduler.register(KindWithContent::SnapshotCreation, None, false)
                {
                    error!("Error while registering snapshot: {}", e);
                }
            })
            .unwrap();
    }

    Ok((index_scheduler, auth_controller))
}

/// Try to start the IndexScheduler and AuthController without checking the VERSION file or anything.
fn open_or_create_database_unchecked(
    opt: &Opt,
    index_scheduler_opt: IndexSchedulerOptions,
    on_failure: OnFailure,
    version: (u32, u32, u32),
) -> anyhow::Result<(IndexScheduler, AuthController)> {
    // we don't want to create anything in the data.ms yet, thus we
    // wrap our two builders in a closure that'll be executed later.
    std::fs::create_dir_all(&index_scheduler_opt.auth_path)?;
    let auth_env = open_auth_store_env(&index_scheduler_opt.auth_path).unwrap();
    let auth_controller = AuthController::new(auth_env.clone(), &opt.master_key);
    let index_scheduler_builder = || -> anyhow::Result<_> {
        Ok(IndexScheduler::new(index_scheduler_opt, auth_env, version)?)
    };

    match (
        index_scheduler_builder(),
        auth_controller.map_err(anyhow::Error::from),
        create_current_version_file(&opt.db_path),
    ) {
        (Ok(i), Ok(a), Ok(())) => Ok((i, a)),
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
            if matches!(on_failure, OnFailure::RemoveDb) {
                std::fs::remove_dir_all(&opt.db_path)?;
            }
            Err(e)
        }
    }
}

/// Ensures Meilisearch version is compatible with the database, returns an error in case of version mismatch.
/// Returns the version that was contained in the version file
fn check_version(
    opt: &Opt,
    index_scheduler_opt: &IndexSchedulerOptions,
    binary_version: (u32, u32, u32),
) -> anyhow::Result<(u32, u32, u32)> {
    let (bin_major, bin_minor, bin_patch) = binary_version;
    let (db_major, db_minor, db_patch) = get_version(&opt.db_path)?;

    if db_major != bin_major || db_minor != bin_minor || db_patch != bin_patch {
        if opt.experimental_dumpless_upgrade {
            update_version_file_for_dumpless_upgrade(
                opt,
                index_scheduler_opt,
                (db_major, db_minor, db_patch),
                (bin_major, bin_minor, bin_patch),
            )?;
        } else {
            return Err(VersionFileError::VersionMismatch {
                major: db_major,
                minor: db_minor,
                patch: db_patch,
            }
            .into());
        }
    }

    Ok((db_major, db_minor, db_patch))
}

/// Persists the version of the current Meilisearch binary to a VERSION file
pub fn update_version_file_for_dumpless_upgrade(
    opt: &Opt,
    index_scheduler_opt: &IndexSchedulerOptions,
    from: (u32, u32, u32),
    to: (u32, u32, u32),
) -> Result<(), VersionFileError> {
    let (from_major, from_minor, from_patch) = from;
    let (to_major, to_minor, to_patch) = to;

    // Early exit in case of error
    if from_major > to_major
        || (from_major == to_major && from_minor > to_minor)
        || (from_major == to_major && from_minor == to_minor && from_patch > to_patch)
    {
        return Err(VersionFileError::DowngradeNotSupported {
            major: from_major,
            minor: from_minor,
            patch: from_patch,
        });
    } else if from_major < 1 || (from_major == to_major && from_minor < 12) {
        return Err(VersionFileError::TooOldForAutomaticUpgrade {
            major: from_major,
            minor: from_minor,
            patch: from_patch,
        });
    }

    // In the case of v1.12, the index-scheduler didn't store its internal version at the time.
    // => We must write it immediately **in the index-scheduler** otherwise we'll update the version file
    //    there is a risk of DB corruption if a restart happens after writing the version file but before
    //    writing the version in the index-scheduler. See <https://github.com/meilisearch/meilisearch/issues/5280>
    if from_major == 1 && from_minor == 12 {
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .max_dbs(Versioning::nb_db())
                .map_size(index_scheduler_opt.task_db_size)
                .open(&index_scheduler_opt.tasks_path)
        }?;
        let mut wtxn = env.write_txn()?;
        let versioning = Versioning::raw_new(&env, &mut wtxn)?;
        versioning.set_version(&mut wtxn, (from_major, from_minor, from_patch))?;
        wtxn.commit()?;
        // Should be instant since we're the only one using the env
        env.prepare_for_closing().wait();
    }

    create_current_version_file(&opt.db_path)?;
    Ok(())
}

/// Ensure you're in a valid state and open the IndexScheduler + AuthController for you.
fn open_or_create_database(
    opt: &Opt,
    index_scheduler_opt: IndexSchedulerOptions,
    empty_db: bool,
    binary_version: (u32, u32, u32),
) -> anyhow::Result<(IndexScheduler, AuthController)> {
    let version = if !empty_db {
        check_version(opt, &index_scheduler_opt, binary_version)?
    } else {
        binary_version
    };

    open_or_create_database_unchecked(opt, index_scheduler_opt, OnFailure::KeepDb, version)
}

fn import_dump(
    db_path: &Path,
    dump_path: &Path,
    index_scheduler: &mut IndexScheduler,
    auth: &mut AuthController,
) -> Result<(), anyhow::Error> {
    let progress = Progress::default();
    let reader = File::open(dump_path)?;
    let mut dump_reader = dump::DumpReader::open(reader)?;

    if let Some(date) = dump_reader.date() {
        tracing::info!(
            version = ?dump_reader.version(), // TODO: get the meilisearch version instead of the dump version
            %date,
            "Importing a dump of meilisearch"
        );
    } else {
        tracing::info!(
            version = ?dump_reader.version(), // TODO: get the meilisearch version instead of the dump version
            "Importing a dump of meilisearch",
        );
    }

    let instance_uid = dump_reader.instance_uid()?;

    // 1. Import the instance-uid.
    if let Some(ref instance_uid) = instance_uid {
        // we don't want to panic if there is an error with the instance-uid.
        let _ = std::fs::write(db_path.join("instance-uid"), instance_uid.to_string().as_bytes());
    };

    // 2. Import the `Key`s.
    let mut keys = Vec::new();
    auth.raw_delete_all_keys()?;
    for key in dump_reader.keys()? {
        let key = key?;
        auth.raw_insert_key(key.clone())?;
        keys.push(key);
    }

    // 3. Import the `ChatCompletionSettings`s.
    for result in dump_reader.chat_completions_settings()? {
        let (name, settings) = result?;
        index_scheduler.put_chat_settings(&name, &settings)?;
    }

    // 4. Import the runtime features and network
    let features = dump_reader.features()?.unwrap_or_default();
    index_scheduler.put_runtime_features(features)?;

    let network = dump_reader.network()?.cloned().unwrap_or_default();
    index_scheduler.put_network(network)?;

    // 4.1 Use all cpus to process dump if `max_indexing_threads` not configured
    let backup_config;
    let base_config = index_scheduler.indexer_config();

    let indexer_config = if base_config.max_threads.is_none() {
        let (thread_pool, _) = default_thread_pool_and_threads();

        let _config = IndexerConfig { thread_pool, ..*base_config };
        backup_config = _config;
        &backup_config
    } else {
        base_config
    };

    // /!\ The tasks must be imported AFTER importing the indexes or else the scheduler might
    // try to process tasks while we're trying to import the indexes.

    // 5. Import the indexes.
    for index_reader in dump_reader.indexes()? {
        let mut index_reader = index_reader?;
        let metadata = index_reader.metadata();
        let uid = metadata.uid.clone();
        tracing::info!("Importing index `{}`.", metadata.uid);

        let date = Some((metadata.created_at, metadata.updated_at));
        let index = index_scheduler.create_raw_index(&metadata.uid, date)?;

        let mut wtxn = index.write_txn()?;

        let mut builder = milli::update::Settings::new(&mut wtxn, &index, indexer_config);
        // 5.1 Import the primary key if there is one.
        if let Some(ref primary_key) = metadata.primary_key {
            builder.set_primary_key(primary_key.to_string());
        }

        // 5.2 Import the settings.
        tracing::info!("Importing the settings.");
        let settings = index_reader.settings()?;
        apply_settings_to_builder(&settings, &mut builder);
        let embedder_stats: Arc<EmbedderStats> = Default::default();
        builder.execute(&|| false, &progress, embedder_stats.clone())?;

        // 5.3 Import the documents.
        // 5.3.1 We need to recreate the grenad+obkv format accepted by the index.
        tracing::info!("Importing the documents.");
        let file = tempfile::tempfile()?;
        let mut builder = DocumentsBatchBuilder::new(BufWriter::new(file));
        for document in index_reader.documents()? {
            builder.append_json_object(&document?)?;
        }

        // This flush the content of the batch builder.
        let file = builder.into_inner()?.into_inner()?;

        // 5.3.2 We feed it to the milli index.
        let reader = BufReader::new(file);
        let reader = DocumentsBatchReader::from_reader(reader)?;

        let embedder_configs = index.embedding_configs().embedding_configs(&wtxn)?;
        let embedders = index_scheduler.embedders(uid.to_string(), embedder_configs)?;

        let builder = milli::update::IndexDocuments::new(
            &mut wtxn,
            &index,
            indexer_config,
            IndexDocumentsConfig {
                update_method: IndexDocumentsMethod::ReplaceDocuments,
                ..Default::default()
            },
            |indexing_step| tracing::trace!("update: {:?}", indexing_step),
            || false,
            &embedder_stats,
        )?;

        let builder = builder.with_embedders(embedders);

        let (builder, user_result) = builder.add_documents(reader)?;
        let user_result = user_result?;
        tracing::info!(documents_found = user_result, "{} documents found.", user_result);
        builder.execute()?;
        wtxn.commit()?;
        tracing::info!("All documents successfully imported.");

        index_scheduler.refresh_index_stats(&uid)?;
    }

    // 6. Import the queue
    let mut index_scheduler_dump = index_scheduler.register_dumped_task()?;
    // 6.1. Import the batches
    for ret in dump_reader.batches()? {
        let batch = ret?;
        index_scheduler_dump.register_dumped_batch(batch)?;
    }

    // 6.2. Import the tasks
    for ret in dump_reader.tasks()? {
        let (task, file) = ret?;
        index_scheduler_dump.register_dumped_task(task, file)?;
    }
    Ok(index_scheduler_dump.finish()?)
}

pub fn configure_data(
    config: &mut web::ServiceConfig,
    index_scheduler: Data<IndexScheduler>,
    auth: Data<AuthController>,
    search_queue: Data<SearchQueue>,
    opt: &Opt,
    (logs_route, logs_stderr): (LogRouteHandle, LogStderrHandle),
    analytics: Data<Analytics>,
) {
    let http_payload_size_limit = opt.http_payload_size_limit.as_u64() as usize;
    config
        .app_data(index_scheduler)
        .app_data(auth)
        .app_data(search_queue)
        .app_data(analytics)
        .app_data(web::Data::new(logs_route))
        .app_data(web::Data::new(logs_stderr))
        .app_data(web::Data::new(opt.clone()))
        .app_data(
            web::JsonConfig::default()
                .limit(http_payload_size_limit)
                .content_type(|mime| mime == mime::APPLICATION_JSON)
                .error_handler(|err, req: &HttpRequest| match err {
                    JsonPayloadError::ContentType => match req.headers().get(CONTENT_TYPE) {
                        Some(content_type) => MeilisearchHttpError::InvalidContentType(
                            content_type.to_str().unwrap_or("unknown").to_string(),
                            vec![mime::APPLICATION_JSON.to_string()],
                        )
                        .into(),
                        None => MeilisearchHttpError::MissingContentType(vec![
                            mime::APPLICATION_JSON.to_string(),
                        ])
                        .into(),
                    },
                    err => PayloadError::from(err).into(),
                }),
        )
        .app_data(PayloadConfig::new(http_payload_size_limit))
        .app_data(
            web::QueryConfig::default().error_handler(|err, _req| PayloadError::from(err).into()),
        );
}

#[cfg(feature = "mini-dashboard")]
pub fn dashboard(config: &mut web::ServiceConfig, enable_frontend: bool) {
    use actix_web::HttpResponse;
    use static_files::Resource;

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    if enable_frontend {
        let generated = generated::generate();
        // Generate routes for mini-dashboard assets
        for (path, resource) in generated.into_iter() {
            let Resource { mime_type, data, .. } = resource;
            // Redirect index.html to /
            if path == "index.html" {
                config.service(web::resource("/").route(web::get().to(move || async move {
                    HttpResponse::Ok().content_type(mime_type).body(data)
                })));
            } else {
                config.service(web::resource(path).route(web::get().to(move || async move {
                    HttpResponse::Ok().content_type(mime_type).body(data)
                })));
            }
        }
    } else {
        config.service(web::resource("/").route(web::get().to(routes::running)));
    }
}

#[cfg(not(feature = "mini-dashboard"))]
pub fn dashboard(config: &mut web::ServiceConfig, _enable_frontend: bool) {
    config.service(web::resource("/").route(web::get().to(routes::running)));
}
