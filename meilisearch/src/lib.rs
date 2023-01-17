#![allow(rustdoc::private_intra_doc_links)]
#[macro_use]
pub mod error;
pub mod analytics;
#[macro_use]
pub mod extractors;
pub mod option;
pub mod routes;
pub mod search;

#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "metrics")]
pub mod route_metrics;

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use actix_cors::Cors;
use actix_http::body::MessageBody;
use actix_web::dev::{ServiceFactory, ServiceResponse};
use actix_web::error::JsonPayloadError;
use actix_web::web::Data;
use actix_web::{middleware, web, HttpRequest};
use analytics::Analytics;
use anyhow::bail;
use error::PayloadError;
use extractors::payload::PayloadConfig;
use http::header::CONTENT_TYPE;
use index_scheduler::{IndexScheduler, IndexSchedulerOptions};
use log::error;
use meilisearch_auth::AuthController;
use meilisearch_types::milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use meilisearch_types::milli::update::{IndexDocumentsConfig, IndexDocumentsMethod};
use meilisearch_types::settings::apply_settings_to_builder;
use meilisearch_types::tasks::KindWithContent;
use meilisearch_types::versioning::{check_version_file, create_version_file};
use meilisearch_types::{compression, milli, VERSION_FILE_NAME};
pub use option::Opt;
use option::ScheduleSnapshot;

use crate::error::MeilisearchHttpError;

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

pub fn create_app(
    index_scheduler: Data<IndexScheduler>,
    auth_controller: AuthController,
    opt: Opt,
    analytics: Arc<dyn Analytics>,
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
                &opt,
                analytics.clone(),
            )
        })
        .configure(routes::configure)
        .configure(|s| dashboard(s, enable_dashboard));
    #[cfg(feature = "metrics")]
    let app = app.configure(|s| configure_metrics_route(s, opt.enable_metrics_route));

    #[cfg(feature = "metrics")]
    let app = app.wrap(Condition::new(opt.enable_metrics_route, route_metrics::RouteMetrics));
    app.wrap(
        Cors::default()
            .send_wildcard()
            .allow_any_header()
            .allow_any_origin()
            .allow_any_method()
            .max_age(86_400), // 24h
    )
    .wrap(middleware::Logger::default())
    .wrap(middleware::Compress::default())
    .wrap(middleware::NormalizePath::new(middleware::TrailingSlash::Trim))
}

enum OnFailure {
    RemoveDb,
    KeepDb,
}

pub fn setup_meilisearch(opt: &Opt) -> anyhow::Result<(Arc<IndexScheduler>, AuthController)> {
    let empty_db = is_empty_db(&opt.db_path);
    let (index_scheduler, auth_controller) = if let Some(ref snapshot_path) = opt.import_snapshot {
        let snapshot_path_exists = snapshot_path.exists();
        // the db is empty and the snapshot exists, import it
        if empty_db && snapshot_path_exists {
            match compression::from_tar_gz(snapshot_path, &opt.db_path) {
                Ok(()) => open_or_create_database_unchecked(opt, OnFailure::RemoveDb)?,
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
            open_or_create_database(opt, empty_db)?
        }
    } else if let Some(ref path) = opt.import_dump {
        let src_path_exists = path.exists();
        // the db is empty and the dump exists, import it
        if empty_db && src_path_exists {
            let (mut index_scheduler, mut auth_controller) =
                open_or_create_database_unchecked(opt, OnFailure::RemoveDb)?;
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
            open_or_create_database(opt, empty_db)?
        }
    } else {
        open_or_create_database(opt, empty_db)?
    };

    // We create a loop in a thread that registers snapshotCreation tasks
    let index_scheduler = Arc::new(index_scheduler);
    if let ScheduleSnapshot::Enabled(snapshot_delay) = opt.schedule_snapshot {
        let snapshot_delay = Duration::from_secs(snapshot_delay);
        let index_scheduler = index_scheduler.clone();
        thread::Builder::new()
            .name(String::from("register-snapshot-tasks"))
            .spawn(move || loop {
                thread::sleep(snapshot_delay);
                if let Err(e) = index_scheduler.register(KindWithContent::SnapshotCreation) {
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
    on_failure: OnFailure,
) -> anyhow::Result<(IndexScheduler, AuthController)> {
    // we don't want to create anything in the data.ms yet, thus we
    // wrap our two builders in a closure that'll be executed later.
    let auth_controller = AuthController::new(&opt.db_path, &opt.master_key);
    let index_scheduler_builder = || -> anyhow::Result<_> {
        Ok(IndexScheduler::new(IndexSchedulerOptions {
            version_file_path: opt.db_path.join(VERSION_FILE_NAME),
            auth_path: opt.db_path.join("auth"),
            tasks_path: opt.db_path.join("tasks"),
            update_file_path: opt.db_path.join("update_files"),
            indexes_path: opt.db_path.join("indexes"),
            snapshots_path: opt.snapshot_dir.clone(),
            dumps_path: opt.dump_dir.clone(),
            task_db_size: opt.max_task_db_size.get_bytes() as usize,
            index_base_map_size: opt.max_index_size.get_bytes() as usize,
            indexer_config: (&opt.indexer_options).try_into()?,
            autobatching_enabled: true,
            index_growth_amount: byte_unit::Byte::from_str("10GiB").unwrap().get_bytes() as usize,
            index_count: 20,
        })?)
    };

    match (
        index_scheduler_builder(),
        auth_controller.map_err(anyhow::Error::from),
        create_version_file(&opt.db_path).map_err(anyhow::Error::from),
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

/// Ensure you're in a valid state and open the IndexScheduler + AuthController for you.
fn open_or_create_database(
    opt: &Opt,
    empty_db: bool,
) -> anyhow::Result<(IndexScheduler, AuthController)> {
    if !empty_db {
        check_version_file(&opt.db_path)?;
    }

    open_or_create_database_unchecked(opt, OnFailure::KeepDb)
}

fn import_dump(
    db_path: &Path,
    dump_path: &Path,
    index_scheduler: &mut IndexScheduler,
    auth: &mut AuthController,
) -> Result<(), anyhow::Error> {
    let reader = File::open(dump_path)?;
    let mut dump_reader = dump::DumpReader::open(reader)?;

    if let Some(date) = dump_reader.date() {
        log::info!(
            "Importing a dump of meilisearch `{:?}` from the {}",
            dump_reader.version(), // TODO: get the meilisearch version instead of the dump version
            date
        );
    } else {
        log::info!(
            "Importing a dump of meilisearch `{:?}`",
            dump_reader.version(), // TODO: get the meilisearch version instead of the dump version
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

    let indexer_config = index_scheduler.indexer_config();

    // /!\ The tasks must be imported AFTER importing the indexes or else the scheduler might
    // try to process tasks while we're trying to import the indexes.

    // 3. Import the indexes.
    for index_reader in dump_reader.indexes()? {
        let mut index_reader = index_reader?;
        let metadata = index_reader.metadata();
        log::info!("Importing index `{}`.", metadata.uid);

        let date = Some((metadata.created_at, metadata.updated_at));
        let index = index_scheduler.create_raw_index(&metadata.uid, date)?;

        let mut wtxn = index.write_txn()?;

        let mut builder = milli::update::Settings::new(&mut wtxn, &index, indexer_config);
        // 3.1 Import the primary key if there is one.
        if let Some(ref primary_key) = metadata.primary_key {
            builder.set_primary_key(primary_key.to_string());
        }

        // 3.2 Import the settings.
        log::info!("Importing the settings.");
        let settings = index_reader.settings()?;
        apply_settings_to_builder(&settings, &mut builder);
        builder.execute(|indexing_step| log::debug!("update: {:?}", indexing_step), || false)?;

        // 3.3 Import the documents.
        // 3.3.1 We need to recreate the grenad+obkv format accepted by the index.
        log::info!("Importing the documents.");
        let file = tempfile::tempfile()?;
        let mut builder = DocumentsBatchBuilder::new(BufWriter::new(file));
        for document in index_reader.documents()? {
            builder.append_json_object(&document?)?;
        }

        // This flush the content of the batch builder.
        let file = builder.into_inner()?.into_inner()?;

        // 3.3.2 We feed it to the milli index.
        let reader = BufReader::new(file);
        let reader = DocumentsBatchReader::from_reader(reader)?;

        let builder = milli::update::IndexDocuments::new(
            &mut wtxn,
            &index,
            indexer_config,
            IndexDocumentsConfig {
                update_method: IndexDocumentsMethod::ReplaceDocuments,
                ..Default::default()
            },
            |indexing_step| log::debug!("update: {:?}", indexing_step),
            || false,
        )?;

        let (builder, user_result) = builder.add_documents(reader)?;
        log::info!("{} documents found.", user_result?);
        builder.execute()?;
        wtxn.commit()?;
        log::info!("All documents successfully imported.");
    }

    // 4. Import the tasks.
    for ret in dump_reader.tasks()? {
        let (task, file) = ret?;
        index_scheduler.register_dumped_task(task, file)?;
    }
    Ok(())
}

pub fn configure_data(
    config: &mut web::ServiceConfig,
    index_scheduler: Data<IndexScheduler>,
    auth: AuthController,
    opt: &Opt,
    analytics: Arc<dyn Analytics>,
) {
    let http_payload_size_limit = opt.http_payload_size_limit.get_bytes() as usize;
    config
        .app_data(index_scheduler)
        .app_data(auth)
        .app_data(web::Data::from(analytics))
        .app_data(
            web::JsonConfig::default()
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

#[cfg(feature = "metrics")]
pub fn configure_metrics_route(config: &mut web::ServiceConfig, enable_metrics_route: bool) {
    if enable_metrics_route {
        config.service(
            web::resource("/metrics").route(web::get().to(crate::route_metrics::get_metrics)),
        );
    }
}

/// Parses the output of
/// [`VERGEN_GIT_SEMVER_LIGHTWEIGHT`](https://docs.rs/vergen/latest/vergen/struct.Git.html#instructions)
///  as a prototype name.
///
/// Returns `Some(prototype_name)` if the following conditions are met on this value:
///
/// 1. starts with `prototype-`,
/// 2. ends with `-<some_number>`,
/// 3. does not end with `<some_number>-<some_number>`.
///
/// Otherwise, returns `None`.
pub fn prototype_name() -> Option<&'static str> {
    let prototype: &'static str = option_env!("VERGEN_GIT_SEMVER_LIGHTWEIGHT")?;

    if !prototype.starts_with("prototype-") {
        return None;
    }

    let mut rsplit_prototype = prototype.rsplit('-');
    // last component MUST be a number
    rsplit_prototype.next()?.parse::<u64>().ok()?;
    // before than last component SHALL NOT be a number
    rsplit_prototype.next()?.parse::<u64>().err()?;

    Some(prototype)
}
