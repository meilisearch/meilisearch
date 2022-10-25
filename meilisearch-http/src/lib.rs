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
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::milli::documents::{DocumentsBatchBuilder, DocumentsBatchReader};
use meilisearch_types::milli::update::{IndexDocumentsConfig, IndexDocumentsMethod};
use meilisearch_types::milli::{self};
use meilisearch_types::settings::apply_settings_to_builder;
pub use option::Opt;

use crate::error::MeilisearchHttpError;

pub static AUTOBATCHING_ENABLED: AtomicBool = AtomicBool::new(false);

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

// TODO: TAMO: Finish setting up things
pub fn setup_meilisearch(opt: &Opt) -> anyhow::Result<(IndexScheduler, AuthController)> {
    // we don't want to create anything in the data.ms yet, thus we
    // wrap our two builders in a closure that'll be executed later.
    let auth_controller_builder = || AuthController::new(&opt.db_path, &opt.master_key);
    let index_scheduler_builder = || {
        IndexScheduler::new(
            opt.db_path.join("tasks"),
            opt.db_path.join("update_files"),
            opt.db_path.join("indexes"),
            opt.snapshot_dir.clone(),
            opt.dumps_dir.clone(),
            opt.max_task_db_size.get_bytes() as usize,
            opt.max_index_size.get_bytes() as usize,
            (&opt.indexer_options).try_into()?,
            true,
        )
    };
    let meilisearch_builder = || -> anyhow::Result<_> {
        // if anything wrong happens we delete the `data.ms` entirely.
        match (
            index_scheduler_builder().map_err(anyhow::Error::from),
            auth_controller_builder().map_err(anyhow::Error::from),
        ) {
            (Ok(i), Ok(a)) => Ok((i, a)),
            (Err(e), _) | (_, Err(e)) => {
                std::fs::remove_dir_all(&opt.db_path)?;
                Err(e)
            }
        }
    };

    let (index_scheduler, auth_controller) = if let Some(ref _path) = opt.import_snapshot {
        // handle the snapshot with something akin to the dumps
        // + the snapshot interval / spawning a thread
        todo!();
    } else if let Some(ref path) = opt.import_dump {
        let empty_db = is_empty_db(&opt.db_path);
        let src_path_exists = path.exists();

        if empty_db && src_path_exists {
            let (mut index_scheduler, mut auth_controller) = meilisearch_builder()?;
            match import_dump(&opt.db_path, path, &mut index_scheduler, &mut auth_controller) {
                Ok(()) => (index_scheduler, auth_controller),
                Err(e) => {
                    std::fs::remove_dir_all(&opt.db_path)?;
                    return Err(e);
                }
            }
        } else if !empty_db && !opt.ignore_dump_if_db_exists {
            bail!(
                "database already exists at {:?}, try to delete it or rename it",
                opt.db_path.canonicalize().unwrap_or_else(|_| opt.db_path.to_owned())
            )
        } else if !src_path_exists && !opt.ignore_missing_dump {
            bail!("dump doesn't exist at {:?}", path)
        } else {
            let (mut index_scheduler, mut auth_controller) = meilisearch_builder()?;
            match import_dump(&opt.db_path, path, &mut index_scheduler, &mut auth_controller) {
                Ok(()) => (index_scheduler, auth_controller),
                Err(e) => {
                    std::fs::remove_dir_all(&opt.db_path)?;
                    return Err(e);
                }
            }
        }
    } else {
        meilisearch_builder()?
    };

    /*
    TODO: We should start a thread to handle the snapshots.
    meilisearch
        // snapshot
        .set_ignore_missing_snapshot(opt.ignore_missing_snapshot)
        .set_ignore_snapshot_if_db_exists(opt.ignore_snapshot_if_db_exists)
        .set_snapshot_interval(Duration::from_secs(opt.snapshot_interval_sec))
        .set_snapshot_dir(opt.snapshot_dir.clone())

    if let Some(ref path) = opt.import_snapshot {
        meilisearch.set_import_snapshot(path.clone());
    }

    if opt.schedule_snapshot {
        meilisearch.set_schedule_snapshot();
    }
    */

    Ok((index_scheduler, auth_controller))
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
        let index = index_scheduler.create_raw_index(&metadata.uid)?;

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
