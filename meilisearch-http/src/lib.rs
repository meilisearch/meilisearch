#![allow(rustdoc::private_intra_doc_links)]
#[macro_use]
pub mod error;
pub mod analytics;
#[macro_use]
pub mod extractors;
pub mod option;
pub mod routes;

#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "metrics")]
pub mod route_metrics;

use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

use crate::error::MeilisearchHttpError;
use actix_web::error::JsonPayloadError;
use actix_web::web::Data;
use analytics::Analytics;
use error::PayloadError;
use http::header::CONTENT_TYPE;
use index_scheduler::milli::update::IndexerConfig;
pub use option::Opt;

use actix_web::{web, HttpRequest};

use extractors::payload::PayloadConfig;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use sysinfo::{RefreshKind, System, SystemExt};

pub static AUTOBATCHING_ENABLED: AtomicBool = AtomicBool::new(false);

// TODO: TAMO: Finish setting up things
pub fn setup_meilisearch(opt: &Opt) -> anyhow::Result<IndexScheduler> {
    let meilisearch = IndexScheduler::new(
        opt.db_path.join("tasks"),
        opt.db_path.join("update_files"),
        opt.db_path.join("indexes"),
        opt.max_index_size.get_bytes() as usize,
        (&opt.indexer_options).try_into()?,
        #[cfg(test)]
        todo!("We'll see later"),
    )?;

    /*
    TODO: We should start a thread to handle the snapshots.
    meilisearch
        .set_max_index_size(opt.max_index_size.get_bytes() as usize)
        .set_max_task_store_size(opt.max_task_db_size.get_bytes() as usize)
        // snapshot
        .set_ignore_missing_snapshot(opt.ignore_missing_snapshot)
        .set_ignore_snapshot_if_db_exists(opt.ignore_snapshot_if_db_exists)
        .set_snapshot_interval(Duration::from_secs(opt.snapshot_interval_sec))
        .set_snapshot_dir(opt.snapshot_dir.clone())
        // dump
        .set_ignore_missing_dump(opt.ignore_missing_dump)
        .set_ignore_dump_if_db_exists(opt.ignore_dump_if_db_exists)
        .set_dump_dst(opt.dumps_dir.clone());

    if let Some(ref path) = opt.import_snapshot {
        meilisearch.set_import_snapshot(path.clone());
    }

    if let Some(ref path) = opt.import_dump {
        meilisearch.set_dump_src(path.clone());
    }

    if opt.schedule_snapshot {
        meilisearch.set_schedule_snapshot();
    }
    */

    Ok(meilisearch)
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
            let Resource {
                mime_type, data, ..
            } = resource;
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
