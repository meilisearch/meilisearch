#![allow(rustdoc::private_intra_doc_links)]
#[macro_use]
pub mod error;
#[macro_use]
pub mod extractors;
#[cfg(all(not(debug_assertions), feature = "analytics"))]
pub mod analytics;
pub mod helpers;
pub mod option;
pub mod routes;
use std::path::Path;
use std::time::Duration;

use crate::error::{MeilisearchHttpError, ResponseError};
use crate::extractors::authentication::AuthConfig;
use actix_web::error::JsonPayloadError;
use http::header::CONTENT_TYPE;
pub use option::Opt;

use actix_web::web;

use extractors::authentication::policies::*;
use extractors::payload::PayloadConfig;
use meilisearch_lib::MeiliSearch;
use sha2::Digest;

#[derive(Clone)]
pub struct ApiKeys {
    pub public: Option<String>,
    pub private: Option<String>,
    pub master: Option<String>,
}

impl ApiKeys {
    pub fn generate_missing_api_keys(&mut self) {
        if let Some(master_key) = &self.master {
            if self.private.is_none() {
                let key = format!("{}-private", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.private = Some(format!("{:x}", sha));
            }
            if self.public.is_none() {
                let key = format!("{}-public", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.public = Some(format!("{:x}", sha));
            }
        }
    }
}

pub fn setup_meilisearch(opt: &Opt) -> anyhow::Result<MeiliSearch> {
    let mut meilisearch = MeiliSearch::builder();
    meilisearch
        .set_max_index_size(opt.max_index_size.get_bytes() as usize)
        .set_max_update_store_size(opt.max_udb_size.get_bytes() as usize)
        .set_ignore_missing_snapshot(opt.ignore_missing_snapshot)
        .set_ignore_snapshot_if_db_exists(opt.ignore_snapshot_if_db_exists)
        .set_dump_dst(opt.dumps_dir.clone())
        .set_snapshot_interval(Duration::from_secs(opt.snapshot_interval_sec))
        .set_snapshot_dir(opt.snapshot_dir.clone());

    if let Some(ref path) = opt.import_snapshot {
        meilisearch.set_import_snapshot(path.clone());
    }

    if let Some(ref path) = opt.import_dump {
        meilisearch.set_dump_src(path.clone());
    }

    if opt.schedule_snapshot {
        meilisearch.set_schedule_snapshot();
    }

    meilisearch.build(opt.db_path.clone(), opt.indexer_options.clone())
}

/// Cleans and setup the temporary file folder in the database directory. This must be done after
/// the meilisearch instance has been created, to not interfere with the snapshot and dump loading.
pub fn setup_temp_dir(db_path: impl AsRef<Path>) -> anyhow::Result<()> {
    // Set the tempfile directory in the current db path, to avoid cross device references. Also
    // remove the previous outstanding files found there
    //
    // TODO: if two processes open the same db, one might delete the other tmpdir. Need to make
    // sure that no one is using it before deleting it.
    let temp_path = db_path.as_ref().join("tmp");
    // Ignore error if tempdir doesn't exist
    let _ = std::fs::remove_dir_all(&temp_path);
    std::fs::create_dir_all(&temp_path)?;
    if cfg!(windows) {
        std::env::set_var("TMP", temp_path);
    } else {
        std::env::set_var("TMPDIR", temp_path);
    }

    Ok(())
}

pub fn configure_data(config: &mut web::ServiceConfig, data: MeiliSearch, opt: &Opt) {
    let http_payload_size_limit = opt.http_payload_size_limit.get_bytes() as usize;
    config
        .app_data(data)
        .app_data(
            web::JsonConfig::default()
                .content_type(|mime| mime == mime::APPLICATION_JSON)
                .error_handler(|err, req| match err {
                    JsonPayloadError::ContentType if req.headers().get(CONTENT_TYPE).is_none() => {
                        ResponseError::from(MeilisearchHttpError::MissingContentType(vec![
                            mime::APPLICATION_JSON.to_string(),
                        ]))
                        .into()
                    }
                    JsonPayloadError::ContentType => {
                        ResponseError::from(MeilisearchHttpError::InvalidContentType(
                            req.headers()
                                .get(CONTENT_TYPE)
                                .unwrap()
                                .to_str()
                                .unwrap_or("unknown")
                                .to_string(),
                            vec![mime::APPLICATION_JSON.to_string()],
                        ))
                        .into()
                    }
                    err => error::payload_error_handler(err).into(),
                }),
        )
        .app_data(PayloadConfig::new(http_payload_size_limit))
        .app_data(
            web::QueryConfig::default()
                .error_handler(|err, _req| error::payload_error_handler(err).into()),
        );
}

pub fn configure_auth(config: &mut web::ServiceConfig, opts: &Opt) {
    let mut keys = ApiKeys {
        master: opts.master_key.clone(),
        private: None,
        public: None,
    };

    keys.generate_missing_api_keys();

    let auth_config = if let Some(ref master_key) = keys.master {
        let private_key = keys.private.as_ref().unwrap();
        let public_key = keys.public.as_ref().unwrap();
        let mut policies = init_policies!(Public, Private, Admin);
        create_users!(
            policies,
            master_key.as_bytes() => { Admin, Private, Public },
            private_key.as_bytes() => { Private, Public },
            public_key.as_bytes() => { Public }
        );
        AuthConfig::Auth(policies)
    } else {
        AuthConfig::NoAuth
    };

    config.app_data(auth_config).app_data(keys);
}

#[cfg(feature = "mini-dashboard")]
pub fn dashboard(config: &mut web::ServiceConfig, enable_frontend: bool) {
    use actix_web::HttpResponse;
    use actix_web_static_files::Resource;

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
                config.service(web::resource("/").route(
                    web::get().to(move || HttpResponse::Ok().content_type(mime_type).body(data)),
                ));
            } else {
                config.service(web::resource(path).route(
                    web::get().to(move || HttpResponse::Ok().content_type(mime_type).body(data)),
                ));
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

#[macro_export]
macro_rules! create_app {
    ($data:expr, $enable_frontend:expr, $opt:expr) => {{
        use actix_cors::Cors;
        use actix_web::middleware::TrailingSlash;
        use actix_web::App;
        use actix_web::{middleware, web};
        use meilisearch_http::error::{MeilisearchHttpError, ResponseError};
        use meilisearch_http::routes;
        use meilisearch_http::{configure_auth, configure_data, dashboard};

        App::new()
            .configure(|s| configure_data(s, $data.clone(), &$opt))
            .configure(|s| configure_auth(s, &$opt))
            .configure(routes::configure)
            .configure(|s| dashboard(s, $enable_frontend))
            .wrap(
                Cors::default()
                    .send_wildcard()
                    .allowed_headers(vec!["content-type", "x-meili-api-key"])
                    .allow_any_origin()
                    .allow_any_method()
                    .max_age(86_400), // 24h
            )
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(middleware::NormalizePath::new(
                middleware::TrailingSlash::Trim,
            ))
    }};
}
