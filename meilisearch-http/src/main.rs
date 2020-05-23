use std::{env, thread};

use actix_cors::Cors;
use actix_web::{middleware, HttpServer};
use main_error::MainError;
use meilisearch_http::data::Data;
use meilisearch_http::helpers::NormalizePath;
use meilisearch_http::option::Opt;
use meilisearch_http::{create_app, index_update_callback};
use structopt::StructOpt;

mod analytics;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[actix_rt::main]
async fn main() -> Result<(), MainError> {
    let opt = Opt::from_args();

    #[cfg(all(not(debug_assertions), feature = "sentry"))]
    let _sentry = sentry::init((
            "https://5ddfa22b95f241198be2271aaf028653@sentry.io/3060337",
            sentry::ClientOptions {
                release: sentry::release_name!(),
                ..Default::default()
            },
    ));

    match opt.env.as_ref() {
        "production" => {
            if opt.master_key.is_none() {
                return Err(
                    "In production mode, the environment variable MEILI_MASTER_KEY is mandatory"
                        .into(),
                );
            }

            #[cfg(all(not(debug_assertions), feature = "sentry"))]
            if !opt.no_analytics {
                sentry::integrations::panic::register_panic_handler();
                sentry::integrations::env_logger::init(None, Default::default());
            }
        }
        "development" => {
            env_logger::from_env(env_logger::Env::default().default_filter_or("info")).init();
        }
        _ => unreachable!(),
    }

    if !opt.no_analytics {
        thread::spawn(analytics::analytics_sender);
    }

    let data = Data::new(opt.clone());

    let data_cloned = data.clone();
    data.db.set_update_callback(Box::new(move |name, status| {
        index_update_callback(name, &data_cloned, status);
    }));

    print_launch_resume(&opt, &data);

    HttpServer::new(move || {
        create_app(&data)
            .wrap(
                Cors::new()
                    .send_wildcard()
                    .allowed_header("x-meili-api-key")
                    .finish(),
            )
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(NormalizePath)
    })
    .bind(opt.http_addr)?
    .run()
    .await?;

    Ok(())
}

pub fn print_launch_resume(opt: &Opt, data: &Data) {
    let ascii_name = r#"
888b     d888          d8b 888 d8b  .d8888b.                                    888
8888b   d8888          Y8P 888 Y8P d88P  Y88b                                   888
88888b.d88888              888     Y88b.                                        888
888Y88888P888  .d88b.  888 888 888  "Y888b.    .d88b.   8888b.  888d888 .d8888b 88888b.
888 Y888P 888 d8P  Y8b 888 888 888     "Y88b. d8P  Y8b     "88b 888P"  d88P"    888 "88b
888  Y8P  888 88888888 888 888 888       "888 88888888 .d888888 888    888      888  888
888   "   888 Y8b.     888 888 888 Y88b  d88P Y8b.     888  888 888    Y88b.    888  888
888       888  "Y8888  888 888 888  "Y8888P"   "Y8888  "Y888888 888     "Y8888P 888  888
"#;

    eprintln!("{}", ascii_name);

    eprintln!("Database path:\t\t{:?}", opt.db_path);
    eprintln!("Server listening on:\t{:?}", opt.http_addr);
    eprintln!("Environment:\t\t{:?}", opt.env);
    eprintln!("Commit SHA:\t\t{:?}", env!("VERGEN_SHA").to_string());
    eprintln!(
        "Build date:\t\t{:?}",
        env!("VERGEN_BUILD_TIMESTAMP").to_string()
    );
    eprintln!(
        "Package version:\t{:?}",
        env!("CARGO_PKG_VERSION").to_string()
    );

    eprintln!();

    if data.api_keys.master.is_some() {
        eprintln!("A Master Key has been set. Requests to MeiliSearch won't be authorized unless you provide an authentication key.");
    } else {
        eprintln!("No master key found; The server will accept unidentified requests. \
            If you need some protection in development mode, please export a key: export MEILI_MASTER_KEY=xxx");
    }

    eprintln!();
    eprintln!("Documentation:\t\thttp://docs.meilisearch.com");
    eprintln!("Source code:\t\thttp://github.com/meilisearch/meilisearch");
    eprintln!("Contact:\t\thttps://docs.meilisearch.com/resources/contact.html or bonjour@meilisearch.com");
    eprintln!();
}
