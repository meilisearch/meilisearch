use std::env;
use std::sync::Arc;

use actix_cors::Cors;
use actix_web::http::KeepAlive;
use actix_web::web::Data;
use actix_web::{middleware, HttpServer};
use clap::Parser;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_http::analytics::Analytics;
use meilisearch_http::{analytics, configure_data, create_app, dashboard, routes};
use meilisearch_http::{setup_meilisearch, Opt};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// does all the setup before meilisearch is launched
fn setup(opt: &Opt) -> anyhow::Result<()> {
    let mut log_builder = env_logger::Builder::new();
    log_builder.parse_filters(&opt.log_level);
    if opt.log_level == "info" {
        // if we are in info we only allow the warn log_level for milli
        log_builder.filter_module("milli", log::LevelFilter::Warn);
    }

    log_builder.init();

    Ok(())
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    setup(&opt)?;

    match opt.env.as_ref() {
        "production" => {
            if opt.master_key.is_none() {
                anyhow::bail!(
                    "In production mode, the environment variable MEILI_MASTER_KEY is mandatory"
                )
            }
        }
        "development" => (),
        _ => unreachable!(),
    }

    let index_scheduler = setup_meilisearch(&opt)?;

    let auth_controller = AuthController::new(&opt.db_path, &opt.master_key)?;

    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    let (analytics, user) = if !opt.no_analytics {
        analytics::SegmentAnalytics::new(&opt, &meilisearch).await
    } else {
        analytics::MockAnalytics::new(&opt)
    };
    #[cfg(any(debug_assertions, not(feature = "analytics")))]
    let (analytics, user) = analytics::MockAnalytics::new(&opt);

    print_launch_resume(&opt, &user);

    run_http(index_scheduler, auth_controller, opt, analytics).await?;

    Ok(())
}

async fn run_http(
    index_scheduler: IndexScheduler,
    auth_controller: AuthController,
    opt: Opt,
    analytics: Arc<dyn Analytics>,
) -> anyhow::Result<()> {
    let enable_dashboard = &opt.env == "development";
    let opt_clone = opt.clone();
    let index_scheduler = Data::new(index_scheduler);

    let http_server = HttpServer::new(move || {
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

        let app = app
            .wrap(
                Cors::default()
                    .send_wildcard()
                    .allow_any_header()
                    .allow_any_origin()
                    .allow_any_method()
                    .max_age(86_400), // 24h
            )
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .wrap(middleware::NormalizePath::new(
                middleware::TrailingSlash::Trim,
            ));

        #[cfg(feature = "metrics")]
        let app = app.wrap(Condition::new(
            opt.enable_metrics_route,
            route_metrics::RouteMetrics,
        ));

        app
    })
    // Disable signals allows the server to terminate immediately when a user enter CTRL-C
    .disable_signals()
    .keep_alive(KeepAlive::Os);

    if let Some(config) = opt_clone.get_ssl_config()? {
        http_server
            .bind_rustls(opt_clone.http_addr, config)?
            .run()
            .await?;
    } else {
        http_server.bind(&opt_clone.http_addr)?.run().await?;
    }
    Ok(())
}

pub fn print_launch_resume(opt: &Opt, user: &str) {
    let commit_sha = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");
    let commit_date = option_env!("VERGEN_GIT_COMMIT_TIMESTAMP").unwrap_or("unknown");
    let protocol = if opt.ssl_cert_path.is_some() && opt.ssl_key_path.is_some() {
        "https"
    } else {
        "http"
    };
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

    eprintln!("Database path:\t\t{:?}", opt.db_path);
    eprintln!("Server listening on:\t\"{}://{}\"", protocol, opt.http_addr);
    eprintln!("Environment:\t\t{:?}", opt.env);
    eprintln!("Commit SHA:\t\t{:?}", commit_sha.to_string());
    eprintln!("Commit date:\t\t{:?}", commit_date.to_string());
    eprintln!(
        "Package version:\t{:?}",
        env!("CARGO_PKG_VERSION").to_string()
    );

    #[cfg(all(not(debug_assertions), feature = "analytics"))]
    {
        if !opt.no_analytics {
            eprintln!(
                "
Thank you for using Meilisearch!

We collect anonymized analytics to improve our product and your experience. To learn more, including how to turn off analytics, visit our dedicated documentation page: https://docs.meilisearch.com/learn/what_is_meilisearch/telemetry.html

Anonymous telemetry:\t\"Enabled\""
            );
        } else {
            eprintln!("Anonymous telemetry:\t\"Disabled\"");
        }
    }

    if !user.is_empty() {
        eprintln!("Instance UID:\t\t\"{}\"", user);
    }

    eprintln!();

    if opt.master_key.is_some() {
        eprintln!("A Master Key has been set. Requests to Meilisearch won't be authorized unless you provide an authentication key.");
    } else {
        eprintln!("No master key found; The server will accept unidentified requests. \
            If you need some protection in development mode, please export a key: export MEILI_MASTER_KEY=xxx");
    }

    eprintln!();
    eprintln!("Documentation:\t\thttps://docs.meilisearch.com");
    eprintln!("Source code:\t\thttps://github.com/meilisearch/meilisearch");
    eprintln!("Contact:\t\thttps://docs.meilisearch.com/resources/contact.html");
    eprintln!();
}
