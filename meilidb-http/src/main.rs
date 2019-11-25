use std::env::VarError::NotPresent;
use std::{env, thread};

use http::header::HeaderValue;
use log::info;
use main_error::MainError;
use structopt::StructOpt;
use tide::middleware::{CorsMiddleware, CorsOrigin};
use tide_log::RequestLogger;

use meilidb_http::data::Data;
use meilidb_http::option::Opt;
use meilidb_http::routes;
use meilidb_http::routes::index::index_update_callback;

mod analytics;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn main() -> Result<(), MainError> {
    env_logger::init();

    let opt = Opt::from_args();
    let data = Data::new(opt.clone());

    if env::var("MEILI_NO_ANALYTICS") == Err(NotPresent) {
        thread::spawn(|| analytics::analytics_sender());
    }

    let data_cloned = data.clone();
    data.db.set_update_callback(Box::new(move |name, status| {
        index_update_callback(name, &data_cloned, status);
    }));

    let mut app = tide::App::with_state(data);

    app.middleware(
        CorsMiddleware::new()
            .allow_origin(CorsOrigin::from("*"))
            .allow_methods(HeaderValue::from_static("GET, POST, OPTIONS")),
    );
    app.middleware(RequestLogger::new());
    app.middleware(tide_compression::Compression::new());
    app.middleware(tide_compression::Decompression::new());

    routes::load_routes(&mut app);

    info!("Server HTTP enabled");
    app.run(opt.http_addr)?;

    Ok(())
}
