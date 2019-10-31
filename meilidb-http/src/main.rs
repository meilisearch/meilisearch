use http::header::HeaderValue;
use log::info;
use main_error::MainError;
use tide::middleware::{CorsMiddleware, CorsOrigin};
use tide_log::RequestLogger;

use meilidb_http::data::Data;
use meilidb_http::option::Opt;
use meilidb_http::routes;

#[cfg(not(target_os = "macos"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn main() -> Result<(), MainError> {
    let opt = Opt::new();

    let data = Data::new(opt.clone());
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
