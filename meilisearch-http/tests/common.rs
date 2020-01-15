use std::error::Error;

use tempdir::TempDir;
use tide::server::Service;
use meilisearch_http::data::Data;
use meilisearch_http::option::Opt;
use meilisearch_http::routes;

use http_service_mock::{make_server, TestBackend};


pub fn setup_server() -> Result<TestBackend<Service<Data>>, Box<dyn Error>>{

    let tmp_dir = TempDir::new("meilisearch")?;

    let opt = Opt {
        db_path: tmp_dir.path().to_str().unwrap().to_string(),
        http_addr: "127.0.0.1:7700".to_owned(),
        api_key: None,
        no_analytics: true,
    };

    let data = Data::new(opt.clone());
    let mut app = tide::with_state(data);
    routes::load_routes(&mut app);
    let http_server = app.into_http_service();
    Ok(make_server(http_server)?)
}
