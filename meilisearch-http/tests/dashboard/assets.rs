use crate::common::Server;
use actix_web::test;
use meilisearch_http::create_app;

#[actix_rt::test]
async fn dashboard_assets_load() {
    let server = Server::new().await;
    let _index = server.index("test");

    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        true,
        &server.service.options
    ))
    .await;

    mod generated {
        include!(concat!(env!("OUT_DIR"), "/generated.rs"));
    }

    let generated = generated::generate();

    for (path, _) in generated.into_iter() {
        // "index.html" redirects to "/"
        let path = if path == "index.html" {
            // dashboard/index.rs contains seperate test for "/"
            continue;
        } else {
            "/".to_owned() + path
        };

        let req = test::TestRequest::get().uri(&path).to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(res.status(), 200);
    }
}
