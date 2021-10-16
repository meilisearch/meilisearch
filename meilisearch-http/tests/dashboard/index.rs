use crate::common::Server;
use actix_web::test;
use meilisearch_http::create_app;

#[actix_rt::test]
async fn dashboard_is_up() {
    let server = Server::new().await;
    let _index = server.index("test");

    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        true,
        &server.service.options
    ))
    .await;

    // "index.html" redirects to "/"
    let req = test::TestRequest::get().uri("/").to_request();

    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    assert_eq!(status_code, 200);
}
