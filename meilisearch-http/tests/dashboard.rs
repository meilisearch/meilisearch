mod common;

#[actix_rt::test]
async fn dashboard() {
    let mut server = common::Server::with_uid("movies");

    let (_response, status_code) = server.get_request("/").await;
    assert_eq!(status_code, 200);

    let (_response, status_code) = server.get_request("/bulma.min.css").await;
    assert_eq!(status_code, 200);
}
