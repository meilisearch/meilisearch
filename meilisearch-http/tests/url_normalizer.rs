mod common;

#[actix_rt::test]
async fn url_normalizer() {
    let mut server = common::Server::with_uid("movies");

    let (_response, status_code) = server.get_request("/version").await;
    assert_eq!(status_code, 200);

    let (_response, status_code) = server.get_request("//version").await;
    assert_eq!(status_code, 200);

    let (_response, status_code) = server.get_request("/version/").await;
    assert_eq!(status_code, 200);

    let (_response, status_code) = server.get_request("//version/").await;
    assert_eq!(status_code, 200);
}
