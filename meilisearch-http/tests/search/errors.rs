use crate::common::Server;
use serde_json::json;

#[actix_rt::test]
async fn search_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .search(json!({"q": "hello"}), |response, code| {
            assert_eq!(code, 404, "{}", response);
            assert_eq!(response["errorCode"], "index_not_found");
        })
        .await;
}

#[actix_rt::test]
async fn search_unexisting_parameter() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .search(json!({"marin": "hello"}), |response, code| {
            assert_eq!(code, 400, "{}", response);
            assert_eq!(response["errorCode"], "bad_request");
        })
        .await;
}
