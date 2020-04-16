use assert_json_diff::assert_json_eq;
use serde_json::json;

mod common;

#[actix_rt::test]
async fn update_stop_words() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    // 1 - Get stop words

    let (response, _status_code) = server.get_stop_words().await;
    assert_eq!(response.as_array().unwrap().is_empty(), true);

    // 2 - Update stop words

    let body = json!(["the", "a"]);
    server.update_stop_words(body.clone()).await;

    // 3 - Get all stop words and compare to the previous one

    let (response, _status_code) = server.get_stop_words().await;
    assert_json_eq!(body, response, ordered: false);

    // 4 - Delete all stop words

    server.delete_stop_words().await;

    // 5 - Get all stop words and check if they are empty

    let (response, _status_code) = server.get_stop_words().await;
    assert_eq!(response.as_array().unwrap().is_empty(), true);
}

#[actix_rt::test]
async fn add_documents_and_stop_words() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies().await;

    // 2 - Update stop words

    let body = json!(["the", "of"]);
    server.update_stop_words(body.clone()).await;

    // 3 - Search for a document with stop words

    let (response, _status_code) = server.search("q=the%20mask").await;
    assert!(!response["hits"].as_array().unwrap().is_empty());

    // 4 - Search for documents with *only* stop words

    let (response, _status_code) = server.search("q=the%20of").await;
    assert!(response["hits"].as_array().unwrap().is_empty());

    // 5 - Delete all stop words

    // server.delete_stop_words();

    // // 6 - Search for a document with one stop word

    // assert!(!response["hits"].as_array().unwrap().is_empty());
}
