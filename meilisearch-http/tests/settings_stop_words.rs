use assert_json_diff::assert_json_eq;
use serde_json::json;

mod common;

#[test]
fn update_stop_words() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    // 1 - Get stop words

    let (response, _status_code) = server.get_stop_words();
    assert_eq!(response.as_array().unwrap().is_empty(), true);

    // 2 - Update stop words

    let body = json!(["the", "a"]);
    server.update_stop_words(body.clone());

    // 3 - Get all stop words and compare to the previous one

    let (response, _status_code) = server.get_stop_words();
    assert_json_eq!(body, response, ordered: false);

    // 4 - Delete all stop words

    server.delete_stop_words();

    // 5 - Get all stop words and check if they are empty

    let (response, _status_code) = server.get_stop_words();
    assert_eq!(response.as_array().unwrap().is_empty(), true);
}

#[test]
fn add_documents_and_stop_words() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    // 2 - Update stop words

    let body = json!(["the", "of"]);
    server.update_stop_words(body.clone());

    // 3 - Search for a document with stop words

    let (response, _status_code) = server.search("q=the%20mask");
    assert!(!response["hits"].as_array().unwrap().is_empty());

    // 4 - Search for documents with *only* stop words

    let (response, _status_code) = server.search("q=the%20of");
    assert!(response["hits"].as_array().unwrap().is_empty());

    // 5 - Delete all stop words

    // server.delete_stop_words();

    // // 6 - Search for a document with one stop word

    // assert!(!response["hits"].as_array().unwrap().is_empty());
}
