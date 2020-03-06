use assert_json_diff::assert_json_eq;
use serde_json::json;

mod common;

#[test]
fn update_stop_words() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "identifier": "id",
    });
    server.create_index(body);

    // 1 - Get stop words

    let (response, _status_code) = server.get_stop_words();
    assert_eq!(response.as_array().unwrap().is_empty(), true);

    // 2 - Update stop words

    let body = json!([
        "the",
        "a"
    ]);
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
