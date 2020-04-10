use assert_json_diff::assert_json_eq;
use serde_json::json;

mod common;

#[test]
fn index_new_fields_default() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    // 1 - Add a document

    let body = json!([{
        "id": 1,
        "title": "I'm a legend",
    }]);

    server.add_or_replace_multiple_documents(body);

    // 2 - Get the complete document

    let expected = json!({
        "id": 1,
        "title": "I'm a legend",
    });

    let (response, status_code) = server.get_document(1);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);

    // 3 - Add a document with more fields

    let body = json!([{
        "id": 2,
        "title": "I'm not a legend",
        "description": "A bad copy of the original movie I'm a lengend"
    }]);

    server.add_or_replace_multiple_documents(body);

    // 4 - Get the complete document

    let expected = json!({
        "id": 2,
        "title": "I'm not a legend",
        "description": "A bad copy of the original movie I'm a lengend"
    });

    let (response, status_code) = server.get_document(2);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);
}

#[test]
fn index_new_fields_true() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    // 1 - Set indexNewFields = true

    server.update_accept_new_fields(json!(true));

    // 2 - Add a document

    let body = json!([{
        "id": 1,
        "title": "I'm a legend",
    }]);

    server.add_or_replace_multiple_documents(body);

    // 3 - Get the complete document

    let expected = json!({
        "id": 1,
        "title": "I'm a legend",
    });

    let (response, status_code) = server.get_document(1);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);

    // 4 - Add a document with more fields

    let body = json!([{
        "id": 2,
        "title": "I'm not a legend",
        "description": "A bad copy of the original movie I'm a lengend"
    }]);

    server.add_or_replace_multiple_documents(body);

    // 5 - Get the complete document

    let expected = json!({
        "id": 2,
        "title": "I'm not a legend",
        "description": "A bad copy of the original movie I'm a lengend"
    });

    let (response, status_code) = server.get_document(2);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);
}

#[test]
fn index_new_fields_false() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    // 1 - Set indexNewFields = false

    server.update_accept_new_fields(json!(false));

    // 2 - Add a document

    let body = json!([{
        "id": 1,
        "title": "I'm a legend",
    }]);

    server.add_or_replace_multiple_documents(body);

    // 3 - Get the complete document

    let expected = json!({
        "id": 1,
    });

    let (response, status_code) = server.get_document(1);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);

    // 4 - Add a document with more fields

    let body = json!([{
        "id": 2,
        "title": "I'm not a legend",
        "description": "A bad copy of the original movie I'm a lengend"
    }]);

    server.add_or_replace_multiple_documents(body);

    // 5 - Get the complete document

    let expected = json!({
        "id": 2,
    });

    let (response, status_code) = server.get_document(2);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);
}

#[test]
fn index_new_fields_true_then_false() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    // 1 - Set indexNewFields = true

    server.update_accept_new_fields(json!(true));

    // 2 - Add a document

    let body = json!([{
        "id": 1,
        "title": "I'm a legend",
    }]);

    server.add_or_replace_multiple_documents(body);

    // 3 - Get the complete document

    let expected = json!({
        "id": 1,
        "title": "I'm a legend",
    });

    let (response, status_code) = server.get_document(1);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);

    // 4 - Set indexNewFields = false

    server.update_accept_new_fields(json!(false));

    // 5 - Add a document with more fields

    let body = json!([{
        "id": 2,
        "title": "I'm not a legend",
        "description": "A bad copy of the original movie I'm a lengend"
    }]);

    server.add_or_replace_multiple_documents(body);

    // 6 - Get the complete document

    let expected = json!({
        "id": 2,
        "title": "I'm not a legend",
    });

    let (response, status_code) = server.get_document(2);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);
}

#[test]
fn index_new_fields_false_then_true() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    // 1 - Set indexNewFields = false

    server.update_accept_new_fields(json!(false));

    // 2 - Add a document

    let body = json!([{
        "id": 1,
        "title": "I'm a legend",
    }]);

    server.add_or_replace_multiple_documents(body);

    // 3 - Get the complete document

    let expected = json!({
        "id": 1,
    });

    let (response, status_code) = server.get_document(1);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);

    // 4 - Set indexNewFields = false

    server.update_accept_new_fields(json!(true));

    // 5 - Add a document with more fields

    let body = json!([{
        "id": 2,
        "title": "I'm not a legend",
        "description": "A bad copy of the original movie I'm a lengend"
    }]);

    server.add_or_replace_multiple_documents(body);

    // 6 - Get the complete document

    let expected = json!({
        "id": 1,
    });

    let (response, status_code) = server.get_document(1);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);

    let expected = json!({
        "id": 2,
        "description": "A bad copy of the original movie I'm a lengend"
    });

    let (response, status_code) = server.get_document(2);
    assert_eq!(status_code, 200);
    assert_json_eq!(response, expected);
}

// Fix issue https://github.com/meilisearch/MeiliSearch/issues/518
#[test]
fn accept_new_fields_does_not_take_into_account_the_primary_key() {
    let mut server = common::Server::with_uid("movies");

    // 1 - Create an index with no primary-key

    let body = json!({
        "uid": "movies",
    });
    let (response, status_code) = server.create_index(body);
    assert_eq!(status_code, 201);
    assert_eq!(response["primaryKey"], json!(null));

    // 2 - Add searchable and displayed attributes as: ["title"] & Set acceptNewFields to false

    let body = json!({
        "searchableAttributes": ["title"],
        "displayedAttributes": ["title"],
        "acceptNewFields": false,
    });

    server.update_all_settings(body);

    // 4 - Add a document

    let body = json!([{
      "id": 1,
      "title": "Test",
      "comment": "comment test"
    }]);

    server.add_or_replace_multiple_documents(body);

    // 5 - Get settings, they should not changed

    let (response, _status_code) = server.get_all_settings();

    let expected = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
        ],
        "distinctAttribute": null,
        "searchableAttributes": ["title"],
        "displayedAttributes": ["title"],
        "stopWords": [],
        "synonyms": {},
        "acceptNewFields": false,
    });

    assert_json_eq!(response, expected, ordered: false);
}
