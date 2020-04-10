use assert_json_diff::assert_json_eq;
use serde_json::json;

mod common;

#[test]
fn write_all_and_delete() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    // 2 - Send the settings

    let body = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "desc(release_date)",
        "desc(rank)",
    ]);

    server.update_ranking_rules(body.clone());

    // 3 - Get all settings and compare to the previous one

    let (response, _status_code) = server.get_ranking_rules();

    assert_json_eq!(body, response, ordered: false);

    // 4 - Delete all settings

    server.delete_ranking_rules();

    // 5 - Get all settings and check if they are empty

    let (response, _status_code) = server.get_ranking_rules();

    let expected = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness"
    ]);

    assert_json_eq!(expected, response, ordered: false);
}

#[test]
fn write_all_and_update() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    // 2 - Send the settings

    let body = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "desc(release_date)",
        "desc(rank)",
    ]);

    server.update_ranking_rules(body.clone());

    // 3 - Get all settings and compare to the previous one

    let (response, _status_code) = server.get_ranking_rules();

    assert_json_eq!(body, response, ordered: false);

    // 4 - Update all settings

    let body = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "desc(release_date)",
    ]);

    server.update_ranking_rules(body);

    // 5 - Get all settings and check if the content is the same of (4)

    let (response, _status_code) = server.get_ranking_rules();

    let expected = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "desc(release_date)",
    ]);

    assert_json_eq!(expected, response, ordered: false);
}

#[test]
fn send_undefined_rule() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    let body = json!(["typos",]);

    let (_response, status_code) = server.update_ranking_rules_sync(body);
    assert_eq!(status_code, 400);
}

#[test]
fn send_malformed_custom_rule() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    let body = json!(["dsc(truc)",]);

    let (_response, status_code) = server.update_ranking_rules_sync(body);
    assert_eq!(status_code, 400);
}

// Test issue https://github.com/meilisearch/MeiliSearch/issues/521
#[test]
fn write_custom_ranking_and_index_documents() {
    let mut server = common::Server::with_uid("movies");
    let body = json!({
        "uid": "movies",
        "primaryKey": "id",
    });
    server.create_index(body);

    // 1 - Add ranking rules with one custom ranking on a string

    let body = json!(["asc(title)", "typo"]);

    server.update_ranking_rules(body);

    // 2 - Add documents

    let body = json!([
      {
        "id": 1,
        "title": "Le Petit Prince",
        "author": "Exupéry"
      },
      {
        "id": 2,
        "title": "Pride and Prejudice",
        "author": "Jane Austen"
      }
    ]);

    server.add_or_replace_multiple_documents(body);

    // 3 - Get the first document and compare

    let expected = json!({
        "id": 1,
        "title": "Le Petit Prince",
        "author": "Exupéry"
    });

    let (response, status_code) = server.get_document(1);
    assert_eq!(status_code, 200);

    assert_json_eq!(response, expected, ordered: false);
}
