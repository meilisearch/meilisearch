use std::convert::Into;
use assert_json_diff::assert_json_eq;
use serde_json::json;

mod common;

#[test]
fn write_all_and_delete() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    // 2 - Send the settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(release_date)",
            "desc(rank)",
        ],
        "distinctAttribute": "movie_id",
        "searchableAttributes": [
            "id",
            "movie_id",
            "title",
            "description",
            "poster",
            "release_date",
            "rank",
        ],
        "displayedAttributes": [
            "title",
            "description",
            "poster",
            "release_date",
            "rank",
        ],
        "stopWords": [
            "the",
            "a",
            "an",
        ],
        "synonyms": {
            "wolverine": ["xmen", "logan"],
            "logan": ["wolverine"],
        },
        "acceptNewFields": false,
    });

    server.update_all_settings(body.clone());

    // 3 - Get all settings and compare to the previous one

    let (response, _status_code) = server.get_all_settings();

    assert_json_eq!(body, response, ordered: false);

    // 4 - Delete all settings

    server.delete_all_settings();

    // 5 - Get all settings and check if they are set to default values

    let (response, _status_code) = server.get_all_settings();

    let expect = json!({
        "rankingRules": [
          "typo",
          "words",
          "proximity",
          "attribute",
          "wordsPosition",
          "exactness"
        ],
        "distinctAttribute": null,
        "searchableAttributes": [
          "poster_path",
          "director",
          "id",
          "production_companies",
          "producer",
          "poster",
          "movie_id",
          "vote_count",
          "cast",
          "release_date",
          "vote_average",
          "rank",
          "genres",
          "overview",
          "description",
          "tagline",
          "popularity",
          "title"
        ],
        "displayedAttributes": [
          "poster_path",
          "poster",
          "vote_count",
          "id",
          "movie_id",
          "title",
          "rank",
          "tagline",
          "cast",
          "producer",
          "production_companies",
          "description",
          "director",
          "genres",
          "release_date",
          "overview",
          "vote_average",
          "popularity"
        ],
        "stopWords": [],
        "synonyms": {},
        "acceptNewFields": true,
    });

    assert_json_eq!(expect, response, ordered: false);
}

#[test]
fn write_all_and_update() {
    let mut server = common::Server::with_uid("movies");
    server.populate_movies();

    // 2 - Send the settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(release_date)",
            "desc(rank)",
        ],
        "distinctAttribute": "movie_id",
        "searchableAttributes": [
            "uid",
            "movie_id",
            "title",
            "description",
            "poster",
            "release_date",
            "rank",
        ],
        "displayedAttributes": [
            "title",
            "description",
            "poster",
            "release_date",
            "rank",
        ],
        "stopWords": [
            "the",
            "a",
            "an",
        ],
        "synonyms": {
            "wolverine": ["xmen", "logan"],
            "logan": ["wolverine"],
        },
        "acceptNewFields": false,
    });

    server.update_all_settings(body.clone());

    // 3 - Get all settings and compare to the previous one

    let (response, _status_code) = server.get_all_settings();

    assert_json_eq!(body, response, ordered: false);

    // 4 - Update all settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(release_date)",
        ],
        "searchableAttributes": [
            "title",
            "description",
            "uid",
        ],
        "displayedAttributes": [
            "title",
            "description",
            "release_date",
            "rank",
            "poster",
        ],
        "stopWords": [
        ],
        "synonyms": {
            "wolverine": ["xmen", "logan"],
            "logan": ["wolverine", "xmen"],
        },
        "acceptNewFields": false,
    });

    server.update_all_settings(body);

    // 5 - Get all settings and check if the content is the same of (4)

    let (response, _status_code) = server.get_all_settings();

    let expected = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(release_date)",
        ],
        "distinctAttribute": null,
        "searchableAttributes": [
            "title",
            "description",
            "uid",
        ],
        "displayedAttributes": [
            "title",
            "description",
            "release_date",
            "rank",
            "poster",
        ],
        "stopWords": [],
        "synonyms": {
            "wolverine": ["xmen", "logan"],
            "logan": ["wolverine", "xmen"],
        },
        "acceptNewFields": false
    });

    assert_json_eq!(expected, response, ordered: false);
}
