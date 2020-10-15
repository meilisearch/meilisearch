use serde_json::json;

mod common;

#[actix_rt::test]
async fn create_index_lazy_by_pushing_documents() {
    let mut server = common::Server::with_uid("movies");

    // 1 - Add documents

    let body = json!([{
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/movies/documents?primaryKey=title";
    let (response, status_code) = server.post_request(&url, body).await;
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");
}

#[actix_rt::test]
async fn create_index_lazy_by_pushing_documents_and_discover_pk() {
    let mut server = common::Server::with_uid("movies");

    // 1 - Add documents

    let body = json!([{
      "id": 1,
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/movies/documents";
    let (response, status_code) = server.post_request(&url, body).await;
    assert_eq!(status_code, 202);
    let update_id = response["updateId"].as_u64().unwrap();
    server.wait_update_id(update_id).await;

    // 3 - Check update success

    let (response, status_code) = server.get_update_status(update_id).await;
    assert_eq!(status_code, 200);
    assert_eq!(response["status"], "processed");
}

#[actix_rt::test]
async fn create_index_lazy_by_pushing_documents_with_wrong_name() {
    let server = common::Server::with_uid("wrong&name");

    let body = json!([{
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/wrong&name/documents?primaryKey=title";
    let (response, status_code) = server.post_request(&url, body).await;
    assert_eq!(status_code, 400);
    assert_eq!(response["errorCode"], "invalid_index_uid");
}

#[actix_rt::test]
async fn create_index_lazy_add_documents_failed() {
    let mut server = common::Server::with_uid("wrong&name");

    let body = json!([{
      "title": "Test",
      "comment": "comment test"
    }]);

    let url = "/indexes/wrong&name/documents";
    let (response, status_code) = server.post_request(&url, body).await;
    assert_eq!(status_code, 400);
    assert_eq!(response["errorCode"], "invalid_index_uid");

    let (_, status_code) = server.get_index().await;
    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_settings() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!({
        "rankingRules": [
            "typo",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(registered)",
            "desc(age)",
        ],
        "distinctAttribute": "id",
        "searchableAttributes": [
            "id",
            "name",
            "color",
            "gender",
            "email",
            "phone",
            "address",
            "registered",
            "about"
        ],
        "displayedAttributes": [
            "name",
            "gender",
            "email",
            "registered",
            "age",
        ],
        "stopWords": [
            "ad",
            "in",
            "ut",
        ],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
        "attributesForFaceting": ["name"],
    });

    server.update_all_settings(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_settings_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!({
        "rankingRules": [
            "other",
            "words",
            "proximity",
            "attribute",
            "wordsPosition",
            "exactness",
            "desc(registered)",
            "desc(age)",
        ],
        "distinctAttribute": "id",
        "searchableAttributes": [
            "id",
            "name",
            "color",
            "gender",
            "email",
            "phone",
            "address",
            "registered",
            "about"
        ],
        "displayedAttributes": [
            "name",
            "gender",
            "email",
            "registered",
            "age",
        ],
        "stopWords": [
            "ad",
            "in",
            "ut",
        ],
        "synonyms": {
            "road": ["street", "avenue"],
            "street": ["avenue"],
        },
        "anotherSettings": ["name"],
    });

    let (_, status_code) = server.update_all_settings_sync(body.clone()).await;
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_ranking_rules() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!([
        "typo",
        "words",
        "proximity",
        "attribute",
        "wordsPosition",
        "exactness",
        "desc(registered)",
        "desc(age)",
    ]);

    server.update_ranking_rules(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_ranking_rules_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!({
        "rankingRules": 123,
    });

    let (_, status_code) = server.update_ranking_rules_sync(body.clone()).await;
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_distinct_attribute() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!("type");

    server.update_distinct_attribute(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_distinct_attribute_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(123);

    let (resp, status_code) = server.update_distinct_attribute_sync(body.clone()).await;
    eprintln!("resp: {:?}", resp);
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (resp, status_code) = server.get_all_settings().await;
    eprintln!("resp: {:?}", resp);
    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_searchable_attributes() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(["title", "description"]);

    server.update_searchable_attributes(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_searchable_attributes_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(123);

    let (_, status_code) = server.update_searchable_attributes_sync(body.clone()).await;
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_displayed_attributes() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(["title", "description"]);

    server.update_displayed_attributes(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_displayed_attributes_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(123);

    let (_, status_code) = server.update_displayed_attributes_sync(body.clone()).await;
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_attributes_for_faceting() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(["title", "description"]);

    server.update_attributes_for_faceting(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_attributes_for_faceting_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(123);

    let (_, status_code) = server
        .update_attributes_for_faceting_sync(body.clone())
        .await;
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_synonyms() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!({
        "road": ["street", "avenue"],
        "street": ["avenue"],
    });

    server.update_synonyms(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_synonyms_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(123);

    let (_, status_code) = server.update_synonyms_sync(body.clone()).await;
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 404);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_stop_words() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(["le", "la", "les"]);

    server.update_stop_words(body.clone()).await;

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 200);
}

#[actix_rt::test]
async fn create_index_lazy_by_sending_stop_words_with_error() {
    let mut server = common::Server::with_uid("movies");
    // 2 - Send the settings

    let body = json!(123);

    let (_, status_code) = server.update_stop_words_sync(body.clone()).await;
    assert_eq!(status_code, 400);

    // 3 - Get all settings and compare to the previous one

    let (_, status_code) = server.get_all_settings().await;

    assert_eq!(status_code, 404);
}
