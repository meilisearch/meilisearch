use meili_snap::snapshot;
use meilisearch::Opt;
use once_cell::sync::Lazy;
use tempfile::TempDir;

use crate::common::{default_settings, Server, Value, NESTED_DOCUMENTS};
use crate::json;

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "genres": ["Action", "Adventure"],
            "id": "287947",
        },
        {
            "title": "Captain Marvel",
            "genres": ["Action", "Adventure"],
            "id": "299537",
        },
        {
            "title": "Escape Room",
            "genres": ["Horror", "Thriller", "Multiple Words"],
            "id": "522681",
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "genres": ["Action", "Comedy"],
            "id": "166428",
        },
        {
            "title": "Gläss",
            "genres": ["Thriller"],
            "id": "450465",
        }
    ])
});

async fn test_settings_documents_indexing_swapping_and_facet_search(
    documents: &Value,
    settings: &Value,
    query: &Value,
    test: impl Fn(Value, actix_http::StatusCode) + std::panic::UnwindSafe + Clone,
) {
    let temp = TempDir::new().unwrap();
    let server = Server::new_with_options(Opt { ..default_settings(temp.path()) }).await.unwrap();

    eprintln!("Documents -> Settings -> test");
    let index = server.index("test");

    let (task, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(code, 202, "{}", task);
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index.update_settings(settings.clone()).await;
    assert_eq!(code, 202, "{}", task);
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.facet_search(query.clone()).await;
    insta::allow_duplicates! {
        test(response, code);
    }

    let (task, code) = server.delete_index("test").await;
    assert_eq!(code, 202, "{}", task);
    server.wait_task(task.uid()).await.succeeded();

    eprintln!("Settings -> Documents -> test");
    let index = server.index("test");

    let (task, code) = index.update_settings(settings.clone()).await;
    assert_eq!(code, 202, "{}", task);
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(code, 202, "{}", task);
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.facet_search(query.clone()).await;
    insta::allow_duplicates! {
        test(response, code);
    }

    let (task, code) = server.delete_index("test").await;
    assert_eq!(code, 202, "{}", task);
    server.wait_task(task.uid()).await.succeeded();
}

#[actix_rt::test]
async fn simple_facet_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 200, "{response}");
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 2);

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "adventure"})).await;

    assert_eq!(code, 200, "{response}");
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 1);
}

#[actix_rt::test]
async fn simple_facet_search_on_movies() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
      {
        "id": 1,
        "title": "Carol",
        "genres": [
          "Romance",
          "Drama"
        ],
        "color": [
          "red"
        ],
        "platforms": [
          "MacOS",
          "Linux",
          "Windows"
        ]
      },
      {
        "id": 2,
        "title": "Wonder Woman",
        "genres": [
          "Action",
          "Adventure"
        ],
        "color": [
          "green"
        ],
        "platforms": [
          "MacOS"
        ]
      },
      {
        "id": 3,
        "title": "Life of Pi",
        "genres": [
          "Adventure",
          "Drama"
        ],
        "color": [
          "blue"
        ],
        "platforms": [
          "Windows"
        ]
      },
      {
        "id": 4,
        "title": "Mad Max: Fury Road",
        "genres": [
          "Adventure",
          "Science Fiction"
        ],
        "color": [
          "red"
        ],
        "platforms": [
          "MacOS",
          "Linux"
        ]
      },
      {
        "id": 5,
        "title": "Moana",
        "genres": [
          "Fantasy",
          "Action"
        ],
        "color": [
          "red"
        ],
        "platforms": [
          "Windows"
        ]
      },
      {
        "id": 6,
        "title": "Philadelphia",
        "genres": [
          "Drama"
        ],
        "color": [
          "blue"
        ],
        "platforms": [
          "MacOS",
          "Linux",
          "Windows"
        ]
      }
    ]);
    let (response, code) =
        index.update_settings_filterable_attributes(json!(["genres", "color"])).await;
    assert_eq!(202, code, "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    let (response, _code) = index.add_documents(documents, None).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetQuery": "", "facetName": "genres", "q": "" })).await;

    assert_eq!(code, 200, "{response}");
    snapshot!(response["facetHits"], @r###"[{"value":"Action","count":2},{"value":"Adventure","count":3},{"value":"Drama","count":3},{"value":"Fantasy","count":1},{"value":"Romance","count":1},{"value":"Science Fiction","count":1}]"###);
}

#[actix_rt::test]
async fn advanced_facet_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    index.update_settings_typo_tolerance(json!({ "enabled": false })).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "adventre"})).await;

    snapshot!(code, @"200 OK");
    snapshot!(response["facetHits"].as_array().unwrap().len(), @"0");

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "àdventure"})).await;

    snapshot!(code, @"200 OK");
    snapshot!(response["facetHits"].as_array().unwrap().len(), @"1");
}

#[actix_rt::test]
async fn more_advanced_facet_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    index.update_settings_typo_tolerance(json!({ "disableOnWords": ["adventre"] })).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "adventre"})).await;

    snapshot!(code, @"200 OK");
    snapshot!(response["facetHits"].as_array().unwrap().len(), @"0");

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "adventure"})).await;

    snapshot!(code, @"200 OK");
    snapshot!(response["facetHits"].as_array().unwrap().len(), @"1");
}

#[actix_rt::test]
async fn simple_facet_search_with_max_values() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_faceting(json!({ "maxValuesPerFacet": 1 })).await;
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 200, "{response}");
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 1);
}

#[actix_rt::test]
async fn simple_facet_search_by_count_with_max_values() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index
        .update_settings_faceting(
            json!({ "maxValuesPerFacet": 1, "sortFacetValuesBy": { "*": "count" } }),
        )
        .await;
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 200, "{response}");
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 1);
}

#[actix_rt::test]
async fn non_filterable_facet_search_error() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;
    assert_eq!(code, 400, "{response}");

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "adv"})).await;
    assert_eq!(code, 400, "{response}");
}

#[actix_rt::test]
async fn facet_search_dont_support_words() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "words"})).await;

    assert_eq!(code, 200, "{response}");
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 0);
}

#[actix_rt::test]
async fn simple_facet_search_with_sort_by_count() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_faceting(json!({ "sortFacetValuesBy": { "*": "count" } })).await;
    index.update_settings_filterable_attributes(json!(["genres"])).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 200, "{response}");
    let hits = response["facetHits"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0], json!({ "value": "Action", "count": 3 }));
    assert_eq!(hits[1], json!({ "value": "Adventure", "count": 2 }));
}

#[actix_rt::test]
async fn add_documents_and_deactivate_facet_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    let (response, _code) = index.add_documents(documents, None).await;
    server.wait_task(response.uid()).await.succeeded();
    let (response, code) = index
        .update_settings(json!({
            "facetSearch": false,
            "filterableAttributes": ["genres"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 400, "{response}");
    snapshot!(response, @r###"
    {
      "message": "The facet search is disabled for this index",
      "code": "facet_search_disabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#facet_search_disabled"
    }
    "###);
}

#[actix_rt::test]
async fn deactivate_facet_search_and_add_documents() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!({
            "facetSearch": false,
            "filterableAttributes": ["genres"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();
    let documents = DOCUMENTS.clone();
    let (response, _code) = index.add_documents(documents, None).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 400, "{response}");
    snapshot!(response, @r###"
    {
      "message": "The facet search is disabled for this index",
      "code": "facet_search_disabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#facet_search_disabled"
    }
    "###);
}

#[actix_rt::test]
async fn deactivate_facet_search_add_documents_and_activate_facet_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!({
            "facetSearch": false,
            "filterableAttributes": ["genres"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();
    let documents = DOCUMENTS.clone();
    let (response, _code) = index.add_documents(documents, None).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
            "facetSearch": true,
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 200, "{response}");
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn deactivate_facet_search_add_documents_and_reset_facet_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!({
            "facetSearch": false,
            "filterableAttributes": ["genres"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();
    let documents = DOCUMENTS.clone();
    let (response, _code) = index.add_documents(documents, None).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
            "facetSearch": serde_json::Value::Null,
        }))
        .await;
    assert_eq!("202", code.as_str(), "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetName": "genres", "facetQuery": "a"})).await;

    assert_eq!(code, 200, "{response}");
    assert_eq!(response["facetHits"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn facet_search_with_filterable_attributes_rules() {
    test_settings_documents_indexing_swapping_and_facet_search(
        &DOCUMENTS,
        &json!({"filterableAttributes": ["genres"]}),
        &json!({"facetName": "genres", "facetQuery": "a"}),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["facetHits"], @r###"[{"value":"Action","count":3},{"value":"Adventure","count":2}]"###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["genres"], "features": {"facetSearch": true, "filter": {"equality": false, "comparison": false}}}]}),
        &json!({"facetName": "genres", "facetQuery": "a"}),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["facetHits"], @r###"[{"value":"Action","count":3},{"value":"Adventure","count":2}]"###);
        },
    ).await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": ["doggos.name"]}),
        &json!({"facetName": "doggos.name", "facetQuery": "b"}),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["facetHits"], @r###"[{"value":"bobby","count":1},{"value":"buddy","count":1}]"###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["doggos.name"], "features": {"facetSearch": true, "filter": {"equality": false, "comparison": false}}}]}),
        &json!({"facetName": "doggos.name", "facetQuery": "b"}),
        |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["facetHits"], @r###"[{"value":"bobby","count":1},{"value":"buddy","count":1}]"###);
        },
    ).await;
}

#[actix_rt::test]
async fn facet_search_with_filterable_attributes_rules_errors() {
    test_settings_documents_indexing_swapping_and_facet_search(
        &DOCUMENTS,
        &json!({"filterableAttributes": ["genres"]}),
        &json!({"facetName": "invalid", "facetQuery": "a"}),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(response["message"], @r###""Attribute `invalid` is not facet-searchable. Available facet-searchable attributes patterns are: `genres`. To make it facet-searchable add it to the `filterableAttributes` index settings.""###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_facet_search(
      &DOCUMENTS,
      &json!({"filterableAttributes": [{"attributePatterns": ["genres"]}]}),
      &json!({"facetName": "genres", "facetQuery": "a"}),
      |response, code| {
          snapshot!(code, @"400 Bad Request");
          snapshot!(response["message"], @r###""Attribute `genres` is not facet-searchable. Note: this attribute matches rule #0 in filterableAttributes, but this rule does not enable facetSearch.\nHint: enable facetSearch in rule #0 by adding `\"facetSearch\": true` to the rule.\nHint: prepend another rule matching genres with facetSearch: true before rule #0""###);
      },
    )
    .await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["genres"], "features": {"facetSearch": false, "filter": {"equality": true, "comparison": true}}}]}),
        &json!({"facetName": "genres", "facetQuery": "a"}),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(response["message"], @r###""Attribute `genres` is not facet-searchable. Note: this attribute matches rule #0 in filterableAttributes, but this rule does not enable facetSearch.\nHint: enable facetSearch in rule #0 by adding `\"facetSearch\": true` to the rule.\nHint: prepend another rule matching genres with facetSearch: true before rule #0""###);
        },
    ).await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["genres"], "features": {"facetSearch": false, "filter": {"equality": false, "comparison": false}}}]}),
        &json!({"facetName": "genres", "facetQuery": "a"}),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(response["message"], @r###""Attribute `genres` is not facet-searchable. Note: this attribute matches rule #0 in filterableAttributes, but this rule does not enable facetSearch.\nHint: enable facetSearch in rule #0 by adding `\"facetSearch\": true` to the rule.\nHint: prepend another rule matching genres with facetSearch: true before rule #0""###);
        },
    ).await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["doggos.name"]}]}),
        &json!({"facetName": "invalid.name", "facetQuery": "b"}),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(response["message"], @r###""Attribute `invalid.name` is not facet-searchable. This index does not have configured facet-searchable attributes. To make it facet-searchable add it to the `filterableAttributes` index settings.""###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["doggos.name"], "features": {"facetSearch": false, "filter": {"equality": true, "comparison": true}}}]}),
        &json!({"facetName": "doggos.name", "facetQuery": "b"}),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(response["message"], @r###""Attribute `doggos.name` is not facet-searchable. Note: this attribute matches rule #0 in filterableAttributes, but this rule does not enable facetSearch.\nHint: enable facetSearch in rule #0 by adding `\"facetSearch\": true` to the rule.\nHint: prepend another rule matching doggos.name with facetSearch: true before rule #0""###);
        },
    ).await;

    test_settings_documents_indexing_swapping_and_facet_search(
        &NESTED_DOCUMENTS,
        &json!({"filterableAttributes": [{"attributePatterns": ["doggos.name"], "features": {"facetSearch": false, "filter": {"equality": false, "comparison": false}}}]}),
        &json!({"facetName": "doggos.name", "facetQuery": "b"}),
        |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(response["message"], @r###""Attribute `doggos.name` is not facet-searchable. Note: this attribute matches rule #0 in filterableAttributes, but this rule does not enable facetSearch.\nHint: enable facetSearch in rule #0 by adding `\"facetSearch\": true` to the rule.\nHint: prepend another rule matching doggos.name with facetSearch: true before rule #0""###);
        },
    ).await;
}

#[actix_rt::test]
async fn distinct_facet_search_on_movies() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
      {
        "id": 1,
        "title": "Carol",
        "genres": ["Romance", "Drama", "Blob"],
        "color": "crimson"
      },
      {
        "id": 2,
        "title": "Wonder Woman",
        "genres": ["Action", "Adventure", "Blob"],
        "color": "emerald"
      },
      {
        "id": 3,
        "title": "Life of Pi",
        "genres": ["Adventure", "Drama", "Blob"],
        "color": "azure"
      },
      {
        "id": 4,
        "title": "Mad Max: Fury Road",
        "genres": ["Adventure", "Science Fiction", "Blob"],
        "color": "scarlet"
      },
      {
        "id": 5,
        "title": "Moana",
        "genres": ["Fantasy", "Action", "Blob"],
        "color": "coral"
      },
      {
        "id": 6,
        "title": "Philadelphia",
        "genres": ["Drama", "Blob"],
        "color": "navy"
      },
      {
        "id": 7,
        "title": "The Matrix",
        "genres": ["Science Fiction", "Action", "Blob"],
        "color": "onyx"
      },
      {
        "id": 8,
        "title": "Inception",
        "genres": ["Science Fiction", "Thriller", "Blob"],
        "color": "cerulean"
      },
      {
        "id": 9,
        "title": "The Shawshank Redemption",
        "genres": ["Drama", "Blob"],
        "color": "slate"
      },
      {
        "id": 10,
        "title": "Pulp Fiction",
        "genres": ["Crime", "Drama", "Blob"],
        "color": "gold"
      },
      {
        "id": 11,
        "title": "The Dark Knight",
        "genres": ["Action", "Crime", "Blob"],
        "color": "obsidian"
      },
      {
        "id": 12,
        "title": "Forrest Gump",
        "genres": ["Drama", "Romance", "Blob"],
        "color": "jade"
      },
      {
        "id": 13,
        "title": "The Godfather",
        "genres": ["Crime", "Drama", "Blob"],
        "color": "sepia"
      },
      {
        "id": 14,
        "title": "Fight Club",
        "genres": ["Drama", "Thriller", "Blob"],
        "color": "ruby"
      },
      {
        "id": 15,
        "title": "Goodfellas",
        "genres": ["Crime", "Biography", "Blob"],
        "color": "charcoal"
      },
      {
        "id": 16,
        "title": "The Silence of the Lambs",
        "genres": ["Crime", "Thriller", "Blob"],
        "color": "amethyst"
      },
      {
        "id": 17,
        "title": "Schindler's List",
        "genres": ["Biography", "Drama", "Blob"],
        "color": "ebony"
      },
      {
        "id": 18,
        "title": "The Lord of the Rings",
        "genres": ["Adventure", "Fantasy", "Blob"],
        "color": "forest"
      },
      {
        "id": 19,
        "title": "Star Wars",
        "genres": ["Science Fiction", "Adventure", "Blob"],
        "color": "amber"
      },
      {
        "id": 20,
        "title": "Jurassic Park",
        "genres": ["Adventure", "Science Fiction", "Blob"],
        "color": "lime"
      },
      {
        "id": 21,
        "title": "Titanic",
        "genres": ["Drama", "Romance", "Blob"],
        "color": "sapphire"
      },
      {
        "id": 22,
        "title": "The Avengers",
        "genres": ["Action", "Science Fiction", "Blob"],
        "color": "burgundy"
      },
      {
        "id": 23,
        "title": "Avatar",
        "genres": ["Science Fiction", "Adventure", "Blob"],
        "color": "turquoise"
      },
      {
        "id": 24,
        "title": "The Green Mile",
        "genres": ["Crime", "Fantasy", "Blob"],
        "color": "emerald"
      },
      {
        "id": 25,
        "title": "Gladiator",
        "genres": ["Action", "Drama", "Blob"],
        "color": "sepia"
      },
      {
        "id": 26,
        "title": "The Departed",
        "genres": ["Crime", "Thriller", "Blob"],
        "color": "crimson"
      },
      {
        "id": 27,
        "title": "Saving Private Ryan",
        "genres": ["Drama", "War", "Blob"],
        "color": "slate"
      },
      {
        "id": 28,
        "title": "Interstellar",
        "genres": ["Science Fiction", "Adventure", "Blob"],
        "color": "azure"
      },
      {
        "id": 29,
        "title": "The Pianist",
        "genres": ["Biography", "Drama", "Blob"],
        "color": "onyx"
      },
      {
        "id": 30,
        "title": "The Usual Suspects",
        "genres": ["Crime", "Mystery", "Blob"],
        "color": "charcoal"
      },
      {
        "id": 31,
        "title": "The Sixth Sense",
        "genres": ["Mystery", "Thriller", "Blob"],
        "color": "amethyst"
      },
      {
        "id": 32,
        "title": "The Princess Bride",
        "genres": ["Adventure", "Romance", "Blob"],
        "color": "ruby"
      },
      {
        "id": 33,
        "title": "Blade Runner",
        "genres": ["Science Fiction", "Noir", "Blob"],
        "color": "sapphire"
      },
      {
        "id": 34,
        "title": "The Big Lebowski",
        "genres": ["Comedy", "Crime", "Blob"],
        "color": "gold"
      },
      {
        "id": 35,
        "title": "Good Will Hunting",
        "genres": ["Drama", "Romance", "Blob"],
        "color": "turquoise"
      },
      {
        "id": 36,
        "title": "The Terminator",
        "genres": ["Action", "Science Fiction", "Blob"],
        "color": "obsidian"
      },
      {
        "id": 37,
        "title": "Casablanca",
        "genres": ["Drama", "Romance", "Blob"],
        "color": "jade"
      },
      {
        "id": 38,
        "title": "The Exorcist",
        "genres": ["Horror", "Thriller", "Blob"],
        "color": "burgundy"
      },
      {
        "id": 39,
        "title": "Apocalypse Now",
        "genres": ["Drama", "War", "Blob"],
        "color": "forest"
      },
      {
        "id": 40,
        "title": "Back to the Future",
        "genres": ["Adventure", "Comedy", "Blob"],
        "color": "amber"
      },
      {
        "id": 41,
        "title": "The Graduate",
        "genres": ["Comedy", "Drama", "Blob"],
        "color": "azure"
      },
      {
        "id": 42,
        "title": "Alien",
        "genres": ["Horror", "Science Fiction", "Blob"],
        "color": "obsidian"
      },
      {
        "id": 43,
        "title": "The Breakfast Club",
        "genres": ["Drama", "Comedy", "Blob"],
        "color": "coral"
      },
      {
        "id": 44,
        "title": "Die Hard",
        "genres": ["Action", "Thriller", "Blob"],
        "color": "scarlet"
      },
      {
        "id": 45,
        "title": "The Sound of Music",
        "genres": ["Drama", "Musical", "Blob"],
        "color": "emerald"
      },
      {
        "id": 46,
        "title": "Jaws",
        "genres": ["Horror", "Thriller", "Blob"],
        "color": "navy"
      },
      {
        "id": 47,
        "title": "Rocky",
        "genres": ["Drama", "Sport", "Blob"],
        "color": "burgundy"
      },
      {
        "id": 48,
        "title": "E.T. the Extra-Terrestrial",
        "genres": ["Adventure", "Science Fiction", "Blob"],
        "color": "amber"
      },
      {
        "id": 49,
        "title": "The Godfather Part II",
        "genres": ["Crime", "Drama", "Blob"],
        "color": "sepia"
      },
      {
        "id": 50,
        "title": "One Flew Over the Cuckoo's Nest",
        "genres": ["Drama", "Blob"],
        "color": "slate"
      }
    ]);
    let (response, code) =
        index.update_settings_filterable_attributes(json!(["genres", "color"])).await;
    assert_eq!(202, code, "{response:?}");
    server.wait_task(response.uid()).await.succeeded();
    let (response, code) = index.update_settings_distinct_attribute(json!("color")).await;
    assert_eq!(202, code, "{response:?}");
    server.wait_task(response.uid()).await.succeeded();

    let (response, _code) = index.add_documents(documents, None).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) =
        index.facet_search(json!({"facetQuery": "blob", "facetName": "genres", "q": "" })).await;

    // non-exhaustive facet count is counting 27 documents with the facet query "blob" but there are only 23 documents with a distinct color.
    assert_eq!(code, 200, "{response}");
    snapshot!(response["facetHits"], @r###"[{"value":"Blob","count":27}]"###);

    let (response, code) =
        index.facet_search(json!({"facetQuery": "blob", "facetName": "genres", "q": "", "exhaustiveFacetCount": true })).await;

    // exhaustive facet count is counting 23 documents with the facet query "blob" which is the number of distinct colors.
    assert_eq!(code, 200, "{response}");
    snapshot!(response["facetHits"], @r###"[{"value":"Blob","count":23}]"###);
}
