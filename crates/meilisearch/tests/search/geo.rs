use meili_snap::{json_string, snapshot};
use meilisearch_types::milli::constants::RESERVED_GEO_FIELD_NAME;
use once_cell::sync::Lazy;

use crate::common::{Server, Value};
use crate::json;

use super::test_settings_documents_indexing_swapping_and_search;

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "id": 1,
            "name": "Taco Truck",
            "address": "444 Salsa Street, Burritoville",
            "type": "Mexican",
            "rating": 9,
            "_geo": {
                "lat": 34.0522,
                "lng": -118.2437
            }
        },
        {
            "id": 2,
            "name": "La Bella Italia",
            "address": "456 Elm Street, Townsville",
            "type": "Italian",
            "rating": 9,
            "_geo": {
                "lat": "45.4777599",
                "lng": "9.1967508"
            }
        },
        {
            "id": 3,
            "name": "Crêpe Truck",
            "address": "2 Billig Avenue, Rouenville",
            "type": "French",
            "rating": 10
        }
    ])
});

#[actix_rt::test]
async fn geo_sort_with_geo_strings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["_geo"])).await;
    index.update_settings_sortable_attributes(json!(["_geo"])).await;
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "filter": "_geoRadius(45.472735, 9.184019, 10000)",
                "sort": ["_geoPoint(0.0, 0.0):asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
            },
        )
        .await;
}

#[actix_rt::test]
async fn geo_bounding_box_with_string_and_number() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.update_settings_filterable_attributes(json!(["_geo"])).await;
    index.update_settings_sortable_attributes(json!(["_geo"])).await;
    let (ret, _code) = index.add_documents(documents, None).await;
    index.wait_task(ret.uid()).await.succeeded();

    index
        .search(
            json!({
                "filter": "_geoBoundingBox([89, 179], [-89, -179])",
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
                {
                  "hits": [
                    {
                      "id": 1,
                      "name": "Taco Truck",
                      "address": "444 Salsa Street, Burritoville",
                      "type": "Mexican",
                      "rating": 9,
                      "_geo": {
                        "lat": 34.0522,
                        "lng": -118.2437
                      }
                    },
                    {
                      "id": 2,
                      "name": "La Bella Italia",
                      "address": "456 Elm Street, Townsville",
                      "type": "Italian",
                      "rating": 9,
                      "_geo": {
                        "lat": "45.4777599",
                        "lng": "9.1967508"
                      }
                    }
                  ],
                  "query": "",
                  "processingTimeMs": "[time]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 2
                }
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn bug_4640() {
    // https://github.com/meilisearch/meilisearch/issues/4640
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.update_settings_filterable_attributes(json!(["_geo"])).await;
    let (ret, _code) = index.update_settings_sortable_attributes(json!(["_geo"])).await;
    index.wait_task(ret.uid()).await.succeeded();

    // Sort the document with the second one first
    index
        .search(
            json!({
                "sort": ["_geoPoint(45.4777599, 9.1967508):asc"],
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
                {
                  "hits": [
                    {
                      "id": 2,
                      "name": "La Bella Italia",
                      "address": "456 Elm Street, Townsville",
                      "type": "Italian",
                      "rating": 9,
                      "_geo": {
                        "lat": "45.4777599",
                        "lng": "9.1967508"
                      },
                      "_geoDistance": 0
                    },
                    {
                      "id": 1,
                      "name": "Taco Truck",
                      "address": "444 Salsa Street, Burritoville",
                      "type": "Mexican",
                      "rating": 9,
                      "_geo": {
                        "lat": 34.0522,
                        "lng": -118.2437
                      },
                      "_geoDistance": 9714063
                    },
                    {
                      "id": 3,
                      "name": "Crêpe Truck",
                      "address": "2 Billig Avenue, Rouenville",
                      "type": "French",
                      "rating": 10
                    }
                  ],
                  "query": "",
                  "processingTimeMs": "[time]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 3
                }
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn geo_asc_with_words() {
    let documents = json!([
      { "id": 0, "doggo": "jean", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 0 } },
      { "id": 1, "doggo": "intel", RESERVED_GEO_FIELD_NAME: { "lat": 88, "lng": 0 } },
      { "id": 2, "doggo": "jean bob", RESERVED_GEO_FIELD_NAME: { "lat": -89, "lng": 0 } },
      { "id": 3, "doggo": "jean michel", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 178 } },
      { "id": 4, "doggo": "bob marley", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": -179 } },
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({"searchableAttributes": ["id", "doggo"], "rankingRules": ["words", "geo:asc"]}),
        &json!({"q": "jean"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
            {
              "hits": [
                {
                  "id": 0,
                  "doggo": "jean",
                  "_geo": {
                    "lat": 0,
                    "lng": 0
                  }
                },
                {
                  "id": 2,
                  "doggo": "jean bob",
                  "_geo": {
                    "lat": -89,
                    "lng": 0
                  }
                },
                {
                  "id": 3,
                  "doggo": "jean michel",
                  "_geo": {
                    "lat": 0,
                    "lng": 178
                  }
                }
              ],
              "query": "jean",
              "processingTimeMs": "[time]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 3
            }
            "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({"searchableAttributes": ["id", "doggo"], "rankingRules": ["words", "geo:asc"]}),
        &json!({"q": "bob"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
            {
              "hits": [
                {
                  "id": 2,
                  "doggo": "jean bob",
                  "_geo": {
                    "lat": -89,
                    "lng": 0
                  }
                },
                {
                  "id": 4,
                  "doggo": "bob marley",
                  "_geo": {
                    "lat": 0,
                    "lng": -179
                  }
                }
              ],
              "query": "bob",
              "processingTimeMs": "[time]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 2
            }
            "###);
        },
    )
    .await;

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({"searchableAttributes": ["id", "doggo"], "rankingRules": ["words", "geo:asc"]}),
        &json!({"q": "intel"}),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
            {
              "hits": [
                {
                  "id": 1,
                  "doggo": "intel",
                  "_geo": {
                    "lat": 88,
                    "lng": 0
                  }
                }
              ],
              "query": "intel",
              "processingTimeMs": "[time]",
              "limit": 20,
              "offset": 0,
              "estimatedTotalHits": 1
            }
            "###);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_sort_with_words() {
    let documents = json!([
      { "id": 0, "doggo": "jean", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 0 } },
      { "id": 1, "doggo": "intel", RESERVED_GEO_FIELD_NAME: { "lat": 88, "lng": 0 } },
      { "id": 2, "doggo": "jean bob", RESERVED_GEO_FIELD_NAME: { "lat": -89, "lng": 0 } },
      { "id": 3, "doggo": "jean michel", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 178 } },
      { "id": 4, "doggo": "bob marley", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": -179 } },
    ]);

    test_settings_documents_indexing_swapping_and_search(
      &documents,
      &json!({"searchableAttributes": ["id", "doggo"], "rankingRules": ["words", "sort"], "sortableAttributes": [RESERVED_GEO_FIELD_NAME]}),
      &json!({"q": "jean", "sort": ["_geoPoint(0.0, 0.0):asc"]}),
      |response, code| {
          assert_eq!(code, 200, "{response}");
          snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }), @r###"
          {
            "hits": [
              {
                "id": 0,
                "doggo": "jean",
                "_geo": {
                  "lat": 0,
                  "lng": 0
                },
                "_geoDistance": 0
              },
              {
                "id": 2,
                "doggo": "jean bob",
                "_geo": {
                  "lat": -89,
                  "lng": 0
                },
                "_geoDistance": 9896348
              },
              {
                "id": 3,
                "doggo": "jean michel",
                "_geo": {
                  "lat": 0,
                  "lng": 178
                },
                "_geoDistance": 19792697
              }
            ],
            "query": "jean",
            "processingTimeMs": "[time]",
            "limit": 20,
            "offset": 0,
            "estimatedTotalHits": 3
          }
          "###);
      },
    )
    .await;
}
