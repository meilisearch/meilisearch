use meili_snap::{json_string, snapshot};
use meilisearch_types::milli::constants::{RESERVED_GEO_FIELD_NAME, RESERVED_GEO_LIST_FIELD_NAME};

use super::test_settings_documents_indexing_swapping_and_search;
use crate::common::{shared_index_with_geo_documents, Server};
use crate::json;

#[actix_rt::test]
async fn geo_sort_with_geo_strings() {
    let index = shared_index_with_geo_documents().await;

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
    let index = shared_index_with_geo_documents().await;

    index
        .search(
            json!({
                "filter": "_geoBoundingBox([89, 179], [-89, -179])",
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
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
                  "estimatedTotalHits": 2,
                  "requestUid": "[uuid]"
                }
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn bug_4640() {
    // https://github.com/meilisearch/meilisearch/issues/4640
    let index = shared_index_with_geo_documents().await;

    // Sort the document with the second one first
    index
        .search(
            json!({
                "sort": ["_geoPoint(45.4777599, 9.1967508):asc"],
            }),
            |response, code| {
                assert_eq!(code, 200, "{response}");
                snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
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
                  "estimatedTotalHits": 3,
                  "requestUid": "[uuid]"
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
            snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
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
              "estimatedTotalHits": 3,
              "requestUid": "[uuid]"
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
            snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
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
              "estimatedTotalHits": 2,
              "requestUid": "[uuid]"
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
            snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
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
              "estimatedTotalHits": 1,
              "requestUid": "[uuid]"
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
          snapshot!(json_string!(response, { ".processingTimeMs" => "[time]", ".requestUid" => "[uuid]" }), @r###"
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
            "estimatedTotalHits": 3,
            "requestUid": "[uuid]"
          }
          "###);
      },
    )
    .await;
}

// =====================================================
// _geo_list integration tests
// =====================================================

#[actix_rt::test]
async fn geo_list_filter_geo_radius() {
    // Documents with _geo_list: a company with multiple offices
    // Office A is near (0, 0), Office B is near (48.8, 2.3) (Paris)
    let documents = json!([
        {
            "id": 1,
            "name": "Multi-Office Corp",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 0.0, "lng": 0.0 },
                { "lat": 48.8566, "lng": 2.3522 }
            ]
        },
        {
            "id": 2,
            "name": "Single-Office Corp",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 40.7128, "lng": -74.0060 }
            ]
        },
        {
            "id": 3,
            "name": "No-Geo Corp"
        }
    ]);

    // Filter near Paris — should match doc 1 (has a point near Paris) but not doc 2 (NYC only)
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "filter": "_geoRadius(48.8566, 2.3522, 10000)"
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            assert_eq!(hits.as_array().unwrap().len(), 1);
            assert_eq!(hits[0]["id"], 1);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_filter_geo_radius_no_match() {
    let documents = json!([
        {
            "id": 1,
            "name": "Company",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 40.7128, "lng": -74.0060 },
                { "lat": 34.0522, "lng": -118.2437 }
            ]
        }
    ]);

    // Filter near Tokyo — neither NYC nor LA should match
    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "filter": "_geoRadius(35.6762, 139.6503, 10000)"
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            assert_eq!(hits.as_array().unwrap().len(), 0);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_sort_asc_closest_point() {
    // Doc 1: has points at (0,0) and (50,50) — closest to (0,0) is 0 distance
    // Doc 2: has point at (10,10) — distance ~1568 km from (0,0)
    // Doc 3: has points at (30,30) and (20,20) — closest to (0,0) is (20,20) ~3111 km
    let documents = json!([
        {
            "id": 1,
            "name": "Near Origin",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 0.0, "lng": 0.0 },
                { "lat": 50.0, "lng": 50.0 }
            ]
        },
        {
            "id": 2,
            "name": "Moderate",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 10.0, "lng": 10.0 }
            ]
        },
        {
            "id": 3,
            "name": "Far",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 30.0, "lng": 30.0 },
                { "lat": 20.0, "lng": 20.0 }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "sort": ["_geoPoint(0.0, 0.0):asc"]
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            let ids: Vec<i64> =
                hits.as_array().unwrap().iter().map(|h| h["id"].as_i64().unwrap()).collect();
            // Doc 1 closest (point at origin), then doc 2 (10,10), then doc 3 (closest point is 20,20)
            assert_eq!(ids, vec![1, 2, 3]);
            // Doc 1 should have _geoDistance 0 (point at origin)
            assert_eq!(hits[0]["_geoDistance"], 0);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_sort_desc() {
    let documents = json!([
        {
            "id": 1,
            "name": "Near",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 0.0, "lng": 0.0 }
            ]
        },
        {
            "id": 2,
            "name": "Far",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 50.0, "lng": 50.0 }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "sort": ["_geoPoint(0.0, 0.0):desc"]
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            let ids: Vec<i64> =
                hits.as_array().unwrap().iter().map(|h| h["id"].as_i64().unwrap()).collect();
            // Descending: farthest first
            assert_eq!(ids, vec![2, 1]);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_single_point_same_as_geo() {
    // A single-element _geo_list should behave identically to _geo
    let documents = json!([
        {
            "id": 1,
            "name": "Single Point",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 48.8566, "lng": 2.3522 }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "filter": "_geoRadius(48.8566, 2.3522, 100)",
            "sort": ["_geoPoint(48.8566, 2.3522):asc"]
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            assert_eq!(hits.as_array().unwrap().len(), 1);
            assert_eq!(hits[0]["_geoDistance"], 0);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_mixed_with_geo() {
    // Doc 1 has _geo only (at origin)
    // Doc 2 has _geo_list only (points at 10,10 and 20,20)
    // Doc 3 has BOTH _geo (at 5,5) and _geo_list (at 30,30 and 40,40)
    let documents = json!([
        {
            "id": 1,
            "name": "Geo Only",
            RESERVED_GEO_FIELD_NAME: { "lat": 0.0, "lng": 0.0 }
        },
        {
            "id": 2,
            "name": "GeoList Only",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 10.0, "lng": 10.0 },
                { "lat": 20.0, "lng": 20.0 }
            ]
        },
        {
            "id": 3,
            "name": "Both",
            RESERVED_GEO_FIELD_NAME: { "lat": 5.0, "lng": 5.0 },
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 30.0, "lng": 30.0 },
                { "lat": 40.0, "lng": 40.0 }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "sortableAttributes": [RESERVED_GEO_FIELD_NAME, RESERVED_GEO_LIST_FIELD_NAME],
            "filterableAttributes": [RESERVED_GEO_FIELD_NAME, RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "sort": ["_geoPoint(0.0, 0.0):asc"]
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            let ids: Vec<i64> =
                hits.as_array().unwrap().iter().map(|h| h["id"].as_i64().unwrap()).collect();
            // Doc 1 at origin (0), Doc 3 closest point is _geo at (5,5), Doc 2 closest point is (10,10)
            assert_eq!(ids, vec![1, 3, 2]);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_filter_geo_bounding_box() {
    // Doc 1: points at (10, 10) and (50, 50) — (10,10) is in bounding box
    // Doc 2: points at (60, 60) and (70, 70) — neither in bounding box
    let documents = json!([
        {
            "id": 1,
            "name": "Has Point In Box",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 10.0, "lng": 10.0 },
                { "lat": 50.0, "lng": 50.0 }
            ]
        },
        {
            "id": 2,
            "name": "Outside Box",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 60.0, "lng": 60.0 },
                { "lat": 70.0, "lng": 70.0 }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "filter": "_geoBoundingBox([20, 20], [0, 0])"
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            assert_eq!(hits.as_array().unwrap().len(), 1);
            assert_eq!(hits[0]["id"], 1);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_document_update_replaces_points() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Initial: doc with _geo_list at (0,0) and (10,10)
    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": 1,
                    "name": "Company",
                    "_geo_list": [
                        { "lat": 0.0, "lng": 0.0 },
                        { "lat": 10.0, "lng": 10.0 }
                    ]
                }
            ]),
            None,
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _) = index
        .update_settings(json!({
            "filterableAttributes": ["_geo_list"],
            "sortableAttributes": ["_geo_list"]
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Verify initial state: doc matches filter near origin
    index
        .search(json!({ "filter": "_geoRadius(0.0, 0.0, 100)" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;

    // Update: move all points far away
    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": 1,
                    "name": "Company",
                    "_geo_list": [
                        { "lat": 60.0, "lng": 60.0 },
                        { "lat": 70.0, "lng": 70.0 }
                    ]
                }
            ]),
            None,
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Old points should be gone: no match near origin
    index
        .search(json!({ "filter": "_geoRadius(0.0, 0.0, 100)" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 0);
        })
        .await;

    // New points should work
    index
        .search(json!({ "filter": "_geoRadius(60.0, 60.0, 100)" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[actix_rt::test]
async fn geo_list_document_deletion() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": 1,
                    "name": "To Delete",
                    "_geo_list": [
                        { "lat": 48.8566, "lng": 2.3522 }
                    ]
                },
                {
                    "id": 2,
                    "name": "To Keep",
                    "_geo_list": [
                        { "lat": 48.8566, "lng": 2.3522 }
                    ]
                }
            ]),
            None,
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _) = index
        .update_settings(json!({
            "filterableAttributes": ["_geo_list"],
            "sortableAttributes": ["_geo_list"]
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Both docs match initially
    index
        .search(json!({ "filter": "_geoRadius(48.8566, 2.3522, 100)" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 2);
        })
        .await;

    // Delete doc 1
    let (task, _) = index.delete_document(1).await;
    server.wait_task(task.uid()).await.succeeded();

    // Only doc 2 should remain
    index
        .search(json!({ "filter": "_geoRadius(48.8566, 2.3522, 100)" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            assert_eq!(hits.as_array().unwrap().len(), 1);
            assert_eq!(hits[0]["id"], 2);
        })
        .await;
}

#[actix_rt::test]
async fn geo_list_with_string_lat_lng() {
    // _geo_list should accept string lat/lng values just like _geo does
    let documents = json!([
        {
            "id": 1,
            "name": "String Coords",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": "48.8566", "lng": "2.3522" },
                { "lat": "40.7128", "lng": "-74.0060" }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "filter": "_geoRadius(48.8566, 2.3522, 100)",
            "sort": ["_geoPoint(48.8566, 2.3522):asc"]
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            assert_eq!(hits.as_array().unwrap().len(), 1);
            assert_eq!(hits[0]["_geoDistance"], 0);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_error_not_an_array() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _) = index
        .update_settings(json!({
            "filterableAttributes": ["_geo_list"],
            "sortableAttributes": ["_geo_list"]
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": "1",
                    "_geo_list": { "lat": 0.0, "lng": 0.0 }
                }
            ]),
            None,
        )
        .await;
    let response = server.wait_task(task.uid()).await;
    response.failed();
    let (response, _) = index.get_task(task.uid()).await;
    snapshot!(response["error"]["code"], @r###""invalid_document_geo_field""###);
}

#[actix_rt::test]
async fn geo_list_error_element_not_object() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _) = index
        .update_settings(json!({
            "filterableAttributes": ["_geo_list"],
            "sortableAttributes": ["_geo_list"]
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": "1",
                    "_geo_list": ["not an object"]
                }
            ]),
            None,
        )
        .await;
    let response = server.wait_task(task.uid()).await;
    response.failed();
    let (response, _) = index.get_task(task.uid()).await;
    snapshot!(response["error"]["code"], @r###""invalid_document_geo_field""###);
}

#[actix_rt::test]
async fn geo_list_error_element_missing_lat_lng() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _) = index
        .update_settings(json!({
            "filterableAttributes": ["_geo_list"],
            "sortableAttributes": ["_geo_list"]
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": "1",
                    "_geo_list": [{ "foo": "bar" }]
                }
            ]),
            None,
        )
        .await;
    let response = server.wait_task(task.uid()).await;
    response.failed();
    let (response, _) = index.get_task(task.uid()).await;
    snapshot!(response["error"]["code"], @r###""invalid_document_geo_field""###);
}

#[actix_rt::test]
async fn geo_list_error_bad_latitude() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _) = index
        .update_settings(json!({
            "filterableAttributes": ["_geo_list"],
            "sortableAttributes": ["_geo_list"]
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Use a non-numeric value for lat which is truly invalid
    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": "1",
                    "_geo_list": [{ "lat": true, "lng": 0.0 }]
                }
            ]),
            None,
        )
        .await;
    let response = server.wait_task(task.uid()).await;
    response.failed();
    let (response, _) = index.get_task(task.uid()).await;
    snapshot!(response["error"]["code"], @r###""invalid_document_geo_field""###);
}

#[actix_rt::test]
async fn geo_list_null_value_ignored() {
    // A null _geo_list should be treated as absence of the field
    let documents = json!([
        {
            "id": 1,
            "name": "Null geo_list",
            RESERVED_GEO_LIST_FIELD_NAME: null
        },
        {
            "id": 2,
            "name": "Has geo_list",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 48.8566, "lng": 2.3522 }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "filter": "_geoRadius(48.8566, 2.3522, 100)"
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            assert_eq!(hits.as_array().unwrap().len(), 1);
            assert_eq!(hits[0]["id"], 2);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_geo_distance_reflects_closest() {
    // Verify that _geoDistance reflects the minimum distance across all _geo_list points
    let documents = json!([
        {
            "id": 1,
            "name": "Multi-point",
            RESERVED_GEO_LIST_FIELD_NAME: [
                { "lat": 0.0, "lng": 0.0 },
                { "lat": 45.0, "lng": 45.0 }
            ]
        }
    ]);

    test_settings_documents_indexing_swapping_and_search(
        &documents,
        &json!({
            "sortableAttributes": [RESERVED_GEO_LIST_FIELD_NAME],
            "filterableAttributes": [RESERVED_GEO_LIST_FIELD_NAME]
        }),
        &json!({
            "sort": ["_geoPoint(0.0, 0.0):asc"]
        }),
        |response, code| {
            assert_eq!(code, 200, "{response}");
            let hits = &response["hits"];
            // The closest point is (0,0) so distance should be 0
            assert_eq!(hits[0]["_geoDistance"], 0);
        },
    )
    .await;
}

#[actix_rt::test]
async fn geo_list_only_in_filterable_not_sortable() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _) = index
        .add_documents(
            json!([
                {
                    "id": 1,
                    "_geo_list": [{ "lat": 48.8566, "lng": 2.3522 }]
                }
            ]),
            None,
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Only add to filterableAttributes, not sortableAttributes
    let (task, _) = index
        .update_settings(json!({
            "filterableAttributes": ["_geo_list"]
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    // Filtering should work
    index
        .search(json!({ "filter": "_geoRadius(48.8566, 2.3522, 100)" }), |response, code| {
            assert_eq!(code, 200, "{response}");
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;

    // Sorting should fail (not in sortableAttributes)
    index
        .search(json!({ "sort": ["_geoPoint(48.8566, 2.3522):asc"] }), |response, code| {
            assert_eq!(code, 400, "{response}");
        })
        .await;
}
