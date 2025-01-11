use meili_snap::{json_string, snapshot};
use once_cell::sync::Lazy;

use crate::common::{Server, Value};
use crate::json;

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
    let server = Server::new().await;
    let index = server.index("test");

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
                assert_eq!(code, 200, "{}", response);
            },
        )
        .await;
}

#[actix_rt::test]
async fn geo_bounding_box_with_string_and_number() {
    let server = Server::new().await;
    let index = server.index("test");

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
                assert_eq!(code, 200, "{}", response);
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
    let server = Server::new().await;
    let index = server.index("test");

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
                assert_eq!(code, 200, "{}", response);
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
