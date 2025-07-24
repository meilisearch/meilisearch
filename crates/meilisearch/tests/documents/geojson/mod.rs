use crate::{common::Server, json};
use meili_snap::{json_string, snapshot};

const LILLE: &str = include_str!("assets/lille.geojson");

#[actix_rt::test]
async fn basic_add_settings_and_geojson_documents() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _status_code) =
        index.update_settings(json!({"filterableAttributes": ["_geojson"]})).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, _) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    snapshot!(response,
    @r#"
    {
      "hits": [],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0
    }
    "#);

    let lille: serde_json::Value = serde_json::from_str(LILLE).unwrap();
    let documents = json!([
        {
            "id": "missing",
        },
        {
            "id": "point",
            "_geojson": { "type": "Point", "coordinates": [1, 1] },
        },
        {
            "id": "lille",
            "_geojson": lille,
        },
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    let response = server.wait_task(task.uid()).await.succeeded();
    snapshot!(json_string!(response, { ".uid" => "[uid]", ".batchUid" => "[batch_uid]", ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 3,
        "indexedDocuments": 3
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    let (response, code) = index.get_all_documents_raw("?ids=missing,point").await;

    snapshot!(code, @"200 OK");
    snapshot!(response,
    @r#"
    {
      "results": [
        {
          "id": "missing"
        },
        {
          "id": "point",
          "_geojson": {
            "type": "Point",
            "coordinates": [
              1,
              1
            ]
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "#);

    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    snapshot!(response,
    @r#"
    {
      "hits": [
        {
          "id": "point",
          "_geojson": {
            "type": "Point",
            "coordinates": [
              1,
              1
            ]
          }
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1
    }
    "#);
}

#[actix_rt::test]
async fn basic_add_geojson_documents_and_settings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let lille: serde_json::Value = serde_json::from_str(LILLE).unwrap();
    let documents = json!([
        {
            "id": "missing",
        },
        {
            "id": "point",
            "_geojson": { "type": "Point", "coordinates": [1, 1] },
        },
        {
            "id": "lille",
            "_geojson": lille,
        },
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    let response = server.wait_task(task.uid()).await.succeeded();
    snapshot!(response,
        @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 3,
        "indexedDocuments": 3
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    snapshot!(response,
    @r#"
    {
      "message": "Index `[uuid]`: Attribute `_geojson` is not filterable. This index does not have configured filterable attributes.\n14:15 _geoPolygon([0,0],[2,0],[2,2],[0,2])",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "#);

    let (task, _status_code) =
        index.update_settings(json!({"filterableAttributes": ["_geojson"]})).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    snapshot!(response,
    @r#"
    {
      "hits": [
        {
          "id": "point",
          "_geojson": {
            "type": "Point",
            "coordinates": [
              1,
              1
            ]
          }
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1
    }
    "#);
}

#[actix_rt::test]
async fn add_and_remove_geojson() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.update_settings(json!({"filterableAttributes": ["_geojson"]})).await;

    let documents = json!([
        {
            "id": "missing",
        },
        {
            "id": 0,
            "_geojson": { "type": "Point", "coordinates": [1, 1] },
        }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, _code) =
        index.search_get("?filter=_geoPolygon([0,0],[0.9,0],[0.9,0.9],[0,0.9])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 0);
    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 1);

    let (task, _) = index.delete_document(0).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, _code) =
        index.search_get("?filter=_geoPolygon([0,0],[0.9,0],[0.9,0.9],[0,0.9])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 0);
    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 0);

    // add it back
    let documents = json!([
        {
            "id": 0,
            "_geojson": { "type": "Point", "coordinates": [1, 1] },
        }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, _code) =
        index.search_get("?filter=_geoPolygon([0,0],[0.9,0],[0.9,0.9],[0,0.9])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 0);
    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 1);
}

#[actix_rt::test]
async fn partial_update_geojson() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _) = index.update_settings(json!({"filterableAttributes": ["_geojson"]})).await;
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        {
            "id": 0,
            "_geojson": { "type": "Point", "coordinates": [1, 1] },
        }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, _code) =
        index.search_get("?filter=_geoPolygon([0,0],[0.9,0],[0.9,0.9],[0,0.9])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 0);
    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 1);

    let documents = json!([
        {
            "id": 0,
            "_geojson": { "type": "Point", "coordinates": [0.5, 0.5] },
        }
    ]);
    let (task, _status_code) = index.update_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, _code) =
        index.search_get("?filter=_geoPolygon([0,0],[0.9,0],[0.9,0.9],[0,0.9])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 1);
    let (response, _code) = index.search_get("?filter=_geoPolygon([0,0],[2,0],[2,2],[0,2])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 1);
    let (response, _code) =
        index.search_get("?filter=_geoPolygon([0.9,0.9],[2,0.9],[2,2],[0.9,2])").await;
    assert_eq!(response.get("hits").unwrap().as_array().unwrap().len(), 0);
}
