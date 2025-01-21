use meilisearch_types::Document;
use serde_json::json;

use crate::search::insert_geo_distance;

#[test]
fn test_insert_geo_distance() {
    let value: Document = serde_json::from_str(
        r#"{
          "_geo": {
            "lat": 50.629973371633746,
            "lng": 3.0569447399419567
          },
          "city": "Lille",
          "id": "1"
        }"#,
    )
    .unwrap();

    let sorters = &["_geoPoint(50.629973371633746,3.0569447399419567):desc".to_string()];
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

    let sorters = &["_geoPoint(50.629973371633746, 3.0569447399419567):asc".to_string()];
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

    let sorters = &["_geoPoint(   50.629973371633746   ,  3.0569447399419567   ):desc".to_string()];
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

    let sorters = &[
        "prix:asc",
        "villeneuve:desc",
        "_geoPoint(50.629973371633746, 3.0569447399419567):asc",
        "ubu:asc",
    ]
    .map(|s| s.to_string());
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

    // only the first geoPoint is used to compute the distance
    let sorters = &[
        "chien:desc",
        "_geoPoint(50.629973371633746, 3.0569447399419567):asc",
        "pangolin:desc",
        "_geoPoint(100.0, -80.0):asc",
        "chat:asc",
    ]
    .map(|s| s.to_string());
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

    // there was no _geoPoint so nothing is inserted in the document
    let sorters = &["chien:asc".to_string()];
    let mut document = value;
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), None);
}

#[test]
fn test_insert_geo_distance_with_coords_as_string() {
    let value: Document = serde_json::from_str(
        r#"{
          "_geo": {
            "lat": "50",
            "lng": 3
          }
        }"#,
    )
    .unwrap();

    let sorters = &["_geoPoint(50,3):desc".to_string()];
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

    let value: Document = serde_json::from_str(
        r#"{
          "_geo": {
            "lat": "50",
            "lng": "3"
          },
          "id": "1"
        }"#,
    )
    .unwrap();

    let sorters = &["_geoPoint(50,3):desc".to_string()];
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));

    let value: Document = serde_json::from_str(
        r#"{
          "_geo": {
            "lat": 50,
            "lng": "3"
          },
          "id": "1"
        }"#,
    )
    .unwrap();

    let sorters = &["_geoPoint(50,3):desc".to_string()];
    let mut document = value.clone();
    insert_geo_distance(sorters, &mut document);
    assert_eq!(document.get("_geoDistance"), Some(&json!(0)));
}
