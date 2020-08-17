use std::convert::Into;

use serde_json::json;
use serde_json::Value;
use std::sync::Mutex;
use std::cell::RefCell;

#[macro_use] mod common;

#[actix_rt::test]
async fn placeholder_search_with_limit() {
    let mut server = common::Server::test_server().await;

    let query = json! ({
        "limit": 3
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 200);
        assert_eq!(response["hits"].as_array().unwrap().len(), 3);
    });
}

#[actix_rt::test]
async fn placeholder_search_with_offset() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "limit": 6,
    });

    // hack to take a value out of macro (must implement UnwindSafe)
    let expected = Mutex::new(RefCell::new(Vec::new()));

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 200);
        // take results at offset 3 as reference
        let lock = expected.lock().unwrap();
        lock.replace(response["hits"].as_array().unwrap()[3..6].iter().cloned().collect());
    });

    let expected = expected.into_inner().unwrap().into_inner();

    let query = json!({
        "limit": 3,
        "offset": 3,
    });
    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 200);
        let response = response["hits"].as_array().unwrap();
        assert_eq!(&expected, response);
    });
}

#[actix_rt::test]
async fn placeholder_search_with_attribute_to_highlight_wildcard() {
    // there should be no highlight in placeholder search
    let mut server = common::Server::test_server().await;

    let query = json!({
        "limit": 1,
        "attributesToHighlight": ["*"]
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 200);
        let result = response["hits"]
            .as_array()
            .unwrap()[0]
            .as_object()
            .unwrap();
        for value in result.values() {
            assert!(value.to_string().find("<em>").is_none());
        }
    });
}

#[actix_rt::test]
async fn placeholder_search_with_matches() {
    // matches is always empty
    let mut server = common::Server::test_server().await;

    let query = json!({
        "matches": true
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 200);
        let result = response["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_object().unwrap()["_matchesInfo"].clone())
            .all(|m| m.as_object().unwrap().is_empty());
        assert!(result);
    });
}

#[actix_rt::test]
async fn placeholder_search_witch_crop() {
    // placeholder search crop always crop from beggining
    let mut server = common::Server::test_server().await;

    let query = json!({
        "attributesToCrop": ["about"],
        "cropLength": 20
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 200);

        let hits = response["hits"].as_array().unwrap();

        for hit in hits {
            let hit = hit.as_object().unwrap();
            let formatted = hit["_formatted"].as_object().unwrap();

            let about = hit["about"].as_str().unwrap();
            let about_formatted = formatted["about"].as_str().unwrap();
            // the formatted about length should be about 20 characters long
            assert!(about_formatted.len() < 20 + 10);
            // the formatted part should be located at the beginning of the original one
            assert_eq!(about.find(&about_formatted).unwrap(), 0);
        }
    });
}

#[actix_rt::test]
async fn placeholder_search_with_attributes_to_retrieve() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "limit": 1,
        "attributesToRetrieve": ["gender", "about"],
    });

    test_post_get_search!(server, query, |response, _status_code| {
        let hit = response["hits"]
            .as_array()
            .unwrap()[0]
            .as_object()
            .unwrap();
        assert_eq!(hit.values().count(), 2);
        let _ = hit["gender"];
        let _ = hit["about"];
    });
}

#[actix_rt::test]
async fn placeholder_search_with_filter() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "filters": "color='green'"
    });

    test_post_get_search!(server, query, |response, _status_code| {
        let hits = response["hits"].as_array().unwrap();
        assert!(hits.iter().all(|v| v["color"].as_str().unwrap() == "Green"));
    });

    let query = json!({
        "filters": "tags=bug"
    });

    test_post_get_search!(server, query, |response, _status_code| {
        let hits = response["hits"].as_array().unwrap();
        let value = Value::String(String::from("bug"));
        assert!(hits.iter().all(|v| v["tags"].as_array().unwrap().contains(&value)));
    });

    let query = json!({
        "filters": "color='green' AND (tags='bug' OR tags='wontfix')"
    });
    test_post_get_search!(server, query, |response, _status_code| {
        let hits = response["hits"].as_array().unwrap();
        let bug = Value::String(String::from("bug"));
        let wontfix = Value::String(String::from("wontfix"));
        assert!(hits.iter().all(|v|
                v["color"].as_str().unwrap() == "Green" &&
                v["tags"].as_array().unwrap().contains(&bug) ||
                v["tags"].as_array().unwrap().contains(&wontfix)));
    });
}

#[actix_rt::test]
async fn placeholder_test_faceted_search_valid() {
    let mut server = common::Server::test_server().await;

    // simple tests on attributes with string value
    let body = json!({
        "attributesForFaceting": ["color"]
    });

    server.update_all_settings(body).await;

    let query = json!({
        "facetFilters": ["color:green"]
    });

    test_post_get_search!(server, query, |response, _status_code| {
        assert!(!response.get("hits").unwrap().as_array().unwrap().is_empty());
        assert!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|value| value.get("color").unwrap() == "Green"));
    });

    let query = json!({
        "facetFilters": [["color:blue"]]
    });

    test_post_get_search!(server, query, |response, _status_code| {
        assert!(!response.get("hits").unwrap().as_array().unwrap().is_empty());
        assert!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|value| value.get("color").unwrap() == "blue"));
    });

    let query = json!({
        "facetFilters": ["color:Blue"]
    });

    test_post_get_search!(server, query, |response, _status_code| {
        assert!(!response.get("hits").unwrap().as_array().unwrap().is_empty());
        assert!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|value| value.get("color").unwrap() == "blue"));
    });

    // test on arrays: ["tags:bug"]
    let body = json!({
        "attributesForFaceting": ["color", "tags"]
    });

    server.update_all_settings(body).await;

    let query = json!({
        "facetFilters": ["tags:bug"]
    });
    test_post_get_search!(server, query, |response, _status_code| {
        assert!(!response.get("hits").unwrap().as_array().unwrap().is_empty());
        assert!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|value| value.get("tags").unwrap().as_array().unwrap().contains(&Value::String("bug".to_owned()))));
    });

    // test and: ["color:blue", "tags:bug"]
    let query = json!({
        "facetFilters": ["color:blue", "tags:bug"]
    });
    test_post_get_search!(server, query, |response, _status_code| {
        assert!(!response.get("hits").unwrap().as_array().unwrap().is_empty());
        assert!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|value| value
                .get("color")
                .unwrap() == "blue"
                && value.get("tags").unwrap().as_array().unwrap().contains(&Value::String("bug".to_owned()))));
    });

    // test or: [["color:blue", "color:green"]]
    let query = json!({
        "facetFilters": [["color:blue", "color:green"]]
    });
    test_post_get_search!(server, query, |response, _status_code| {
        assert!(!response.get("hits").unwrap().as_array().unwrap().is_empty());
        assert!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|value|
                value
                .get("color")
                .unwrap() == "blue"
                || value
                .get("color")
                .unwrap() == "Green"));
    });
    // test and-or: ["tags:bug", ["color:blue", "color:green"]]
    let query = json!({
        "facetFilters": ["tags:bug", ["color:blue", "color:green"]]
    });
    test_post_get_search!(server, query, |response, _status_code| {
        assert!(!response.get("hits").unwrap().as_array().unwrap().is_empty());
        assert!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|value|
                value
                .get("tags")
                .unwrap()
                .as_array()
                .unwrap()
                .contains(&Value::String("bug".to_owned()))
                && (value
                    .get("color")
                    .unwrap() == "blue"
                    || value
                    .get("color")
                    .unwrap() == "Green")));

    });
}

#[actix_rt::test]
async fn placeholder_test_faceted_search_invalid() {
    let mut server = common::Server::test_server().await;

    //no faceted attributes set
    let query = json!({
        "facetFilters": ["color:blue"]
    });
    test_post_get_search!(server, query, |_response, status_code| assert_ne!(status_code, 202));

    let body = json!({
        "attributesForFaceting": ["color", "tags"]
    });
    server.update_all_settings(body).await;
    // empty arrays are error
    // []
    let query = json!({
        "facetFilters": []
    });
    test_post_get_search!(server, query, |_response, status_code| assert_ne!(status_code, 202));
    // [[]]
    let query = json!({
        "facetFilters": [[]]
    });
    test_post_get_search!(server, query, |_response, status_code| assert_ne!(status_code, 202));
    // ["color:green", []]
    let query = json!({
        "facetFilters": ["color:green", []]
    });
    test_post_get_search!(server, query, |_response, status_code| assert_ne!(status_code, 202));

    // too much depth
    // [[[]]]
    let query = json!({
        "facetFilters": [[[]]]
    });
    test_post_get_search!(server, query, |_response, status_code| assert_ne!(status_code, 202));
    // [["color:green", ["color:blue"]]]
    let query = json!({
        "facetFilters": [["color:green", ["color:blue"]]]
    });
    test_post_get_search!(server, query, |_response, status_code| assert_ne!(status_code, 202));
    // "color:green"
    let query = json!({
        "facetFilters": "color:green"
    });
    test_post_get_search!(server, query, |_response, status_code| assert_ne!(status_code, 202));
}

#[actix_rt::test]
async fn placeholder_test_facet_count() {
    let mut server = common::Server::test_server().await;

    // test without facet distribution
    let query = json!({
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert!(response.get("exhaustiveFacetsCount").is_none());
        assert!(response.get("facetsDistribution").is_none());
    });

    // test no facets set, search on color
    let query = json!({
        "facetsDistribution": ["color"]
    });
    test_post_get_search!(server, query.clone(), |_response, status_code|{
        assert_eq!(status_code, 400);
    });

    let body = json!({
        "attributesForFaceting": ["color", "tags"]
    });
    server.update_all_settings(body).await;
    // same as before, but now facets are set:
    test_post_get_search!(server, query, |response, _status_code|{
        println!("{}", response);
        assert!(response.get("exhaustiveFacetsCount").is_some());
        assert_eq!(response.get("facetsDistribution").unwrap().as_object().unwrap().values().count(), 1);
    });
    // searching on color and tags
    let query = json!({
        "facetsDistribution": ["color", "tags"]
    });
    test_post_get_search!(server, query, |response, _status_code|{
        let facets = response.get("facetsDistribution").unwrap().as_object().unwrap();
        assert_eq!(facets.values().count(), 2);
        assert_ne!(!facets.get("color").unwrap().as_object().unwrap().values().count(), 0);
        assert_ne!(!facets.get("tags").unwrap().as_object().unwrap().values().count(), 0);
    });
    // wildcard
    let query = json!({
        "facetsDistribution": ["*"]
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert_eq!(response.get("facetsDistribution").unwrap().as_object().unwrap().values().count(), 2);
    });
    // wildcard with other attributes:
    let query = json!({
        "facetsDistribution": ["color", "*"]
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert_eq!(response.get("facetsDistribution").unwrap().as_object().unwrap().values().count(), 2);
    });

    // empty facet list
    let query = json!({
        "facetsDistribution": []
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert_eq!(response.get("facetsDistribution").unwrap().as_object().unwrap().values().count(), 0);
    });

    // attr not set as facet passed:
    let query = json!({
        "facetsDistribution": ["gender"]
    });
    test_post_get_search!(server, query, |_response, status_code|{
        assert_eq!(status_code, 400);
    });

}

#[actix_rt::test]
#[should_panic]
async fn placeholder_test_bad_facet_distribution() {
    let mut server = common::Server::test_server().await;
    // string instead of array:
    let query = json!({
        "facetsDistribution": "color"
    });
    test_post_get_search!(server, query, |_response, _status_code| {});

    // invalid value in array:
    let query = json!({
        "facetsDistribution": ["color", true]
    });
    test_post_get_search!(server, query, |_response, _status_code| {});
}

#[actix_rt::test]
async fn placeholder_test_sort() {
    let mut server = common::Server::test_server().await;

    let body = json!({
        "rankingRules": ["asc(age)"],
        "attributesForFaceting": ["color"]
    });
    server.update_all_settings(body).await;
    let query = json!({ });
    test_post_get_search!(server, query, |response, _status_code| {
        let hits = response["hits"].as_array().unwrap();
        hits.iter().map(|v| v["age"].as_u64().unwrap()).fold(0, |prev, cur| {
            assert!(cur >= prev);
            cur
        });
    });

    let query = json!({
        "facetFilters": ["color:green"]
    });
    test_post_get_search!(server, query, |response, _status_code| {
        let hits = response["hits"].as_array().unwrap();
        hits.iter().map(|v| v["age"].as_u64().unwrap()).fold(0, |prev, cur| {
            assert!(cur >= prev);
            cur
        });
    });
}

#[actix_rt::test]
async fn placeholder_search_with_empty_query() {
    let mut server = common::Server::test_server().await;

    let query = json! ({
        "q": "",
        "limit": 3
    });

    test_post_get_search!(server, query, |response, status_code| {
        eprintln!("{}", response);
        assert_eq!(status_code, 200);
        assert_eq!(response["hits"].as_array().unwrap().len(), 3);
    });
}
