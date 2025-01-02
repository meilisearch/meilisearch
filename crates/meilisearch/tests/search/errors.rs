use meili_snap::*;

use crate::common::{shared_does_not_exists_index, Server};
use crate::json;

#[actix_rt::test]
async fn search_unexisting_index() {
    let index = shared_does_not_exists_index().await;
    let expected_response = json!({
        "message": "Index `DOES_NOT_EXISTS` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    index
        .search(json!({"q": "hello"}), |response, code| {
            assert_eq!(code, 404);
            assert_eq!(response, expected_response);
        })
        .await;
}

#[actix_rt::test]
async fn search_unexisting_parameter() {
    let server = Server::new_shared();
    let index = server.unique_index();

    index
        .search(json!({"marin": "hello"}), |response, code| {
            assert_eq!(code, 400, "{}", response);
            assert_eq!(response["code"], "bad_request");
        })
        .await;
}

#[actix_rt::test]
async fn search_bad_q() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"q": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.q`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_search_q",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_q"
    }
    "###);
    // Can't make the `q` fail with a get search since it'll accept anything as a string.
}

#[actix_rt::test]
async fn search_bad_offset() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"offset": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.offset`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_search_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_offset"
    }
    "###);

    let (response, code) = index.search_get("?offset=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `doggo` as a positive integer",
      "code": "invalid_search_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_offset"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_limit() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"limit": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.limit`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_search_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_limit"
    }
    "###);

    let (response, code) = index.search_get("?limit=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_search_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_limit"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_page() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"page": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.page`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_search_page",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_page"
    }
    "###);

    let (response, code) = index.search_get("?page=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `page`: could not parse `doggo` as a positive integer",
      "code": "invalid_search_page",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_page"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_hits_per_page() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"hitsPerPage": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.hitsPerPage`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_search_hits_per_page",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_hits_per_page"
    }
    "###);

    let (response, code) = index.search_get("?hitsPerPage=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `hitsPerPage`: could not parse `doggo` as a positive integer",
      "code": "invalid_search_hits_per_page",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_hits_per_page"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_attributes_to_retrieve() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"attributesToRetrieve": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.attributesToRetrieve`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_search_attributes_to_retrieve",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_attributes_to_retrieve"
    }
    "###);
    // Can't make the `attributes_to_retrieve` fail with a get search since it'll accept anything as an array of strings.
}

#[actix_rt::test]
async fn search_bad_retrieve_vectors() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"retrieveVectors": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.retrieveVectors`: expected a boolean, but found a string: `\"doggo\"`",
      "code": "invalid_search_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_retrieve_vectors"
    }
    "###);

    let (response, code) = index.search_post(json!({"retrieveVectors": [true]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.retrieveVectors`: expected a boolean, but found an array: `[true]`",
      "code": "invalid_search_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_retrieve_vectors"
    }
    "###);

    let (response, code) = index.search_get("?retrieveVectors=").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `retrieveVectors`: could not parse `` as a boolean, expected either `true` or `false`",
      "code": "invalid_search_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_retrieve_vectors"
    }
    "###);

    let (response, code) = index.search_get("?retrieveVectors=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `retrieveVectors`: could not parse `doggo` as a boolean, expected either `true` or `false`",
      "code": "invalid_search_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_retrieve_vectors"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_attributes_to_crop() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"attributesToCrop": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.attributesToCrop`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_search_attributes_to_crop",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_attributes_to_crop"
    }
    "###);
    // Can't make the `attributes_to_crop` fail with a get search since it'll accept anything as an array of strings.
}

#[actix_rt::test]
async fn search_bad_crop_length() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"cropLength": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.cropLength`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_search_crop_length",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_crop_length"
    }
    "###);

    let (response, code) = index.search_get("?cropLength=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `cropLength`: could not parse `doggo` as a positive integer",
      "code": "invalid_search_crop_length",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_crop_length"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_attributes_to_highlight() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"attributesToHighlight": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.attributesToHighlight`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_search_attributes_to_highlight",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_attributes_to_highlight"
    }
    "###);
    // Can't make the `attributes_to_highlight` fail with a get search since it'll accept anything as an array of strings.
}

#[actix_rt::test]
async fn search_bad_filter() {
    // Since a filter is deserialized as a json Value it will never fail to deserialize.
    // Thus the error message is not generated by deserr but written by us.
    let server = Server::new_shared();
    let index = server.unique_index();
    // Also, to trigger the error message we need to effectively create the index or else it'll throw an
    // index does not exists error.
    let (response, _code) = index.create(None).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.search_post(json!({ "filter": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid syntax for the filter parameter: `expected String, Array, found: true`.",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "###);
    // Can't make the `filter` fail with a get search since it'll accept anything as a strings.
}

#[actix_rt::test]
async fn search_bad_sort() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"sort": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.sort`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_search_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    }
    "###);
    // Can't make the `sort` fail with a get search since it'll accept anything as a strings.
}

#[actix_rt::test]
async fn search_bad_show_matches_position() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"showMatchesPosition": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.showMatchesPosition`: expected a boolean, but found a string: `\"doggo\"`",
      "code": "invalid_search_show_matches_position",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_show_matches_position"
    }
    "###);

    let (response, code) = index.search_get("?showMatchesPosition=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `showMatchesPosition`: could not parse `doggo` as a boolean, expected either `true` or `false`",
      "code": "invalid_search_show_matches_position",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_show_matches_position"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_facets() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"facets": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.facets`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
    // Can't make the `attributes_to_highlight` fail with a get search since it'll accept anything as an array of strings.
}

#[actix_rt::test]
async fn search_bad_threshold() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"rankingScoreThreshold": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.rankingScoreThreshold`: expected a number, but found a string: `\"doggo\"`",
      "code": "invalid_search_ranking_score_threshold",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_ranking_score_threshold"
    }
    "###);
}

#[actix_rt::test]
async fn search_invalid_threshold() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"rankingScoreThreshold": 42})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.rankingScoreThreshold`: the value of `rankingScoreThreshold` is invalid, expected a float between `0.0` and `1.0`.",
      "code": "invalid_search_ranking_score_threshold",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_ranking_score_threshold"
    }
    "###);
}

#[actix_rt::test]
async fn search_non_filterable_facets() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    // Wait for the settings update to complete
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.search_post(json!({"facets": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, attribute `doggo` is not filterable. The available filterable attribute is `title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);

    let (response, code) = index.search_get("?facets=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, attribute `doggo` is not filterable. The available filterable attribute is `title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn search_non_filterable_facets_multiple_filterable() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _code) =
        index.update_settings(json!({"filterableAttributes": ["title", "genres"]})).await;
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.search_post(json!({"facets": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, attribute `doggo` is not filterable. The available filterable attributes are `genres, title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);

    let (response, code) = index.search_get("?facets=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, attribute `doggo` is not filterable. The available filterable attributes are `genres, title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn search_non_filterable_facets_no_filterable() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _code) = index.update_settings(json!({"filterableAttributes": []})).await;
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.search_post(json!({"facets": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, this index does not have configured filterable attributes.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);

    let (response, code) = index.search_get("?facets=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, this index does not have configured filterable attributes.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn search_non_filterable_facets_multiple_facets() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _uid) =
        index.update_settings(json!({"filterableAttributes": ["title", "genres"]})).await;
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.search_post(json!({"facets": ["doggo", "neko"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, attributes `doggo, neko` are not filterable. The available filterable attributes are `genres, title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);

    let (response, code) = index.search_get("?facets=doggo,neko").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid facet distribution, attributes `doggo, neko` are not filterable. The available filterable attributes are `genres, title`.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn search_bad_highlight_pre_tag() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"highlightPreTag": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.highlightPreTag`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_search_highlight_pre_tag",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_highlight_pre_tag"
    }
    "###);
    // Can't make the `highlight_pre_tag` fail with a get search since it'll accept anything as a strings.
}

#[actix_rt::test]
async fn search_bad_highlight_post_tag() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"highlightPostTag": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.highlightPostTag`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_search_highlight_post_tag",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_highlight_post_tag"
    }
    "###);
    // Can't make the `highlight_post_tag` fail with a get search since it'll accept anything as a strings.
}

#[actix_rt::test]
async fn search_bad_crop_marker() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"cropMarker": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.cropMarker`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_search_crop_marker",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_crop_marker"
    }
    "###);
    // Can't make the `crop_marker` fail with a get search since it'll accept anything as a strings.
}

#[actix_rt::test]
async fn search_bad_matching_strategy() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index.search_post(json!({"matchingStrategy": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown value `doggo` at `.matchingStrategy`: expected one of `last`, `all`, `frequency`",
      "code": "invalid_search_matching_strategy",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_matching_strategy"
    }
    "###);

    let (response, code) = index.search_post(json!({"matchingStrategy": {"doggo": "doggo"}})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.matchingStrategy`: expected a string, but found an object: `{\"doggo\":\"doggo\"}`",
      "code": "invalid_search_matching_strategy",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_matching_strategy"
    }
    "###);

    let (response, code) = index.search_get("?matchingStrategy=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown value `doggo` for parameter `matchingStrategy`: expected one of `last`, `all`, `frequency`",
      "code": "invalid_search_matching_strategy",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_matching_strategy"
    }
    "###);
}

#[actix_rt::test]
async fn filter_invalid_syntax_object() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"filter": "title & Glass"}), |response, code| {
            snapshot!(response, @r###"
            {
              "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `title & Glass`.\n1:14 title & Glass",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
            snapshot!(code, @"400 Bad Request");
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_syntax_array() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"filter": ["title & Glass"]}), |response, code| {
            snapshot!(response, @r###"
            {
              "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `title & Glass`.\n1:14 title & Glass",
              "code": "invalid_search_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
            }
            "###);
            snapshot!(code, @"400 Bad Request");
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_syntax_string() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "Found unexpected characters at the end of the filter: `XOR title = Glass`. You probably forgot an `OR` or an `AND` rule.\n15:32 title = Glass XOR title = Glass",
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": "title = Glass XOR title = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_array() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `many` is not filterable. Available filterable attributes are: `title`.\n1:5 many = Glass", index.uid),
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": ["many = Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_string() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `many` is not filterable. Available filterable attributes are: `title`.\n1:5 many = Glass", index.uid),
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": "many = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_attribute_array() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:13 _geo = Glass",
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": ["_geo = Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_attribute_string() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:13 _geo = Glass",
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": "_geo = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_attribute_array() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:21 _geoDistance = Glass",
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": ["_geoDistance = Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_attribute_string() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
       "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:21 _geoDistance = Glass",
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": "_geoDistance = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_point_array() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:18 _geoPoint = Glass",
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": ["_geoPoint = Glass"]}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_point_string() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
       "message": "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:18 _geoPoint = Glass",
        "code": "invalid_search_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    });
    index
        .search(json!({"filter": "_geoPoint = Glass"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;
}

#[actix_rt::test]
async fn sort_geo_reserved_attribute() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"sortableAttributes": ["id"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a sort expression. Use the _geoPoint(latitude, longitude) built-in rule to sort on _geo field coordinates.",
        "code": "invalid_search_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    });
    index
        .search(
            json!({
                "sort": ["_geo:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_reserved_attribute() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (task, _code) = index.update_settings(json!({"sortableAttributes": ["id"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a sort expression.",
        "code": "invalid_search_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    });
    index
        .search(
            json!({
                "sort": ["_geoDistance:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_unsortable_attribute() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _code) = index.update_settings(json!({"sortableAttributes": ["id"]})).await;
    index.wait_task(response.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `title` is not sortable. Available sortable attributes are: `id`.", index.uid),
        "code": "invalid_search_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    });
    index
        .search(
            json!({
                "sort": ["title:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_invalid_syntax() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, _code) = index.update_settings(json!({"sortableAttributes": ["id"]})).await;
    index.wait_task(response.uid()).await.succeeded();

    let expected_response = json!({
        "message": "Invalid syntax for the sort parameter: expected expression ending by `:asc` or `:desc`, found `title`.",
        "code": "invalid_search_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    });
    index
        .search(
            json!({
                "sort": ["title"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn sort_unset_ranking_rule() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, _code) = index
        .update_settings(
            json!({"sortableAttributes": ["title"], "rankingRules": ["proximity", "exactness"]}),
        )
        .await;
    index.wait_task(response.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: You must specify where `sort` is listed in the rankingRules setting to use the sort parameter at search time.", index.uid),
        "code": "invalid_search_sort",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    });
    index
        .search(
            json!({
                "sort": ["title:asc"]
            }),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_on_unknown_field() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _code) =
        index.update_settings_searchable_attributes(json!(["id", "title"])).await;
    index.wait_task(response.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `unknown` is not searchable. Available searchable attributes are: `id, title`.", index.uid),
        "code": "invalid_search_attributes_to_search_on",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_attributes_to_search_on"
    });
    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["unknown"]}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_on_unknown_field_plus_joker() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (response, _code) =
        index.update_settings_searchable_attributes(json!(["id", "title"])).await;
    index.wait_task(response.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `unknown` is not searchable. Available searchable attributes are: `id, title`.", index.uid),
        "code": "invalid_search_attributes_to_search_on",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_attributes_to_search_on"
    });
    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["*", "unknown"]}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;

    index
        .search(
            json!({"q": "Captain Marvel", "attributesToSearchOn": ["unknown", "*"]}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn distinct_at_search_time() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `doggo.truc` is not filterable and thus, cannot be used as distinct attribute. This index does not have configured filterable attributes.", index.uid),
        "code": "invalid_search_distinct",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_distinct"
    });
    let (response, code) =
        index.search_post(json!({"page": 0, "hitsPerPage": 2, "distinct": "doggo.truc"})).await;
    assert_eq!(response, expected_response);
    assert_eq!(code, 400);

    let (task, _) = index.update_settings_filterable_attributes(json!(["color", "machin"])).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `doggo.truc` is not filterable and thus, cannot be used as distinct attribute. Available filterable attributes are: `color, machin`.", index.uid),
        "code": "invalid_search_distinct",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_distinct"
    });
    let (response, code) =
        index.search_post(json!({"page": 0, "hitsPerPage": 2, "distinct": "doggo.truc"})).await;
    assert_eq!(response, expected_response);
    assert_eq!(code, 400);

    let (task, _) = index.update_settings_displayed_attributes(json!(["color"])).await;
    index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": format!("Index `{}`: Attribute `doggo.truc` is not filterable and thus, cannot be used as distinct attribute. Available filterable attributes are: `color, <..hidden-attributes>`.", index.uid),
        "code": "invalid_search_distinct",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_search_distinct"
    });
    let (response, code) =
        index.search_post(json!({"page": 0, "hitsPerPage": 2, "distinct": "doggo.truc"})).await;
    assert_eq!(response, expected_response);
    assert_eq!(code, 400);

    let (response, code) =
        index.search_post(json!({"page": 0, "hitsPerPage": 2, "distinct": true})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.distinct`: expected a string, but found a boolean: `true`",
      "code": "invalid_search_distinct",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_distinct"
    }
    "###);
}

#[actix_rt::test]
async fn search_with_contains_without_enabling_the_feature() {
    // Since a filter is deserialized as a json Value it will never fail to deserialize.
    // Thus the error message is not generated by deserr but written by us.
    let server = Server::new_shared();
    let index = server.unique_index();
    // Also, to trigger the error message we need to effectively create the index or else it'll throw an
    // index does not exists error.
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({ "filter": "doggo CONTAINS kefir" }), |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Using `CONTAINS` or `STARTS WITH` in a filter requires enabling the `contains filter` experimental feature. See https://github.com/orgs/meilisearch/discussions/763\n7:15 doggo CONTAINS kefir",
              "code": "feature_not_enabled",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
            }
            "###);
        })
        .await;
    index
        .search(json!({ "filter": "doggo != echo AND doggo CONTAINS kefir" }), |response, code| {
            snapshot!(code, @"400 Bad Request");
            snapshot!(json_string!(response), @r###"
            {
              "message": "Using `CONTAINS` or `STARTS WITH` in a filter requires enabling the `contains filter` experimental feature. See https://github.com/orgs/meilisearch/discussions/763\n25:33 doggo != echo AND doggo CONTAINS kefir",
              "code": "feature_not_enabled",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
            }
            "###);
        })
        .await;

    // For the post search we can also use the arrays syntaxes
    let (response, code) =
        index.search_post(json!({ "filter": ["doggo != echo", "doggo CONTAINS kefir"] })).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Using `CONTAINS` or `STARTS WITH` in a filter requires enabling the `contains filter` experimental feature. See https://github.com/orgs/meilisearch/discussions/763\n7:15 doggo CONTAINS kefir",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
    let (response, code) =
        index.search_post(json!({ "filter": ["doggo != echo", ["doggo CONTAINS kefir"]] })).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Using `CONTAINS` or `STARTS WITH` in a filter requires enabling the `contains filter` experimental feature. See https://github.com/orgs/meilisearch/discussions/763\n7:15 doggo CONTAINS kefir",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}
