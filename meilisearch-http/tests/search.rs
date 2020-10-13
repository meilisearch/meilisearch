use std::convert::Into;

use assert_json_diff::assert_json_eq;
use serde_json::json;
use serde_json::Value;

#[macro_use] mod common;

#[actix_rt::test]
async fn search() {
    let mut server = common::Server::test_server().await;

    let query = json! ({
        "q": "exercitation"
    });

    let expected = json!([
        {
          "id": 1,
          "balance": "$1,706.13",
          "picture": "http://placehold.it/32x32",
          "age": 27,
          "color": "Green",
          "name": "Cherry Orr",
          "gender": "female",
          "email": "cherryorr@chorizon.com",
          "phone": "+1 (995) 479-3174",
          "address": "442 Beverly Road, Ventress, New Mexico, 3361",
          "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
          "registered": "2020-03-18T11:12:21 -01:00",
          "latitude": -24.356932,
          "longitude": 27.184808,
          "tags": [
            "new issue",
            "bug"
          ],
          "isActive": true
        },
        {
          "id": 59,
          "balance": "$1,921.58",
          "picture": "http://placehold.it/32x32",
          "age": 31,
          "color": "Green",
          "name": "Harper Carson",
          "gender": "male",
          "email": "harpercarson@chorizon.com",
          "phone": "+1 (912) 430-3243",
          "address": "883 Dennett Place, Knowlton, New Mexico, 9219",
          "about": "Exercitation minim esse proident cillum velit et deserunt incididunt adipisicing minim. Cillum Lorem consectetur laborum id consequat exercitation velit. Magna dolor excepteur sunt deserunt dolor ullamco non sint proident ipsum. Reprehenderit voluptate sit veniam consectetur ea sunt duis labore deserunt ipsum aute. Eiusmod aliqua anim voluptate id duis tempor aliqua commodo sunt. Do officia ea consectetur nostrud eiusmod laborum.\r\n",
          "registered": "2019-12-07T07:33:15 -01:00",
          "latitude": -60.812605,
          "longitude": -27.129016,
          "tags": [
            "bug",
            "new issue"
          ],
          "isActive": true
        },
        {
          "id": 49,
          "balance": "$1,476.39",
          "picture": "http://placehold.it/32x32",
          "age": 28,
          "color": "brown",
          "name": "Maureen Dale",
          "gender": "female",
          "email": "maureendale@chorizon.com",
          "phone": "+1 (984) 538-3684",
          "address": "817 Newton Street, Bannock, Wyoming, 1468",
          "about": "Tempor mollit exercitation excepteur cupidatat reprehenderit ad ex. Nulla laborum proident incididunt quis. Esse laborum deserunt qui anim. Sunt incididunt pariatur cillum anim proident eu ullamco dolor excepteur. Ullamco amet culpa nostrud adipisicing duis aliqua consequat duis non eu id mollit velit. Deserunt ullamco amet in occaecat.\r\n",
          "registered": "2018-04-26T06:04:40 -02:00",
          "latitude": -64.196802,
          "longitude": -117.396238,
          "tags": [
            "wontfix"
          ],
          "isActive": true
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        let hits = response["hits"].as_array().unwrap();
        let hits: Vec<Value> = hits.iter().cloned().take(3).collect();
        assert_json_eq!(expected.clone(), serde_json::to_value(hits).unwrap(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_no_params() {
    let mut server = common::Server::test_server().await;

    let query = json! ({});

    // an empty search should return the 20 first indexed document
    let dataset: Vec<Value> = serde_json::from_slice(include_bytes!("assets/test_set.json")).unwrap();
    let expected: Vec<Value> = dataset.into_iter().take(20).collect();
    let expected: Value = serde_json::to_value(expected).unwrap();

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_in_unexisting_index() {
    let mut server = common::Server::with_uid("test");

    let query = json! ({
        "q": "exercitation"
    });

    let expected = json! ({
        "message": "Index test not found",
        "errorCode": "index_not_found",
        "errorType": "invalid_request_error",
        "errorLink": "https://docs.meilisearch.com/errors#index_not_found"
      });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(404, status_code);
        assert_json_eq!(expected.clone(), response.clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_unexpected_params() {

    let query = json! ({"lol": "unexpected"});

    let expected = "unknown field `lol`, expected one of `q`, `offset`, `limit`, `attributesToRetrieve`, `attributesToCrop`, `cropLength`, `attributesToHighlight`, `filters`, `matches`, `facetFilters`, `facetsDistribution` at line 1 column 6";

    let post_query = serde_json::from_str::<meilisearch_http::routes::search::SearchQueryPost>(&query.clone().to_string());
    assert!(post_query.is_err());
    assert_eq!(expected.clone(), post_query.err().unwrap().to_string());

    let get_query: Result<meilisearch_http::routes::search::SearchQuery, _> = serde_json::from_str(&query.clone().to_string());
    assert!(get_query.is_err());
    assert_eq!(expected.clone(), get_query.err().unwrap().to_string());
}

#[actix_rt::test]
async fn search_with_limit() {
    let mut server = common::Server::test_server().await;

    let query = json! ({
        "q": "exercitation",
        "limit": 3
    });

    let expected = json!([
        {
          "id": 1,
          "balance": "$1,706.13",
          "picture": "http://placehold.it/32x32",
          "age": 27,
          "color": "Green",
          "name": "Cherry Orr",
          "gender": "female",
          "email": "cherryorr@chorizon.com",
          "phone": "+1 (995) 479-3174",
          "address": "442 Beverly Road, Ventress, New Mexico, 3361",
          "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
          "registered": "2020-03-18T11:12:21 -01:00",
          "latitude": -24.356932,
          "longitude": 27.184808,
          "tags": [
            "new issue",
            "bug"
          ],
          "isActive": true
        },
        {
          "id": 59,
          "balance": "$1,921.58",
          "picture": "http://placehold.it/32x32",
          "age": 31,
          "color": "Green",
          "name": "Harper Carson",
          "gender": "male",
          "email": "harpercarson@chorizon.com",
          "phone": "+1 (912) 430-3243",
          "address": "883 Dennett Place, Knowlton, New Mexico, 9219",
          "about": "Exercitation minim esse proident cillum velit et deserunt incididunt adipisicing minim. Cillum Lorem consectetur laborum id consequat exercitation velit. Magna dolor excepteur sunt deserunt dolor ullamco non sint proident ipsum. Reprehenderit voluptate sit veniam consectetur ea sunt duis labore deserunt ipsum aute. Eiusmod aliqua anim voluptate id duis tempor aliqua commodo sunt. Do officia ea consectetur nostrud eiusmod laborum.\r\n",
          "registered": "2019-12-07T07:33:15 -01:00",
          "latitude": -60.812605,
          "longitude": -27.129016,
          "tags": [
            "bug",
            "new issue"
          ],
          "isActive": true
        },
        {
          "id": 49,
          "balance": "$1,476.39",
          "picture": "http://placehold.it/32x32",
          "age": 28,
          "color": "brown",
          "name": "Maureen Dale",
          "gender": "female",
          "email": "maureendale@chorizon.com",
          "phone": "+1 (984) 538-3684",
          "address": "817 Newton Street, Bannock, Wyoming, 1468",
          "about": "Tempor mollit exercitation excepteur cupidatat reprehenderit ad ex. Nulla laborum proident incididunt quis. Esse laborum deserunt qui anim. Sunt incididunt pariatur cillum anim proident eu ullamco dolor excepteur. Ullamco amet culpa nostrud adipisicing duis aliqua consequat duis non eu id mollit velit. Deserunt ullamco amet in occaecat.\r\n",
          "registered": "2018-04-26T06:04:40 -02:00",
          "latitude": -64.196802,
          "longitude": -117.396238,
          "tags": [
            "wontfix"
          ],
          "isActive": true
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_offset() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "exercitation",
        "limit": 3,
        "offset": 1
    });

    let expected = json!([
        {
          "id": 59,
          "balance": "$1,921.58",
          "picture": "http://placehold.it/32x32",
          "age": 31,
          "color": "Green",
          "name": "Harper Carson",
          "gender": "male",
          "email": "harpercarson@chorizon.com",
          "phone": "+1 (912) 430-3243",
          "address": "883 Dennett Place, Knowlton, New Mexico, 9219",
          "about": "Exercitation minim esse proident cillum velit et deserunt incididunt adipisicing minim. Cillum Lorem consectetur laborum id consequat exercitation velit. Magna dolor excepteur sunt deserunt dolor ullamco non sint proident ipsum. Reprehenderit voluptate sit veniam consectetur ea sunt duis labore deserunt ipsum aute. Eiusmod aliqua anim voluptate id duis tempor aliqua commodo sunt. Do officia ea consectetur nostrud eiusmod laborum.\r\n",
          "registered": "2019-12-07T07:33:15 -01:00",
          "latitude": -60.812605,
          "longitude": -27.129016,
          "tags": [
            "bug",
            "new issue"
          ],
          "isActive": true
        },
        {
          "id": 49,
          "balance": "$1,476.39",
          "picture": "http://placehold.it/32x32",
          "age": 28,
          "color": "brown",
          "name": "Maureen Dale",
          "gender": "female",
          "email": "maureendale@chorizon.com",
          "phone": "+1 (984) 538-3684",
          "address": "817 Newton Street, Bannock, Wyoming, 1468",
          "about": "Tempor mollit exercitation excepteur cupidatat reprehenderit ad ex. Nulla laborum proident incididunt quis. Esse laborum deserunt qui anim. Sunt incididunt pariatur cillum anim proident eu ullamco dolor excepteur. Ullamco amet culpa nostrud adipisicing duis aliqua consequat duis non eu id mollit velit. Deserunt ullamco amet in occaecat.\r\n",
          "registered": "2018-04-26T06:04:40 -02:00",
          "latitude": -64.196802,
          "longitude": -117.396238,
          "tags": [
            "wontfix"
          ],
          "isActive": true
        },
        {
          "id": 0,
          "balance": "$2,668.55",
          "picture": "http://placehold.it/32x32",
          "age": 36,
          "color": "Green",
          "name": "Lucas Hess",
          "gender": "male",
          "email": "lucashess@chorizon.com",
          "phone": "+1 (998) 478-2597",
          "address": "412 Losee Terrace, Blairstown, Georgia, 2825",
          "about": "Mollit ad in exercitation quis. Anim est ut consequat fugiat duis magna aliquip velit nisi. Commodo eiusmod est consequat proident consectetur aliqua enim fugiat. Aliqua adipisicing laboris elit proident enim veniam laboris mollit. Incididunt fugiat minim ad nostrud deserunt tempor in. Id irure officia labore qui est labore nulla nisi. Magna sit quis tempor esse consectetur amet labore duis aliqua consequat.\r\n",
          "registered": "2016-06-21T09:30:25 -02:00",
          "latitude": -44.174957,
          "longitude": -145.725388,
          "tags": [
            "bug",
            "bug"
          ],
          "isActive": false
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_attribute_to_highlight_wildcard() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToHighlight": ["*"]
    });

    let expected = json!([
        {
          "id": 1,
          "balance": "$1,706.13",
          "picture": "http://placehold.it/32x32",
          "age": 27,
          "color": "Green",
          "name": "Cherry Orr",
          "gender": "female",
          "email": "cherryorr@chorizon.com",
          "phone": "+1 (995) 479-3174",
          "address": "442 Beverly Road, Ventress, New Mexico, 3361",
          "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
          "registered": "2020-03-18T11:12:21 -01:00",
          "latitude": -24.356932,
          "longitude": 27.184808,
          "tags": [
            "new issue",
            "bug"
          ],
          "isActive": true,
          "_formatted": {
            "id": 1,
            "balance": "$1,706.13",
            "picture": "http://placehold.it/32x32",
            "age": 27,
            "color": "Green",
            "name": "<em>Cherry</em> Orr",
            "gender": "female",
            "email": "<em>cherry</em>orr@chorizon.com",
            "phone": "+1 (995) 479-3174",
            "address": "442 Beverly Road, Ventress, New Mexico, 3361",
            "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
            "registered": "2020-03-18T11:12:21 -01:00",
            "latitude": -24.356932,
            "longitude": 27.184808,
            "tags": [
              "new issue",
              "bug"
            ],
            "isActive": true
          }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_attribute_to_highlight_1() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToHighlight": ["name"]
    });

    let expected = json!([
        {
          "id": 1,
          "balance": "$1,706.13",
          "picture": "http://placehold.it/32x32",
          "age": 27,
          "color": "Green",
          "name": "Cherry Orr",
          "gender": "female",
          "email": "cherryorr@chorizon.com",
          "phone": "+1 (995) 479-3174",
          "address": "442 Beverly Road, Ventress, New Mexico, 3361",
          "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
          "registered": "2020-03-18T11:12:21 -01:00",
          "latitude": -24.356932,
          "longitude": 27.184808,
          "tags": [
            "new issue",
            "bug"
          ],
          "isActive": true,
          "_formatted": {
            "id": 1,
            "balance": "$1,706.13",
            "picture": "http://placehold.it/32x32",
            "age": 27,
            "color": "Green",
            "name": "<em>Cherry</em> Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "phone": "+1 (995) 479-3174",
            "address": "442 Beverly Road, Ventress, New Mexico, 3361",
            "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
            "registered": "2020-03-18T11:12:21 -01:00",
            "latitude": -24.356932,
            "longitude": 27.184808,
            "tags": [
              "new issue",
              "bug"
            ],
            "isActive": true
          }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_matches() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "matches": true
    });

    let expected = json!([
        {
          "id": 1,
          "balance": "$1,706.13",
          "picture": "http://placehold.it/32x32",
          "age": 27,
          "color": "Green",
          "name": "Cherry Orr",
          "gender": "female",
          "email": "cherryorr@chorizon.com",
          "phone": "+1 (995) 479-3174",
          "address": "442 Beverly Road, Ventress, New Mexico, 3361",
          "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
          "registered": "2020-03-18T11:12:21 -01:00",
          "latitude": -24.356932,
          "longitude": 27.184808,
          "tags": [
            "new issue",
            "bug"
          ],
          "isActive": true,
          "_matchesInfo": {
            "name": [
              {
                "start": 0,
                "length": 6
              }
            ],
            "email": [
              {
                "start": 0,
                "length": 6
              }
            ]
          }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_crop() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "exercitation",
        "limit": 1,
        "attributesToCrop": ["about"],
        "cropLength": 20
    });

    let expected = json!([
        {
            "id": 1,
            "balance": "$1,706.13",
            "picture": "http://placehold.it/32x32",
            "age": 27,
            "color": "Green",
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "phone": "+1 (995) 479-3174",
            "address": "442 Beverly Road, Ventress, New Mexico, 3361",
            "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
            "registered": "2020-03-18T11:12:21 -01:00",
            "latitude": -24.356932,
            "longitude": 27.184808,
            "tags": [
              "new issue",
              "bug"
            ],
            "isActive": true,
            "_formatted": {
              "id": 1,
              "balance": "$1,706.13",
              "picture": "http://placehold.it/32x32",
              "age": 27,
              "color": "Green",
              "name": "Cherry Orr",
              "gender": "female",
              "email": "cherryorr@chorizon.com",
              "phone": "+1 (995) 479-3174",
              "address": "442 Beverly Road, Ventress, New Mexico, 3361",
              "about": "Exercitation officia",
              "registered": "2020-03-18T11:12:21 -01:00",
              "latitude": -24.356932,
              "longitude": 27.184808,
              "tags": [
                "new issue",
                "bug"
              ],
              "isActive": true
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_attributes_to_retrieve() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["name","age","color","gender"],
    });

    let expected = json!([
      {
          "name": "Cherry Orr",
          "age": 27,
          "color": "Green",
          "gender": "female"
      }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_attributes_to_retrieve_wildcard() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["*"],
    });

    let expected = json!([
        {
            "id": 1,
            "isActive": true,
            "balance": "$1,706.13",
            "picture": "http://placehold.it/32x32",
            "age": 27,
            "color": "Green",
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "phone": "+1 (995) 479-3174",
            "address": "442 Beverly Road, Ventress, New Mexico, 3361",
            "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
            "registered": "2020-03-18T11:12:21 -01:00",
            "latitude": -24.356932,
            "longitude": 27.184808,
            "tags": [
                "new issue",
                "bug"
            ]
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_filter() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "exercitation",
        "limit": 3,
        "filters": "gender='male'"
    });

    let expected = json!([
        {
            "id": 59,
            "balance": "$1,921.58",
            "picture": "http://placehold.it/32x32",
            "age": 31,
            "color": "Green",
            "name": "Harper Carson",
            "gender": "male",
            "email": "harpercarson@chorizon.com",
            "phone": "+1 (912) 430-3243",
            "address": "883 Dennett Place, Knowlton, New Mexico, 9219",
            "about": "Exercitation minim esse proident cillum velit et deserunt incididunt adipisicing minim. Cillum Lorem consectetur laborum id consequat exercitation velit. Magna dolor excepteur sunt deserunt dolor ullamco non sint proident ipsum. Reprehenderit voluptate sit veniam consectetur ea sunt duis labore deserunt ipsum aute. Eiusmod aliqua anim voluptate id duis tempor aliqua commodo sunt. Do officia ea consectetur nostrud eiusmod laborum.\r\n",
            "registered": "2019-12-07T07:33:15 -01:00",
            "latitude": -60.812605,
            "longitude": -27.129016,
            "tags": [
                "bug",
                "new issue"
            ],
            "isActive": true
        },
        {
            "id": 0,
            "balance": "$2,668.55",
            "picture": "http://placehold.it/32x32",
            "age": 36,
            "color": "Green",
            "name": "Lucas Hess",
            "gender": "male",
            "email": "lucashess@chorizon.com",
            "phone": "+1 (998) 478-2597",
            "address": "412 Losee Terrace, Blairstown, Georgia, 2825",
            "about": "Mollit ad in exercitation quis. Anim est ut consequat fugiat duis magna aliquip velit nisi. Commodo eiusmod est consequat proident consectetur aliqua enim fugiat. Aliqua adipisicing laboris elit proident enim veniam laboris mollit. Incididunt fugiat minim ad nostrud deserunt tempor in. Id irure officia labore qui est labore nulla nisi. Magna sit quis tempor esse consectetur amet labore duis aliqua consequat.\r\n",
            "registered": "2016-06-21T09:30:25 -02:00",
            "latitude": -44.174957,
            "longitude": -145.725388,
            "tags": [
                "bug",
                "bug"
            ],
            "isActive": false
        },
        {
            "id": 66,
            "balance": "$1,061.49",
            "picture": "http://placehold.it/32x32",
            "age": 35,
            "color": "brown",
            "name": "Higgins Aguilar",
            "gender": "male",
            "email": "higginsaguilar@chorizon.com",
            "phone": "+1 (911) 540-3791",
            "address": "132 Sackman Street, Layhill, Guam, 8729",
            "about": "Anim ea dolore exercitation minim. Proident cillum non deserunt cupidatat veniam non occaecat aute ullamco irure velit laboris ex aliquip. Voluptate incididunt non ex nulla est ipsum. Amet anim do velit sunt irure sint minim nisi occaecat proident tempor elit exercitation nostrud.\r\n",
            "registered": "2015-04-05T02:10:07 -02:00",
            "latitude": 74.702813,
            "longitude": 151.314972,
            "tags": [
                "bug"
            ],
            "isActive": true
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });

    let expected = json!([
        {
            "id": 0,
            "balance": "$2,668.55",
            "picture": "http://placehold.it/32x32",
            "age": 36,
            "color": "Green",
            "name": "Lucas Hess",
            "gender": "male",
            "email": "lucashess@chorizon.com",
            "phone": "+1 (998) 478-2597",
            "address": "412 Losee Terrace, Blairstown, Georgia, 2825",
            "about": "Mollit ad in exercitation quis. Anim est ut consequat fugiat duis magna aliquip velit nisi. Commodo eiusmod est consequat proident consectetur aliqua enim fugiat. Aliqua adipisicing laboris elit proident enim veniam laboris mollit. Incididunt fugiat minim ad nostrud deserunt tempor in. Id irure officia labore qui est labore nulla nisi. Magna sit quis tempor esse consectetur amet labore duis aliqua consequat.\r\n",
            "registered": "2016-06-21T09:30:25 -02:00",
            "latitude": -44.174957,
            "longitude": -145.725388,
            "tags": [
                "bug",
                "bug"
            ],
            "isActive": false
        }
    ]);

    let query = json!({
        "q": "exercitation",
        "limit": 3,
        "filters": "name='Lucas Hess'"
    });

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });

    let expected = json!([
        {
            "id": 2,
            "balance": "$2,467.47",
            "picture": "http://placehold.it/32x32",
            "age": 34,
            "color": "blue",
            "name": "Patricia Goff",
            "gender": "female",
            "email": "patriciagoff@chorizon.com",
            "phone": "+1 (864) 463-2277",
            "address": "866 Hornell Loop, Cresaptown, Ohio, 1700",
            "about": "Non culpa duis dolore Lorem aliqua. Labore veniam laborum cupidatat nostrud ea exercitation. Esse nostrud sit veniam laborum minim ullamco nulla aliqua est cillum magna. Duis non esse excepteur veniam voluptate sunt cupidatat nostrud consequat sint adipisicing ut excepteur. Incididunt sit aliquip non id magna amet deserunt esse quis dolor.\r\n",
            "registered": "2014-10-28T12:59:30 -01:00",
            "latitude": -64.008555,
            "longitude": 11.867098,
            "tags": [
                "good first issue"
            ],
            "isActive": true
        },
        {
            "id": 75,
            "balance": "$1,913.42",
            "picture": "http://placehold.it/32x32",
            "age": 24,
            "color": "Green",
            "name": "Emma Jacobs",
            "gender": "female",
            "email": "emmajacobs@chorizon.com",
            "phone": "+1 (899) 554-3847",
            "address": "173 Tapscott Street, Esmont, Maine, 7450",
            "about": "Laboris consequat consectetur tempor labore ullamco ullamco voluptate quis quis duis ut ad. In est irure quis amet sunt nulla ad ut sit labore ut eu quis duis. Nostrud cupidatat aliqua sunt occaecat minim id consequat officia deserunt laborum. Ea dolor reprehenderit laborum veniam exercitation est nostrud excepteur laborum minim id qui et.\r\n",
            "registered": "2019-03-29T06:24:13 -01:00",
            "latitude": -35.53722,
            "longitude": 155.703874,
            "tags": [],
            "isActive": false
        }
    ]);
    let query = json!({
        "q": "exercitation",
        "limit": 3,
        "filters": "gender='female' AND (name='Patricia Goff' OR name='Emma Jacobs')"
    });
    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });

    let expected = json!([
        {
            "id": 30,
            "balance": "$2,021.11",
            "picture": "http://placehold.it/32x32",
            "age": 32,
            "color": "blue",
            "name": "Stacy Espinoza",
            "gender": "female",
            "email": "stacyespinoza@chorizon.com",
            "phone": "+1 (999) 487-3253",
            "address": "931 Alabama Avenue, Bangor, Alaska, 8215",
            "about": "Id reprehenderit cupidatat exercitation anim ad nisi irure. Minim est proident mollit laborum. Duis ad duis eiusmod quis.\r\n",
            "registered": "2014-07-16T06:15:53 -02:00",
            "latitude": 41.560197,
            "longitude": 177.697,
            "tags": [
                "new issue",
                "new issue",
                "bug"
            ],
            "isActive": true
        },
        {
            "id": 31,
            "balance": "$3,609.82",
            "picture": "http://placehold.it/32x32",
            "age": 32,
            "color": "blue",
            "name": "Vilma Garza",
            "gender": "female",
            "email": "vilmagarza@chorizon.com",
            "phone": "+1 (944) 585-2021",
            "address": "565 Tech Place, Sedley, Puerto Rico, 858",
            "about": "Excepteur et fugiat mollit incididunt cupidatat. Mollit nisi veniam sint eu exercitation amet labore. Voluptate est magna est amet qui minim excepteur cupidatat dolor quis id excepteur aliqua reprehenderit. Proident nostrud ex veniam officia nisi enim occaecat ex magna officia id consectetur ad eu. In et est reprehenderit cupidatat ad minim veniam proident nulla elit nisi veniam proident ex. Eu in irure sit veniam amet incididunt fugiat proident quis ullamco laboris.\r\n",
            "registered": "2017-06-30T07:43:52 -02:00",
            "latitude": -12.574889,
            "longitude": -54.771186,
            "tags": [
                "new issue",
                "wontfix",
                "wontfix"
            ],
            "isActive": false
        },
        {
            "id": 2,
            "balance": "$2,467.47",
            "picture": "http://placehold.it/32x32",
            "age": 34,
            "color": "blue",
            "name": "Patricia Goff",
            "gender": "female",
            "email": "patriciagoff@chorizon.com",
            "phone": "+1 (864) 463-2277",
            "address": "866 Hornell Loop, Cresaptown, Ohio, 1700",
            "about": "Non culpa duis dolore Lorem aliqua. Labore veniam laborum cupidatat nostrud ea exercitation. Esse nostrud sit veniam laborum minim ullamco nulla aliqua est cillum magna. Duis non esse excepteur veniam voluptate sunt cupidatat nostrud consequat sint adipisicing ut excepteur. Incididunt sit aliquip non id magna amet deserunt esse quis dolor.\r\n",
            "registered": "2014-10-28T12:59:30 -01:00",
            "latitude": -64.008555,
            "longitude": 11.867098,
            "tags": [
                "good first issue"
            ],
            "isActive": true
        }
    ]);
    let query = json!({
        "q": "exerciatation",
        "limit": 3,
        "filters": "gender='female' AND (name='Patricia Goff' OR age > 30)"
    });
    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });

    let expected = json!([
        {
            "id": 59,
            "balance": "$1,921.58",
            "picture": "http://placehold.it/32x32",
            "age": 31,
            "color": "Green",
            "name": "Harper Carson",
            "gender": "male",
            "email": "harpercarson@chorizon.com",
            "phone": "+1 (912) 430-3243",
            "address": "883 Dennett Place, Knowlton, New Mexico, 9219",
            "about": "Exercitation minim esse proident cillum velit et deserunt incididunt adipisicing minim. Cillum Lorem consectetur laborum id consequat exercitation velit. Magna dolor excepteur sunt deserunt dolor ullamco non sint proident ipsum. Reprehenderit voluptate sit veniam consectetur ea sunt duis labore deserunt ipsum aute. Eiusmod aliqua anim voluptate id duis tempor aliqua commodo sunt. Do officia ea consectetur nostrud eiusmod laborum.\r\n",
            "registered": "2019-12-07T07:33:15 -01:00",
            "latitude": -60.812605,
            "longitude": -27.129016,
            "tags": [
                "bug",
                "new issue"
            ],
            "isActive": true
        },
        {
            "id": 0,
            "balance": "$2,668.55",
            "picture": "http://placehold.it/32x32",
            "age": 36,
            "color": "Green",
            "name": "Lucas Hess",
            "gender": "male",
            "email": "lucashess@chorizon.com",
            "phone": "+1 (998) 478-2597",
            "address": "412 Losee Terrace, Blairstown, Georgia, 2825",
            "about": "Mollit ad in exercitation quis. Anim est ut consequat fugiat duis magna aliquip velit nisi. Commodo eiusmod est consequat proident consectetur aliqua enim fugiat. Aliqua adipisicing laboris elit proident enim veniam laboris mollit. Incididunt fugiat minim ad nostrud deserunt tempor in. Id irure officia labore qui est labore nulla nisi. Magna sit quis tempor esse consectetur amet labore duis aliqua consequat.\r\n",
            "registered": "2016-06-21T09:30:25 -02:00",
            "latitude": -44.174957,
            "longitude": -145.725388,
            "tags": [
                "bug",
                "bug"
            ],
            "isActive": false
        },
        {
            "id": 66,
            "balance": "$1,061.49",
            "picture": "http://placehold.it/32x32",
            "age": 35,
            "color": "brown",
            "name": "Higgins Aguilar",
            "gender": "male",
            "email": "higginsaguilar@chorizon.com",
            "phone": "+1 (911) 540-3791",
            "address": "132 Sackman Street, Layhill, Guam, 8729",
            "about": "Anim ea dolore exercitation minim. Proident cillum non deserunt cupidatat veniam non occaecat aute ullamco irure velit laboris ex aliquip. Voluptate incididunt non ex nulla est ipsum. Amet anim do velit sunt irure sint minim nisi occaecat proident tempor elit exercitation nostrud.\r\n",
            "registered": "2015-04-05T02:10:07 -02:00",
            "latitude": 74.702813,
            "longitude": 151.314972,
            "tags": [
                "bug"
            ],
            "isActive": true
        }
    ]);
    let query = json!({
        "q": "exerciatation",
        "limit": 3,
        "filters": "NOT gender = 'female' AND age > 30"
    });

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });

    let expected = json!([
        {
            "id": 11,
            "balance": "$1,351.43",
            "picture": "http://placehold.it/32x32",
            "age": 28,
            "color": "Green",
            "name": "Evans Wagner",
            "gender": "male",
            "email": "evanswagner@chorizon.com",
            "phone": "+1 (889) 496-2332",
            "address": "118 Monaco Place, Lutsen, Delaware, 6209",
            "about": "Sunt consectetur enim ipsum consectetur occaecat reprehenderit nulla pariatur. Cupidatat do exercitation tempor voluptate duis nostrud dolor consectetur. Excepteur aliquip Lorem voluptate cillum est. Nisi velit nulla nostrud ea id officia laboris et.\r\n",
            "registered": "2016-10-27T01:26:31 -02:00",
            "latitude": -77.673222,
            "longitude": -142.657214,
            "tags": [
                "good first issue",
                "good first issue"
            ],
            "isActive": true
        }
    ]);
    let query = json!({
        "q": "exerciatation",
        "filters": "NOT gender = 'female' AND name='Evans Wagner'"
    });

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_attributes_to_highlight_and_matches() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToHighlight": ["name","email"],
        "matches": true,
    });

    let expected = json!([
        {
            "id": 1,
            "balance": "$1,706.13",
            "picture": "http://placehold.it/32x32",
            "age": 27,
            "color": "Green",
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "phone": "+1 (995) 479-3174",
            "address": "442 Beverly Road, Ventress, New Mexico, 3361",
            "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
            "registered": "2020-03-18T11:12:21 -01:00",
            "latitude": -24.356932,
            "longitude": 27.184808,
            "tags": [
                "new issue",
                "bug"
            ],
            "isActive": true,
            "_formatted": {
                "id": 1,
                "balance": "$1,706.13",
                "picture": "http://placehold.it/32x32",
                "age": 27,
                "color": "Green",
                "name": "<em>Cherry</em> Orr",
                "gender": "female",
                "email": "<em>cherry</em>orr@chorizon.com",
                "phone": "+1 (995) 479-3174",
                "address": "442 Beverly Road, Ventress, New Mexico, 3361",
                "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
                "registered": "2020-03-18T11:12:21 -01:00",
                "latitude": -24.356932,
                "longitude": 27.184808,
                "tags": [
                    "new issue",
                    "bug"
                ],
                "isActive": true
            },
            "_matchesInfo": {
                "email": [
                    {
                        "start": 0,
                        "length": 6
                    }
                ],
                "name": [
                    {
                        "start": 0,
                        "length": 6
                    }
                ]
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_attributes_to_highlight_and_matches_and_crop() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "exerciatation",
        "limit": 1,
        "attributesToCrop": ["about"],
        "cropLength": 20,
        "attributesToHighlight": ["about"],
        "matches": true,
    });

    let expected = json!([
        {
            "id": 1,
            "balance": "$1,706.13",
            "picture": "http://placehold.it/32x32",
            "age": 27,
            "color": "Green",
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "phone": "+1 (995) 479-3174",
            "address": "442 Beverly Road, Ventress, New Mexico, 3361",
            "about": "Exercitation officia mollit proident nostrud ea. Pariatur voluptate labore nostrud magna duis non elit et incididunt Lorem velit duis amet commodo. Irure in velit laboris pariatur. Do tempor ex deserunt duis minim amet.\r\n",
            "registered": "2020-03-18T11:12:21 -01:00",
            "latitude": -24.356932,
            "longitude": 27.184808,
            "tags": [
                "new issue",
                "bug"
            ],
            "isActive": true,
            "_formatted": {
                "id": 1,
                "balance": "$1,706.13",
                "picture": "http://placehold.it/32x32",
                "age": 27,
                "color": "Green",
                "name": "Cherry Orr",
                "gender": "female",
                "email": "cherryorr@chorizon.com",
                "phone": "+1 (995) 479-3174",
                "address": "442 Beverly Road, Ventress, New Mexico, 3361",
                "about": "<em>Exercitation</em> officia",
                "registered": "2020-03-18T11:12:21 -01:00",
                "latitude": -24.356932,
                "longitude": 27.184808,
                "tags": [
                    "new issue",
                    "bug"
                ],
                "isActive": true
            },
            "_matchesInfo": {
                "about": [
                    {
                        "start": 0,
                        "length": 12
                    }
                ]
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["name","age","gender","email"],
        "attributesToHighlight": ["name"],
    });

    let expected = json!([
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "_formatted": {
                "name": "<em>Cherry</em> Orr"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes_2() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "exercitation",
        "limit": 1,
        "attributesToRetrieve": ["name","age","gender"],
        "attributesToCrop": ["about"],
        "cropLength": 20,
    });

    let expected = json!([
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "_formatted": {
                "about": "Exercitation officia"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes_3() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "exercitation",
        "limit": 1,
        "attributesToRetrieve": ["name","age","gender"],
        "attributesToCrop": ["about:20"],
    });

    let expected = json!( [
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "_formatted": {
                "about": "Exercitation officia"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes_4() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["name","age","email","gender"],
        "attributesToCrop": ["name:0","email:6"],
    });

    let expected = json!([
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "_formatted": {
                "name": "Cherry",
                "email": "cherryorr"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes_5() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["name","age","email","gender"],
        "attributesToCrop": ["*","email:6"],
    });

    let expected = json!([
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "_formatted": {
                "name": "Cherry Orr",
                "email": "cherryorr",
                "age": 27,
                "gender": "female"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes_6() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["name","age","email","gender"],
        "attributesToCrop": ["*","email:10"],
        "attributesToHighlight": ["name"],
    });

    let expected = json!([
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "_formatted": {
                "age": 27,
                "name": "<em>Cherry</em> Orr",
                "gender": "female",
                "email": "cherryorr@"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes_7() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["name","age","gender","email"],
        "attributesToCrop": ["*","email:6"],
        "attributesToHighlight": ["*"],
    });

    let expected = json!([
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "_formatted": {
                "age": 27,
                "name": "<em>Cherry</em> Orr",
                "gender": "female",
                "email": "<em>cherry</em>orr"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn search_with_differents_attributes_8() {
    let mut server = common::Server::test_server().await;

    let query = json!({
        "q": "cherry",
        "limit": 1,
        "attributesToRetrieve": ["name","age","email","gender","address"],
        "attributesToCrop": ["*","email:6"],
        "attributesToHighlight": ["*","address"],
    });

    let expected = json!([
        {
            "age": 27,
            "name": "Cherry Orr",
            "gender": "female",
            "email": "cherryorr@chorizon.com",
            "address": "442 Beverly Road, Ventress, New Mexico, 3361",
            "_formatted": {
                "age": 27,
                "name": "<em>Cherry</em> Orr",
                "gender": "female",
                "email": "<em>cherry</em>orr",
                "address": "442 Beverly Road, Ventress, New Mexico, 3361"
            }
        }
    ]);

    test_post_get_search!(server, query, |response, _status_code| {
        assert_json_eq!(expected.clone(), response["hits"].clone(), ordered: false);
    });
}

#[actix_rt::test]
async fn test_faceted_search_valid() {
    // set facetting attributes before adding documents
    let mut server = common::Server::with_uid("test");
    server.create_index(json!({ "uid": "test" })).await;

    let body = json!({
        "attributesForFaceting": ["color"]
    });
    server.update_all_settings(body).await;

    let dataset = include_bytes!("assets/test_set.json");
    let body: Value = serde_json::from_slice(dataset).unwrap();
    server.add_or_update_multiple_documents(body).await;

    // simple tests on attributes with string value

    let query = json!({
        "q": "a",
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
        "q": "a",
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
        "q": "a",
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
        "q": "a",
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
        "q": "a",
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
        "q": "a",
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
        "q": "a",
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
async fn test_faceted_search_invalid() {
    let mut server = common::Server::test_server().await;

    //no faceted attributes set
    let query = json!({
        "q": "a",
        "facetFilters": ["color:blue"]
    });

    test_post_get_search!(server, query, |response, status_code| {

        assert_eq!(status_code, 400);
        assert_eq!(response["errorCode"], "invalid_facet");
    });

    let body = json!({
        "attributesForFaceting": ["color", "tags"]
    });
    server.update_all_settings(body).await;
    // empty arrays are error
    // []
    let query = json!({
        "q": "a",
        "facetFilters": []
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 400);
        assert_eq!(response["errorCode"], "invalid_facet");
    });
    // [[]]
    let query = json!({
        "q": "a",
        "facetFilters": [[]]
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 400);
        assert_eq!(response["errorCode"], "invalid_facet");
    });

    // ["color:green", []]
    let query = json!({
        "q": "a",
        "facetFilters": ["color:green", []]
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 400);
        assert_eq!(response["errorCode"], "invalid_facet");
    });

    // too much depth
    // [[[]]]
    let query = json!({
        "q": "a",
        "facetFilters": [[[]]]
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 400);
        assert_eq!(response["errorCode"], "invalid_facet");
    });

    // [["color:green", ["color:blue"]]]
    let query = json!({
        "q": "a",
        "facetFilters": [["color:green", ["color:blue"]]]
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 400);
        assert_eq!(response["errorCode"], "invalid_facet");
    });

    // "color:green"
    let query = json!({
        "q": "a",
        "facetFilters": "color:green"
    });

    test_post_get_search!(server, query, |response, status_code| {
        assert_eq!(status_code, 400);
        assert_eq!(response["errorCode"], "invalid_facet");
    });
}

#[actix_rt::test]
async fn test_facet_count() {
    let mut server = common::Server::test_server().await;

    // test without facet distribution
    let query = json!({
        "q": "a",
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert!(response.get("exhaustiveFacetsCount").is_none());
        assert!(response.get("facetsDistribution").is_none());
    });

    // test no facets set, search on color
    let query = json!({
        "q": "a",
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
        // assert that case is preserved
        assert!(response["facetsDistribution"]
            .as_object()
            .unwrap()["color"]
            .as_object()
            .unwrap()
            .get("Green")
            .is_some());
    });
    // searching on color and tags
    let query = json!({
        "q": "a",
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
        "q": "a",
        "facetsDistribution": ["*"]
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert_eq!(response.get("facetsDistribution").unwrap().as_object().unwrap().values().count(), 2);
    });
    // wildcard with other attributes:
    let query = json!({
        "q": "a",
        "facetsDistribution": ["color", "*"]
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert_eq!(response.get("facetsDistribution").unwrap().as_object().unwrap().values().count(), 2);
    });

    // empty facet list
    let query = json!({
        "q": "a",
        "facetsDistribution": []
    });
    test_post_get_search!(server, query, |response, _status_code|{
        assert_eq!(response.get("facetsDistribution").unwrap().as_object().unwrap().values().count(), 0);
    });

    // attr not set as facet passed:
    let query = json!({
        "q": "a",
        "facetsDistribution": ["gender"]
    });
    test_post_get_search!(server, query, |_response, status_code|{
        assert_eq!(status_code, 400);
    });

}

#[actix_rt::test]
#[should_panic]
async fn test_bad_facet_distribution() {
    let mut server = common::Server::test_server().await;
    // string instead of array:
    let query = json!({
        "q": "a",
        "facetsDistribution": "color"
    });
    test_post_get_search!(server, query, |_response, _status_code| {});

    // invalid value in array:
    let query = json!({
        "q": "a",
        "facetsDistribution": ["color", true]
    });
    test_post_get_search!(server, query, |_response, _status_code| {});
}

#[actix_rt::test]
async fn highlight_cropped_text() {
    let mut server = common::Server::with_uid("test");

    let body = json!({
        "uid": "test",
        "primaryKey": "id",
    });
    server.create_index(body).await;

    let doc = json!([
        {
            "id": 1,
            "body": r##"well, it may not work like that, try the following: 
1. insert your trip
2. google your `searchQuery`
3. find a solution 
> say hello"##
        }
    ]);
    server.add_or_replace_multiple_documents(doc).await;

    // tests from #680
    //let query = "q=insert&attributesToHighlight=*&attributesToCrop=body&cropLength=30";
    let query = json!({
        "q": "insert",
        "attributesToHighlight": ["*"],
        "attributesToCrop": ["body"],
        "cropLength": 30,
    });
    let expected_response = "that, try the following: \n1. <em>insert</em> your trip\n2. google your";
    test_post_get_search!(server, query, |response, _status_code|{
        assert_eq!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .get(0)
            .unwrap()
            .as_object()
            .unwrap()
            .get("_formatted")
            .unwrap()
            .as_object()
            .unwrap()
            .get("body")
            .unwrap()
            , &Value::String(expected_response.to_owned()));
    });

    //let query = "q=insert&attributesToHighlight=*&attributesToCrop=body&cropLength=80";
    let query = json!({
        "q": "insert",
        "attributesToHighlight": ["*"],
        "attributesToCrop": ["body"],
        "cropLength": 80,
    });
    let expected_response = "well, it may not work like that, try the following: \n1. <em>insert</em> your trip\n2. google your `searchQuery`\n3. find a solution \n> say hello";
    test_post_get_search!(server, query, |response, _status_code| {
        assert_eq!(response
            .get("hits")
            .unwrap()
            .as_array()
            .unwrap()
            .get(0)
            .unwrap()
            .as_object()
            .unwrap()
            .get("_formatted")
            .unwrap()
            .as_object()
            .unwrap()
            .get("body")
            .unwrap()
            , &Value::String(expected_response.to_owned()));
    });
}

#[actix_rt::test]
async fn well_formated_error_with_bad_request_params() {
    let mut server = common::Server::with_uid("test");
    let query = "foo=bar";
    let (response, _status_code) = server.search_get(query).await;
    assert!(response.get("message").is_some());
    assert!(response.get("errorCode").is_some());
    assert!(response.get("errorType").is_some());
    assert!(response.get("errorLink").is_some());
}


#[actix_rt::test]
async fn update_documents_with_facet_distribution() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "uid": "test",
        "primaryKey": "id",
    });

    server.create_index(body).await;
    let settings = json!({
        "attributesForFaceting": ["genre"],
        "displayedAttributes": ["genre"],
        "searchableAttributes": ["genre"]
    });
    server.update_all_settings(settings).await;
    let update1 = json!([
        {
            "id": "1",
            "type": "album",
            "title": "Nevermind",
            "genre": ["grunge", "alternative"]
        },
        {
            "id": "2",
            "type": "album",
            "title": "Mellon Collie and the Infinite Sadness",
            "genre": ["alternative", "rock"]
        },
        {
            "id": "3",
            "type": "album",
            "title": "The Queen Is Dead",
            "genre": ["indie", "rock"]
        }
    ]);
    server.add_or_update_multiple_documents(update1).await;
    let search = json!({
        "q": "album",
        "facetsDistribution": ["genre"]
    });
    let (response1, _) = server.search_post(search.clone()).await;
    let expected_facet_distribution = json!({
        "genre": {
            "grunge": 1,
            "alternative": 2,
            "rock": 2,
            "indie": 1
        }
    });
    assert_json_eq!(expected_facet_distribution.clone(), response1["facetsDistribution"].clone());

    let update2 = json!([
        {
            "id": "3",
            "title": "The Queen Is Very Dead"
        }
    ]);
    server.add_or_update_multiple_documents(update2).await;
    let (response2, _) = server.search_post(search).await;
    assert_json_eq!(expected_facet_distribution, response2["facetsDistribution"].clone());
}
