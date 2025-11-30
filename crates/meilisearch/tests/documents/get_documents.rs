use actix_web::http::header::ACCEPT_ENCODING;
use actix_web::test;
use meili_snap::*;
use urlencoding::encode as urlencode;

use crate::common::encoder::Encoder;
use crate::common::{
    shared_does_not_exists_index, shared_empty_index, shared_index_with_geo_documents,
    shared_index_with_test_set, GetAllDocumentsOptions, Server, Value,
};
use crate::json;

// TODO: partial test since we are testing error, amd error is not yet fully implemented in
// transplant
#[actix_rt::test]
async fn get_unexisting_index_single_document() {
    let (_response, code) = shared_does_not_exists_index().await.get_document(1, None).await;
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn error_get_unexisting_document() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.get_document(1, None).await;

    let expected_response = json!({
        "message": "Document `1` not found.",
        "code": "document_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#document_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn get_document() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();
    let documents = json!([
        {
            "id": 0,
            "nested": { "content": "foobar" },
        }
    ]);
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_document(0, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        response,
        json!({
            "id": 0,
            "nested": { "content": "foobar" },
        })
    );

    let (response, code) = index.get_document(0, Some(json!({ "fields": ["id"] }))).await;
    assert_eq!(code, 200);
    assert_eq!(
        response,
        json!({
            "id": 0,
        })
    );

    let (response, code) =
        index.get_document(0, Some(json!({ "fields": ["nested.content"] }))).await;
    assert_eq!(code, 200);
    assert_eq!(
        response,
        json!({
            "nested": { "content": "foobar" },
        })
    );
}

#[actix_rt::test]
async fn get_document_sorted() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.load_test_set(server).await;

    let (task, _status_code) =
        index.update_settings_sortable_attributes(json!(["age", "email", "gender", "name"])).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, _code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["id", "age", "email"]),
            sort: Some(vec!["age:asc", "email:desc"]),
            ..Default::default()
        })
        .await;
    let results = response["results"].as_array().unwrap();
    snapshot!(json_string!(results), @r#"
    [
      {
        "id": 5,
        "age": 20,
        "email": "warrenwatson@chorizon.com"
      },
      {
        "id": 6,
        "age": 20,
        "email": "sheliaberry@chorizon.com"
      },
      {
        "id": 57,
        "age": 20,
        "email": "kaitlinconner@chorizon.com"
      },
      {
        "id": 45,
        "age": 20,
        "email": "irenebennett@chorizon.com"
      },
      {
        "id": 40,
        "age": 21,
        "email": "staffordemerson@chorizon.com"
      },
      {
        "id": 41,
        "age": 21,
        "email": "salinasgamble@chorizon.com"
      },
      {
        "id": 63,
        "age": 21,
        "email": "knowleshebert@chorizon.com"
      },
      {
        "id": 50,
        "age": 21,
        "email": "guerramcintyre@chorizon.com"
      },
      {
        "id": 44,
        "age": 22,
        "email": "jonispears@chorizon.com"
      },
      {
        "id": 56,
        "age": 23,
        "email": "tuckerbarry@chorizon.com"
      },
      {
        "id": 51,
        "age": 23,
        "email": "keycervantes@chorizon.com"
      },
      {
        "id": 60,
        "age": 23,
        "email": "jodyherrera@chorizon.com"
      },
      {
        "id": 70,
        "age": 23,
        "email": "glassperkins@chorizon.com"
      },
      {
        "id": 75,
        "age": 24,
        "email": "emmajacobs@chorizon.com"
      },
      {
        "id": 68,
        "age": 24,
        "email": "angelinadyer@chorizon.com"
      },
      {
        "id": 17,
        "age": 25,
        "email": "ortegabrennan@chorizon.com"
      },
      {
        "id": 76,
        "age": 25,
        "email": "claricegardner@chorizon.com"
      },
      {
        "id": 43,
        "age": 25,
        "email": "arnoldbender@chorizon.com"
      },
      {
        "id": 12,
        "age": 25,
        "email": "aidakirby@chorizon.com"
      },
      {
        "id": 9,
        "age": 26,
        "email": "kellimendez@chorizon.com"
      }
    ]
    "#);

    let (response, _code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["id", "gender", "name"]),
            sort: Some(vec!["gender:asc", "name:asc"]),
            ..Default::default()
        })
        .await;
    let results = response["results"].as_array().unwrap();
    snapshot!(json_string!(results), @r#"
    [
      {
        "id": 3,
        "name": "Adeline Flynn",
        "gender": "female"
      },
      {
        "id": 12,
        "name": "Aida Kirby",
        "gender": "female"
      },
      {
        "id": 68,
        "name": "Angelina Dyer",
        "gender": "female"
      },
      {
        "id": 15,
        "name": "Aurelia Contreras",
        "gender": "female"
      },
      {
        "id": 36,
        "name": "Barbra Valenzuela",
        "gender": "female"
      },
      {
        "id": 23,
        "name": "Blanca Mcclain",
        "gender": "female"
      },
      {
        "id": 53,
        "name": "Caitlin Burnett",
        "gender": "female"
      },
      {
        "id": 71,
        "name": "Candace Sawyer",
        "gender": "female"
      },
      {
        "id": 65,
        "name": "Carole Rowland",
        "gender": "female"
      },
      {
        "id": 33,
        "name": "Cecilia Greer",
        "gender": "female"
      },
      {
        "id": 1,
        "name": "Cherry Orr",
        "gender": "female"
      },
      {
        "id": 38,
        "name": "Christina Short",
        "gender": "female"
      },
      {
        "id": 7,
        "name": "Chrystal Boyd",
        "gender": "female"
      },
      {
        "id": 76,
        "name": "Clarice Gardner",
        "gender": "female"
      },
      {
        "id": 73,
        "name": "Eleanor Shepherd",
        "gender": "female"
      },
      {
        "id": 75,
        "name": "Emma Jacobs",
        "gender": "female"
      },
      {
        "id": 16,
        "name": "Estella Bass",
        "gender": "female"
      },
      {
        "id": 62,
        "name": "Estelle Ramirez",
        "gender": "female"
      },
      {
        "id": 20,
        "name": "Florence Long",
        "gender": "female"
      },
      {
        "id": 42,
        "name": "Graciela Russell",
        "gender": "female"
      }
    ]
    "#);
}

#[actix_rt::test]
async fn get_document_geosorted() {
    let index = shared_index_with_geo_documents().await;

    let (response, _code) = index
        .get_all_documents(GetAllDocumentsOptions {
            sort: Some(vec!["_geoPoint(45.4777599, 9.1967508):asc"]),
            ..Default::default()
        })
        .await;
    let results = response["results"].as_array().unwrap();
    snapshot!(json_string!(results), @r#"
    [
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
        "id": 3,
        "name": "Crêpe Truck",
        "address": "2 Billig Avenue, Rouenville",
        "type": "French",
        "rating": 10
      }
    ]
    "#);
}

#[actix_rt::test]
async fn get_document_sort_the_unsortable() {
    let index = shared_index_with_test_set().await;

    let (response, _code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["id", "name"]),
            sort: Some(vec!["name:asc"]),
            ..Default::default()
        })
        .await;

    snapshot!(json_string!(response), @r#"
    {
      "message": "Attribute `name` is not sortable. This index does not have configured sortable attributes.",
      "code": "invalid_document_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_sort"
    }
    "#);
}

#[actix_rt::test]
async fn error_get_unexisting_index_all_documents() {
    let index = shared_does_not_exists_index().await;
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;

    let expected_response = json!({
        "message": "Index `DOES_NOT_EXISTS` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn get_no_document() {
    let index = shared_empty_index().await;

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert!(response["results"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn get_all_documents_no_options() {
    let index = shared_index_with_test_set().await;

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    assert_eq!(results.len(), 20);
    let first = json!({
        "id":0,
        "isActive":false,
        "balance":"$2,668.55",
        "picture":"http://placehold.it/32x32",
        "age":36,
        "color":"Green",
        "name":"Lucas Hess",
        "gender":"male",
        "email":"lucashess@chorizon.com",
        "phone":"+1 (998) 478-2597",
        "address":"412 Losee Terrace, Blairstown, Georgia, 2825",
        "about":"Mollit ad in exercitation quis. Anim est ut consequat fugiat duis magna aliquip velit nisi. Commodo eiusmod est consequat proident consectetur aliqua enim fugiat. Aliqua adipisicing laboris elit proident enim veniam laboris mollit. Incididunt fugiat minim ad nostrud deserunt tempor in. Id irure officia labore qui est labore nulla nisi. Magna sit quis tempor esse consectetur amet labore duis aliqua consequat.\r\n",
        "registered":"2016-06-21T09:30:25 -02:00",
        "latitude":-44.174957,
        "longitude":-145.725388,
        "tags":["bug"
            ,"bug"]});
    assert_eq!(first, results[0]);
}

#[actix_rt::test]
async fn get_all_documents_no_options_with_response_compression() {
    let index = shared_index_with_test_set().await;

    let app = Server::new_shared().init_web_app().await;
    let req = test::TestRequest::get()
        .uri(&format!("/indexes/{}/documents?", urlencode(&index.uid)))
        .insert_header((ACCEPT_ENCODING, "gzip"))
        .to_request();

    let res = test::call_service(&app, req).await;

    assert_eq!(res.status(), 200);

    let bytes = test::read_body(res).await;
    let decoded = Encoder::Gzip.decode(bytes);
    let parsed_response =
        serde_json::from_slice::<Value>(decoded.into().as_ref()).expect("Expecting valid json");

    let arr = parsed_response["results"].as_array().unwrap();
    assert_eq!(arr.len(), 20);
}

#[actix_rt::test]
async fn test_get_all_documents_limit() {
    let index = shared_index_with_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions { limit: Some(5), ..Default::default() })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 5);
    assert_eq!(response["results"][0]["id"], json!(0));
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["limit"], json!(5));
    assert_eq!(response["total"], json!(77));
}

#[actix_rt::test]
async fn test_get_all_documents_offset() {
    let index = shared_index_with_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions { offset: Some(5), ..Default::default() })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    assert_eq!(response["results"][0]["id"], json!(5));
    assert_eq!(response["offset"], json!(5));
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["total"], json!(77));
}

#[actix_rt::test]
async fn test_get_all_documents_attributes_to_retrieve() {
    let index = shared_index_with_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["name"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    for results in response["results"].as_array().unwrap() {
        assert_eq!(results.as_object().unwrap().keys().count(), 1);
        assert!(results["name"] != json!(null));
    }
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["total"], json!(77));

    let (response, code) = index.get_all_documents_raw("?fields=").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    for results in response["results"].as_array().unwrap() {
        assert_eq!(results.as_object().unwrap().keys().count(), 0);
    }
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["total"], json!(77));

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["wrong"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    for results in response["results"].as_array().unwrap() {
        assert_eq!(results.as_object().unwrap().keys().count(), 0);
    }
    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["total"], json!(77));

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["name", "tags"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    for results in response["results"].as_array().unwrap() {
        assert_eq!(results.as_object().unwrap().keys().count(), 2);
        assert!(results["name"] != json!(null));
        assert!(results["tags"] != json!(null));
    }

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions { fields: Some(vec!["*"]), ..Default::default() })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    for results in response["results"].as_array().unwrap() {
        assert_eq!(results.as_object().unwrap().keys().count(), 16);
    }

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["*", "wrong"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    for results in response["results"].as_array().unwrap() {
        assert_eq!(results.as_object().unwrap().keys().count(), 16);
    }
}

#[actix_rt::test]
async fn get_document_s_nested_attributes_to_retrieve() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        {
            "id": 0,
            "content.truc": "foobar",
        },
        {
            "id": 1,
            "content": {
                "truc": "foobar",
                "machin": "bidule",
            },
        },
    ]);
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.get_document(0, Some(json!({ "fields": ["content"] }))).await;
    assert_eq!(code, 200);
    assert_eq!(response, json!({}));
    let (response, code) = index.get_document(1, Some(json!({ "fields": ["content"] }))).await;
    assert_eq!(code, 200);
    assert_eq!(
        response,
        json!({
            "content": {
                "truc": "foobar",
                "machin": "bidule",
            },
        })
    );

    let (response, code) = index.get_document(0, Some(json!({ "fields": ["content.truc"] }))).await;
    assert_eq!(code, 200);
    assert_eq!(
        response,
        json!({
            "content.truc": "foobar",
        })
    );
    let (response, code) = index.get_document(1, Some(json!({ "fields": ["content.truc"] }))).await;
    assert_eq!(code, 200);
    assert_eq!(
        response,
        json!({
            "content": {
                "truc": "foobar",
            },
        })
    );
}

#[actix_rt::test]
async fn get_documents_displayed_attributes_is_ignored() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.load_test_set(server).await;
    index.update_settings(json!({"displayedAttributes": ["gender"]})).await;

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 20);
    assert_eq!(response["results"][0].as_object().unwrap().keys().count(), 16);
    assert!(response["results"][0]["gender"] != json!(null));

    assert_eq!(response["offset"], json!(0));
    assert_eq!(response["limit"], json!(20));
    assert_eq!(response["total"], json!(77));

    let (response, code) = index.get_document(0, None).await;
    assert_eq!(code, 200);
    assert_eq!(response.as_object().unwrap().keys().count(), 16);
    assert!(response.as_object().unwrap().get("gender").is_some());
}

#[actix_rt::test]
async fn get_document_by_filter() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.update_settings_filterable_attributes(json!(["color"])).await;
    let (task, _code) = index
        .add_documents(
            json!([
                { "id": 0, "color": "red" },
                { "id": 1, "color": "blue" },
                { "id": 2, "color": "blue" },
                { "id": 3 },
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.fetch_documents(json!({})).await;
    let (response2, code2) = index.get_all_documents_raw("").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 0,
          "color": "red"
        },
        {
          "id": 1,
          "color": "blue"
        },
        {
          "id": 2,
          "color": "blue"
        },
        {
          "id": 3
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) = index.fetch_documents(json!({ "filter": "color = blue" })).await;
    let (response2, code2) = index.get_all_documents_raw("?filter=color=blue").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 1,
          "color": "blue"
        },
        {
          "id": 2,
          "color": "blue"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) =
        index.fetch_documents(json!({ "offset": 1, "limit": 1, "filter": "color != blue" })).await;
    let (response2, code2) =
        index.get_all_documents_raw("?filter=color!=blue&offset=1&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 3
        }
      ],
      "offset": 1,
      "limit": 1,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) = index
        .fetch_documents(json!({ "limit": 1, "filter": "color != blue", "fields": ["color"] }))
        .await;
    let (response2, code2) =
        index.get_all_documents_raw("?limit=1&filter=color!=blue&fields=color").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "color": "red"
        }
      ],
      "offset": 0,
      "limit": 1,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    // Now testing more complex filter that the get route can't represent

    let (response, code) =
        index.fetch_documents(json!({ "filter": [["color = blue", "color = red"]] })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 0,
          "color": "red"
        },
        {
          "id": 1,
          "color": "blue"
        },
        {
          "id": 2,
          "color": "blue"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);

    let (response, code) =
        index.fetch_documents(json!({ "filter": [["color != blue"], "color EXISTS"] })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 0,
          "color": "red"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
}

#[actix_rt::test]
async fn get_document_by_ids() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index
        .add_documents(
            json!([
                { "id": 0, "color": "red" },
                { "id": 1, "color": "blue" },
                { "id": 2, "color": "blue" },
                { "id": 3 },
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .fetch_documents(json!({
          "ids": ["0", 1, 2, 3]
        }))
        .await;
    let (response2, code2) = index.get_all_documents_raw("?ids=0,1,2,3").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 0,
          "color": "red"
        },
        {
          "id": 1,
          "color": "blue"
        },
        {
          "id": 2,
          "color": "blue"
        },
        {
          "id": 3
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) = index.fetch_documents(json!({ "ids": [2, "1"] })).await;
    let (response2, code2) = index.get_all_documents_raw("?ids=2,1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 1,
          "color": "blue"
        },
        {
          "id": 2,
          "color": "blue"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) =
        index.fetch_documents(json!({ "offset": 1, "limit": 1, "ids": ["0", 0, 3] })).await;
    let (response2, code2) = index.get_all_documents_raw("?ids=3,0&offset=1&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 3
        }
      ],
      "offset": 1,
      "limit": 1,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) =
        index.fetch_documents(json!({ "limit": 1, "ids": [0, 3], "fields": ["color"] })).await;
    let (response2, code2) = index.get_all_documents_raw("?limit=1&ids=0,3&fields=color").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "color": "red"
        }
      ],
      "offset": 0,
      "limit": 1,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    // Now testing more complex requests that the get route can't represent

    let (response, code) = index.fetch_documents(json!({ "ids": [] })).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [],
      "offset": 0,
      "limit": 20,
      "total": 0
    }
    "###);
}

#[actix_rt::test]
async fn get_document_invalid_ids() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index
        .add_documents(
            json!([
                { "id": 0, "color": "red" },
                { "id": 1, "color": "blue" },
                { "id": 2, "color": "blue" },
                { "id": 3 },
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.fetch_documents(json!({"ids": ["0", "illegal/docid"] })).await;
    let (response2, code2) = index.get_all_documents_raw("?ids=0,illegal/docid").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "message": "In `.ids[1]`: Document identifier `\"illegal/docid\"` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), and can not be more than 511 bytes.",
      "code": "invalid_document_ids",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_ids"
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);
}

#[actix_rt::test]
async fn get_document_not_found_ids() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index
        .add_documents(
            json!([
                { "id": 0, "color": "red" },
                { "id": 1, "color": "blue" },
                { "id": 2, "color": "blue" },
                { "id": 3 },
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.fetch_documents(json!({"ids": ["0", 3, 42] })).await;
    let (response2, code2) = index.get_all_documents_raw("?ids=0,3,42").await;
    // the document with id 42 is not in the results since it doesn't exist
    // however, no error is raised
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 0,
          "color": "red"
        },
        {
          "id": 3
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);
}

#[actix_rt::test]
async fn get_document_by_ids_and_filter() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.update_settings_filterable_attributes(json!(["color"])).await;
    let (task, _code) = index
        .add_documents(
            json!([
                { "id": 0, "color": "red" },
                { "id": 1, "color": "blue" },
                { "id": 2, "color": "blue" },
                { "id": 3 },
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.fetch_documents(json!({"ids": [2], "filter": "color = blue" })).await;
    let (response2, code2) = index.get_all_documents_raw("?ids=2&filter=color=blue").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 2,
          "color": "blue"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) = index
        .fetch_documents(
            json!({ "offset": 1, "limit": 1, "ids": [0, 1, 2, 3], "filter": "color != blue" }),
        )
        .await;
    let (response2, code2) =
        index.get_all_documents_raw("?ids=0,1,2,3&filter=color!=blue&offset=1&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 3
        }
      ],
      "offset": 1,
      "limit": 1,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    let (response, code) = index
        .fetch_documents(json!({ "limit": 1, "ids": [0, 1, 2,3], "filter": "color != blue", "fields": ["color"] }))
        .await;
    let (response2, code2) =
        index.get_all_documents_raw("?ids=0,1,2,3&limit=1&filter=color!=blue&fields=color").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "color": "red"
        }
      ],
      "offset": 0,
      "limit": 1,
      "total": 2
    }
    "###);
    assert_eq!(code, code2);
    assert_eq!(response, response2);

    // Now testing more complex filter that the get route can't represent

    let (response, code) = index
        .fetch_documents(json!({ "ids": [0, "2"], "filter": [["color = blue", "color = red"]] }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [
        {
          "id": 0,
          "color": "red"
        },
        {
          "id": 2,
          "color": "blue"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);

    let (response, code) = index
        .fetch_documents(json!({ "filter": [["color != blue"], "color EXISTS"], "ids": [1, 2, 3] }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "results": [],
      "offset": 0,
      "limit": 20,
      "total": 0
    }
    "###);
}

#[actix_rt::test]
async fn get_document_with_vectors() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": null }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(value.uid()).await.succeeded();

    // by default you shouldn't see the `_vectors` object
    let (documents, _code) = index.get_all_documents(Default::default()).await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir"
        },
        {
          "id": 1,
          "name": "echo"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
    let (documents, _code) = index.get_document(0, None).await;
    snapshot!(json_string!(documents), @r###"
    {
      "id": 0,
      "name": "kefir"
    }
    "###);

    // if we try to retrieve the vectors with the `fields` parameter they
    // still shouldn't be displayed
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions {
            fields: Some(vec!["name", "_vectors"]),
            ..Default::default()
        })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "name": "kefir"
        },
        {
          "name": "echo"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
    let (documents, _code) =
        index.get_document(0, Some(json!({"fields": ["name", "_vectors"]}))).await;
    snapshot!(json_string!(documents), @r###"
    {
      "name": "kefir"
    }
    "###);

    // If we specify the retrieve vectors boolean and nothing else we should get the vectors
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "manual": {
              "embeddings": [],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
    let (documents, _code) = index.get_document(0, Some(json!({"retrieveVectors": true}))).await;
    snapshot!(json_string!(documents), @r###"
    {
      "id": 0,
      "name": "kefir",
      "_vectors": {
        "manual": {
          "embeddings": [
            [
              0.0,
              0.0,
              0.0
            ]
          ],
          "regenerate": false
        }
      }
    }
    "###);

    // If we specify the retrieve vectors boolean and exclude vectors form the `fields` we should still get the vectors
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions {
            retrieve_vectors: true,
            fields: Some(vec!["name"]),
            ..Default::default()
        })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "name": "kefir",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "name": "echo",
          "_vectors": {
            "manual": {
              "embeddings": [],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
    let (documents, _code) =
        index.get_document(0, Some(json!({"retrieveVectors": true, "fields": ["name"]}))).await;
    snapshot!(json_string!(documents), @r###"
    {
      "name": "kefir",
      "_vectors": {
        "manual": {
          "embeddings": [
            [
              0.0,
              0.0,
              0.0
            ]
          ],
          "regenerate": false
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn test_fetch_documents_pagination_with_sorting() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    // Set name as sortable attribute
    let (task, code) = index.update_settings_sortable_attributes(json!(["name"])).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!((0..50)
        .map(|i| json!({"id": i, "name": format!("doc_{:05}", std::cmp::min(i, 5))}))
        .collect::<Vec<_>>());

    // Add documents as described in the bug report
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Request 1 (first page): offset 0, limit 2
    let (response, code) = index
        .fetch_documents(json!({
            "offset": 0,
            "limit": 2,
            "sort": ["name:asc"]
        }))
        .await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    snapshot!(json_string!(results), @r###"
    [
      {
        "id": 0,
        "name": "doc_00000"
      },
      {
        "id": 1,
        "name": "doc_00001"
      }
    ]
    "###);

    // Request 2 (second page): offset 2, limit 2
    let (response, code) = index
        .fetch_documents(json!({
            "offset": 2,
            "limit": 2,
            "sort": ["name:asc"]
        }))
        .await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    snapshot!(json_string!(results), @r###"
    [
      {
        "id": 2,
        "name": "doc_00002"
      },
      {
        "id": 3,
        "name": "doc_00003"
      }
    ]
    "###);

    // Request 3 (third page): offset 4, limit 2
    let (response, code) = index
        .fetch_documents(json!({
            "offset": 4,
            "limit": 2,
            "sort": ["name:asc"]
        }))
        .await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    snapshot!(json_string!(results), @r###"
    [
      {
        "id": 4,
        "name": "doc_00004"
      },
      {
        "id": 5,
        "name": "doc_00005"
      }
    ]
    "###);

    // Request 4 (fourth page): offset 6, limit 2
    let (response, code) = index
        .fetch_documents(json!({
            "offset": 6,
            "limit": 2,
            "sort": ["name:asc"]
        }))
        .await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    snapshot!(json_string!(results), @r###"
    [
      {
        "id": 6,
        "name": "doc_00005"
      },
      {
        "id": 7,
        "name": "doc_00005"
      }
    ]
    "###);
}

// <https://github.com/meilisearch/meilisearch/issues/5998>
#[actix_rt::test]
async fn get_document_sort_field_not_in_any_document() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _code) = index.update_settings_sortable_attributes(json!(["created_at"])).await;
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        { "id": 1, "name": "Document 1" },
        { "id": 2, "name": "Document 2" }
    ]);
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .fetch_documents(json!({
            "sort": ["created_at:asc"]
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "results": [
        {
          "id": 1,
          "name": "Document 1"
        },
        {
          "id": 2,
          "name": "Document 2"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
}

#[actix_rt::test]
async fn get_document_sort_includes_docs_without_field() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _code) = index.update_settings_sortable_attributes(json!(["created_at"])).await;
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        { "id": 1, "name": "Doc without created_at" },
        { "id": 2, "name": "Doc with created_at", "created_at": "2025-01-15" },
        { "id": 3, "name": "Another doc without created_at" },
        { "id": 4, "name": "Another doc with created_at", "created_at": "2025-01-10" }
    ]);
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .fetch_documents(json!({
            "sort": ["created_at:asc"]
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "results": [
        {
          "id": 4,
          "name": "Another doc with created_at",
          "created_at": "2025-01-10"
        },
        {
          "id": 2,
          "name": "Doc with created_at",
          "created_at": "2025-01-15"
        },
        {
          "id": 1,
          "name": "Doc without created_at"
        },
        {
          "id": 3,
          "name": "Another doc without created_at"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);
}

#[actix_rt::test]
async fn get_document_sort_desc_includes_docs_without_field() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _code) = index.update_settings_sortable_attributes(json!(["priority"])).await;
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        { "id": 1, "name": "Low priority", "priority": 1 },
        { "id": 2, "name": "No priority" },
        { "id": 3, "name": "High priority", "priority": 10 }
    ]);
    let (task, _code) = index.add_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index
        .fetch_documents(json!({
            "sort": ["priority:desc"]
        }))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "results": [
        {
          "id": 3,
          "name": "High priority",
          "priority": 10
        },
        {
          "id": 1,
          "name": "Low priority",
          "priority": 1
        },
        {
          "id": 2,
          "name": "No priority"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);
}
