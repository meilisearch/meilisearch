use crate::common::GetAllDocumentsOptions;
use crate::common::Server;

use serde_json::json;

// TODO: partial test since we are testing error, amd error is not yet fully implemented in
// transplant
#[actix_rt::test]
async fn get_unexisting_index_single_document() {
    let server = Server::new().await;
    let (_response, code) = server.index("test").get_document(1, None).await;
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn error_get_unexisting_document() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
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
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    let documents = serde_json::json!([
        {
            "id": 0,
            "content": "foobar",
        }
    ]);
    let (_, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    index.wait_update_id(0).await;
    let (response, code) = index.get_document(0, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        response,
        serde_json::json!(        {
            "id": 0,
            "content": "foobar",
        })
    );
}

#[actix_rt::test]
async fn error_get_unexisting_index_all_documents() {
    let server = Server::new().await;
    let (response, code) = server
        .index("test")
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;

    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn get_no_documents() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;
    assert_eq!(code, 201);

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;
    assert_eq!(code, 200);
    assert!(response.as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn get_all_documents_no_options() {
    let server = Server::new().await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;
    assert_eq!(code, 200);
    let arr = response.as_array().unwrap();
    assert_eq!(arr.len(), 20);
    let first = serde_json::json!({
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
    assert_eq!(first, arr[0]);
}

#[actix_rt::test]
async fn test_get_all_documents_limit() {
    let server = Server::new().await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            limit: Some(5),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 5);
    assert_eq!(response.as_array().unwrap()[0]["id"], 0);
}

#[actix_rt::test]
async fn test_get_all_documents_offset() {
    let server = Server::new().await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            offset: Some(5),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(response.as_array().unwrap()[0]["id"], 13);
}

#[actix_rt::test]
async fn test_get_all_documents_attributes_to_retrieve() {
    let server = Server::new().await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            attributes_to_retrieve: Some(vec!["name"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(
        response.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .keys()
            .count(),
        1
    );
    assert!(response.as_array().unwrap()[0]
        .as_object()
        .unwrap()
        .get("name")
        .is_some());

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            attributes_to_retrieve: Some(vec![]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(
        response.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .keys()
            .count(),
        0
    );

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            attributes_to_retrieve: Some(vec!["wrong"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(
        response.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .keys()
            .count(),
        0
    );

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            attributes_to_retrieve: Some(vec!["name", "tags"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(
        response.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .keys()
            .count(),
        2
    );

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            attributes_to_retrieve: Some(vec!["*"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(
        response.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .keys()
            .count(),
        16
    );

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            attributes_to_retrieve: Some(vec!["*", "wrong"]),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(
        response.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .keys()
            .count(),
        16
    );
}

#[actix_rt::test]
async fn get_documents_displayed_attributes() {
    let server = Server::new().await;
    let index = server.index("test");
    index
        .update_settings(json!({"displayedAttributes": ["gender"]}))
        .await;
    index.load_test_set().await;

    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(
        response.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .keys()
            .count(),
        1
    );
    assert!(response.as_array().unwrap()[0]
        .as_object()
        .unwrap()
        .get("gender")
        .is_some());

    let (response, code) = index.get_document(0, None).await;
    assert_eq!(code, 200);
    assert_eq!(response.as_object().unwrap().keys().count(), 1);
    assert!(response.as_object().unwrap().get("gender").is_some());
}
