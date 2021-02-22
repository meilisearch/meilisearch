use crate::common::Server;
use crate::common::GetAllDocumentsOptions;

// TODO: partial test since we are testing error, amd error is not yet fully implemented in
// transplant
#[actix_rt::test]
async fn get_unexisting_index_single_document() {
    let server = Server::new().await;
    let (_response, code) = server
        .index("test")
        .get_document(1, None)
        .await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn get_unexisting_index_all_documents() {
    let server = Server::new().await;
    let (_response, code) = server
        .index("test")
        .get_all_documents(GetAllDocumentsOptions::default())
        .await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn get_no_documents() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_, code) = index.create(None).await;
    assert_eq!(code, 200);

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert!(response.as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn get_all_documents_no_options() {
    let server = Server::new().await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
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

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions { limit: Some(5), ..Default::default() }).await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 5);
    assert_eq!(response.as_array().unwrap()[0]["id"], 0);
}

#[actix_rt::test]
async fn test_get_all_documents_offset() {
    let server = Server::new().await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions { offset: Some(5), ..Default::default() }).await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(response.as_array().unwrap()[0]["id"], 13);
}

#[actix_rt::test]
async fn test_get_all_documents_attributes_to_retrieve() {
    let server = Server::new().await;
    let index = server.index("test");
    index.load_test_set().await;

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions { attributes_to_retrieve: Some(vec!["name"]), ..Default::default() }).await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(response.as_array().unwrap()[0].as_object().unwrap().keys().count(), 1);
    assert!(response.as_array().unwrap()[0].as_object().unwrap().get("name").is_some());

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions { attributes_to_retrieve: Some(vec![]), ..Default::default() }).await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(response.as_array().unwrap()[0].as_object().unwrap().keys().count(), 0);

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions { attributes_to_retrieve: Some(vec!["name", "tags"]), ..Default::default() }).await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 20);
    assert_eq!(response.as_array().unwrap()[0].as_object().unwrap().keys().count(), 2);
}
