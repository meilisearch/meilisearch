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
}
