use serde_json::json;

use crate::common::encoder::Encoder;
use crate::common::{GetAllDocumentsOptions, Server};

#[actix_rt::test]
async fn error_document_update_create_index_bad_uid() {
    let server = Server::new().await;
    let index = server.index("883  fj!");
    let (response, code) = index.update_documents(json!([{"id": 1}]), None).await;

    let expected_response = json!({
        "message": "`883  fj!` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
        "code": "invalid_index_uid",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    });

    assert_eq!(code, 400);
    assert_eq!(response, expected_response);
}

#[actix_rt::test]
async fn document_update_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "primary": 1,
            "content": "foo",
        }
    ]);
    let (_response, code) = index.update_documents(documents, Some("primary")).await;
    assert_eq!(code, 202);

    index.wait_task(0).await;

    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["uid"], 0);
    assert_eq!(response["type"], "documentAdditionOrUpdate");
    assert_eq!(response["details"]["indexedDocuments"], 1);
    assert_eq!(response["details"]["receivedDocuments"], 1);

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn update_document() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (_response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    index.wait_task(0).await;

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_task(1).await;

    let (response, code) = index.get_task(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert_eq!(response.to_string(), r##"{"doc_id":1,"content":"foo","other":"bar"}"##);
}

#[actix_rt::test]
async fn update_document_gzip_encoded() {
    let server = Server::new().await;
    let index = server.index_with_encoder("test", Encoder::Gzip);

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (_response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    index.wait_task(0).await;

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_task(1).await;

    let (response, code) = index.get_task(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert_eq!(response.to_string(), r##"{"doc_id":1,"content":"foo","other":"bar"}"##);
}

#[actix_rt::test]
async fn update_larger_dataset() {
    let server = Server::new().await;
    let index = server.index("test");
    let documents = serde_json::from_str(include_str!("../assets/test_set.json")).unwrap();
    index.update_documents(documents, None).await;
    index.wait_task(0).await;
    let (response, code) = index.get_task(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["type"], "documentAdditionOrUpdate");
    assert_eq!(response["details"]["indexedDocuments"], 77);
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions { limit: Some(1000), ..Default::default() })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 77);
}

#[actix_rt::test]
async fn error_update_documents_bad_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "docid": "foo & bar",
            "content": "foobar"
        }
    ]);
    index.update_documents(documents, None).await;
    let response = index.wait_task(1).await;
    assert_eq!(response["status"], json!("failed"));
    assert_eq!(
        response["error"]["message"],
        json!(
            r#"Document identifier `"foo & bar"` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_)."#
        )
    );
    assert_eq!(response["error"]["code"], json!("invalid_document_id"));
    assert_eq!(response["error"]["type"], json!("invalid_request"));
    assert_eq!(
        response["error"]["link"],
        json!("https://docs.meilisearch.com/errors#invalid_document_id")
    );
}

#[actix_rt::test]
async fn error_update_documents_missing_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    index.update_documents(documents, None).await;
    let response = index.wait_task(1).await;
    assert_eq!(response["status"], "failed");
    assert_eq!(
        response["error"]["message"],
        r#"Document doesn't have a `docid` attribute: `{"id":"11","content":"foobar"}`."#
    );
    assert_eq!(response["error"]["code"], "missing_document_id");
    assert_eq!(response["error"]["type"], "invalid_request");
    assert_eq!(
        response["error"]["link"],
        "https://docs.meilisearch.com/errors#missing_document_id"
    );
}
