use meili_snap::{json_string, snapshot};

use crate::common::encoder::Encoder;
use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;

#[actix_rt::test]
async fn error_document_update_create_index_bad_uid() {
    let server = Server::new_shared();
    let index = server.unique_index_with_prefix("883  fj!");
    let (response, code) = index.update_documents(json!([{"id": 1}]), None).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "`883  fj!-[uuid]` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }"###);
}

#[actix_rt::test]
async fn document_update_with_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
        {
            "primary": 1,
            "content": "foo",
        }
    ]);
    let (response, code) = index.update_documents(documents, Some("primary")).await;
    assert_eq!(code, 202);

    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.get_task(response.uid()).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "documentAdditionOrUpdate");
    assert_eq!(response["details"]["indexedDocuments"], 1);
    assert_eq!(response["details"]["receivedDocuments"], 1);

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn update_document() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    server.wait_task(response.uid()).await.succeeded();

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.get_task(response.uid()).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    snapshot!(response, @r###"
    {
      "doc_id": 1,
      "content": "foo",
      "other": "bar"
    }
    "###);
}

#[actix_rt::test]
async fn update_document_gzip_encoded() {
    let server = Server::new_shared();
    let index = server.unique_index_with_encoder(Encoder::Gzip);

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    server.wait_task(response.uid()).await.succeeded();

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.get_task(response.uid()).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    snapshot!(response, @r###"
    {
      "doc_id": 1,
      "content": "foo",
      "other": "bar"
    }
    "###);
}

#[actix_rt::test]
async fn update_larger_dataset() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let documents = serde_json::from_str(include_str!("../assets/test_set.json")).unwrap();
    let (task, _code) = index.update_documents(documents, None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_task(task.uid()).await;
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
    let server = Server::new_shared();
    let index = server.unique_index();
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "docid": "foo & bar",
            "content": "foobar"
        }
    ]);
    let (task, _code) = index.update_documents(documents, None).await;
    let response = server.wait_task(task.uid()).await;
    assert_eq!(response["status"], json!("failed"));
    assert_eq!(
        response["error"]["message"],
        json!(
            r#"Document identifier `"foo & bar"` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), and can not be more than 511 bytes."#
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
    let server = Server::new_shared();
    let index = server.unique_index();
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    let (task, _code) = index.update_documents(documents, None).await;
    let response = server.wait_task(task.uid()).await;
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

#[actix_rt::test]
async fn update_faceted_document() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!({
            "rankingRules": ["facet:asc"],
        }))
        .await;
    assert_eq!("202", code.as_str(), "{:?}", response);
    server.wait_task(response.uid()).await.succeeded();

    let documents: Vec<_> = (0..1000)
        .map(|id| {
            json!({
                "doc_id": id,
                "facet": (id/3),
            })
        })
        .collect();

    let (response, code) = index.add_documents(documents.into(), None).await;
    assert_eq!(code, 202);

    server.wait_task(response.uid()).await.succeeded();

    let documents = json!([
        {
            "doc_id": 9,
            "facet": 1.5,
        }
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    server.wait_task(response.uid()).await.succeeded();

    index
        .search(json!({"limit": 10}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "doc_id": 0,
                "facet": 0
              },
              {
                "doc_id": 1,
                "facet": 0
              },
              {
                "doc_id": 2,
                "facet": 0
              },
              {
                "doc_id": 3,
                "facet": 1
              },
              {
                "doc_id": 4,
                "facet": 1
              },
              {
                "doc_id": 5,
                "facet": 1
              },
              {
                "doc_id": 9,
                "facet": 1.5
              },
              {
                "doc_id": 6,
                "facet": 2
              },
              {
                "doc_id": 7,
                "facet": 2
              },
              {
                "doc_id": 8,
                "facet": 2
              }
            ]
            "###);
        })
        .await;
}
