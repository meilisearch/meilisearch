use meili_snap::{json_string, snapshot};

use crate::common::{shared_does_not_exists_index, GetAllDocumentsOptions, Server};
use crate::json;

#[actix_rt::test]
async fn delete_one_document_unexisting_index() {
    let server = Server::new_shared();
    let index = shared_does_not_exists_index().await;
    let (task, code) = index.delete_document_by_filter_fail(json!({"filter": "a = b"})).await;
    assert_eq!(code, 202);

    server.wait_task(task.uid()).await.failed();
}

#[actix_rt::test]
async fn delete_one_unexisting_document() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.create(None).await;
    let (response, code) = index.delete_document(0).await;
    assert_eq!(code, 202, "{response}");
    server.wait_task(response.uid()).await.succeeded();
}

#[actix_rt::test]
async fn delete_one_document() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _status_code) =
        index.add_documents(json!([{ "id": 0, "content": "foobar" }]), None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, status_code) = index.delete_document(0).await;
    assert_eq!(status_code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let (_response, code) = index.get_document(0, None).await;
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn clear_all_documents_unexisting_index() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.clear_all_documents().await;
    assert_eq!(code, 202);

    server.wait_task(task.uid()).await.failed();
}

#[actix_rt::test]
async fn clear_all_documents() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _status_code) = index
        .add_documents(
            json!([{ "id": 1, "content": "foobar" }, { "id": 0, "content": "foobar" }]),
            None,
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, code) = index.clear_all_documents().await;
    assert_eq!(code, 202);

    let _update = server.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert!(response["results"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn clear_all_documents_empty_index() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, _status_code) = index.create(None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, code) = index.clear_all_documents().await;
    assert_eq!(code, 202);

    let _update = server.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert!(response["results"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn error_delete_batch_unexisting_index() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.delete_batch(vec![]).await;
    let expected_response = json!({
        "message": format!("Index `{}` not found.", index.uid),
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });
    assert_eq!(code, 202);

    let response = server.wait_task(task.uid()).await.failed();
    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn delete_batch() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task,_status_code) = index.add_documents(json!([{ "id": 1, "content": "foobar" }, { "id": 0, "content": "foobar" }, { "id": 3, "content": "foobar" }]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, code) = index.delete_batch(vec![1, 0]).await;
    assert_eq!(code, 202);

    let _update = server.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);
    assert_eq!(response["results"][0]["id"], json!(3));
}

#[actix_rt::test]
async fn delete_no_document_batch() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task,_status_code) = index.add_documents(json!([{ "id": 1, "content": "foobar" }, { "id": 0, "content": "foobar" }, { "id": 3, "content": "foobar" }]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.delete_batch(vec![]).await;
    assert_eq!(code, 202, "{response}");

    let _update = server.wait_task(response.uid()).await.succeeded();
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 3);
}

#[actix_rt::test]
async fn delete_document_by_filter() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.update_settings_filterable_attributes(json!(["color"])).await;
    let (task, _status_code) = index
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

    let (stats, _) = index.stats().await;
    snapshot!(json_string!(stats, {
        ".rawDocumentDbSize" => "[size]",
        ".avgDocumentSize" => "[size]",
    }), @r###"
    {
      "numberOfDocuments": 4,
      "rawDocumentDbSize": "[size]",
      "avgDocumentSize": "[size]",
      "isIndexing": false,
      "numberOfEmbeddings": 0,
      "numberOfEmbeddedDocuments": 0,
      "fieldDistribution": {
        "color": 3,
        "id": 4
      }
    }
    "###);

    let (response, code) =
        index.delete_document_by_filter(json!({ "filter": "color = blue"})).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "documentDeletion",
      "enqueuedAt": "[date]"
    }
    "###);

    let response = server.wait_task(response.uid()).await.succeeded();
    snapshot!(json_string!(response, { ".uid" => "[uid]", ".batchUid" => "[batch_uid]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 2,
        "originalFilter": "\"color = blue\""
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (stats, _) = index.stats().await;
    snapshot!(json_string!(stats, {
        ".rawDocumentDbSize" => "[size]",
        ".avgDocumentSize" => "[size]",
    }), @r###"
    {
      "numberOfDocuments": 2,
      "rawDocumentDbSize": "[size]",
      "avgDocumentSize": "[size]",
      "isIndexing": false,
      "numberOfEmbeddings": 0,
      "numberOfEmbeddedDocuments": 0,
      "fieldDistribution": {
        "color": 1,
        "id": 2
      }
    }
    "###);

    let (documents, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
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

    let (response, code) =
        index.delete_document_by_filter(json!({ "filter": "color NOT EXISTS"})).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "documentDeletion",
      "enqueuedAt": "[date]"
    }
    "###);

    let response = server.wait_task(response.uid()).await.succeeded();
    snapshot!(json_string!(response, { ".uid" => "[uid]", ".batchUid" => "[batch_uid]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 1,
        "originalFilter": "\"color NOT EXISTS\""
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (stats, _) = index.stats().await;
    snapshot!(json_string!(stats, {
        ".rawDocumentDbSize" => "[size]",
        ".avgDocumentSize" => "[size]",
    }), @r###"
    {
      "numberOfDocuments": 1,
      "rawDocumentDbSize": "[size]",
      "avgDocumentSize": "[size]",
      "isIndexing": false,
      "numberOfEmbeddings": 0,
      "numberOfEmbeddedDocuments": 0,
      "fieldDistribution": {
        "color": 1,
        "id": 1
      }
    }
    "###);

    let (documents, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
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
async fn delete_document_by_complex_filter() {
    let server = Server::new_shared();
    let index = server.unique_index();
    index.update_settings_filterable_attributes(json!(["color"])).await;
    let (task, _status_code) = index
        .add_documents(
            json!([
                { "id": 0, "color": "red" },
                { "id": 1, "color": "blue" },
                { "id": 2, "color": "blue" },
                { "id": 3, "color": "green" },
                { "id": 4 },
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();
    let (response, code) = index
        .delete_document_by_filter(
            json!({ "filter": ["color != red", "color != green", "color EXISTS"] }),
        )
        .await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "documentDeletion",
      "enqueuedAt": "[date]"
    }
    "###);

    let response = server.wait_task(response.uid()).await.succeeded();
    snapshot!(json_string!(response, { ".uid" => "[uid]", ".batchUid" => "[batch_uid]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 2,
        "originalFilter": "[\"color != red\",\"color != green\",\"color EXISTS\"]"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "color": "red"
        },
        {
          "id": 3,
          "color": "green"
        },
        {
          "id": 4
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);

    let (response, code) = index
        .delete_document_by_filter(json!({ "filter": [["color = green", "color NOT EXISTS"]] }))
        .await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".taskUid" => "[task_uid]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "taskUid": "[task_uid]",
      "indexUid": "[uuid]",
      "status": "enqueued",
      "type": "documentDeletion",
      "enqueuedAt": "[date]"
    }
    "###);

    let response = server.wait_task(response.uid()).await.succeeded();
    snapshot!(json_string!(response, { ".uid" => "[uid]", ".batchUid" => "[batch_uid]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "[uuid]",
      "status": "succeeded",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 2,
        "originalFilter": "[[\"color = green\",\"color NOT EXISTS\"]]"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
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
