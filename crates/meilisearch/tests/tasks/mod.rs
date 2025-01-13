mod errors;
mod webhook;

use meili_snap::insta::assert_json_snapshot;
use meili_snap::snapshot;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn error_get_unexisting_task_status() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_task(1).await;

    let expected_response = json!({
        "message": "Task `1` not found.",
        "code": "task_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#task_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn get_task_status() {
    let server = Server::new().await;
    let index = server.index("test");
    let (create_task, _status_code) = index.create(None).await;
    let (add_task, _status_code) = index
        .add_documents(
            json!([{
                "id": 1,
                "content": "foobar",
            }]),
            None,
        )
        .await;
    index.wait_task(create_task.uid()).await.succeeded();
    let (_response, code) = index.get_task(add_task.uid()).await;
    assert_eq!(code, 200);
    // TODO check response format, as per #48
}

#[actix_rt::test]
async fn list_tasks() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    index
        .add_documents(serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(), None)
        .await;
    let (response, code) = index.list_tasks().await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn list_tasks_pagination_and_reverse() {
    let server = Server::new().await;
    // First of all we want to create a lot of tasks very quickly. The fastest way is to delete a lot of unexisting indexes
    let mut last_task = None;
    for i in 0..10 {
        let index = server.index(format!("test-{i}"));
        last_task = Some(index.create(None).await.0.uid());
    }
    server.wait_task(last_task.unwrap()).await;

    let (response, code) = server.tasks_filter("limit=3").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let task_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{task_ids:?}"), @"[9, 8, 7]");

    let (response, code) = server.tasks_filter("limit=3&from=1").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let task_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{task_ids:?}"), @"[1, 0]");

    // In reversed order

    let (response, code) = server.tasks_filter("limit=3&reverse=true").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let task_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{task_ids:?}"), @"[0, 1, 2]");

    let (response, code) = server.tasks_filter("limit=3&from=8&reverse=true").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let task_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{task_ids:?}"), @"[8, 9]");
}

#[actix_rt::test]
async fn list_tasks_with_star_filters() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    index
        .add_documents(serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(), None)
        .await;
    let (response, code) = index.service.get("/tasks?indexUids=test").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index.service.get("/tasks?indexUids=*").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index.service.get("/tasks?indexUids=*,pasteque").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index.service.get("/tasks?types=*").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) =
        index.service.get("/tasks?types=*,documentAdditionOrUpdate&statuses=*").await;
    assert_eq!(code, 200, "{:?}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index
        .service
        .get("/tasks?types=*,documentAdditionOrUpdate&statuses=*,failed&indexUids=test")
        .await;
    assert_eq!(code, 200, "{:?}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index
        .service
        .get("/tasks?types=*,documentAdditionOrUpdate&statuses=*,failed&indexUids=test,*")
        .await;
    assert_eq!(code, 200, "{:?}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn list_tasks_status_filtered() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.failed();

    let (response, code) = index.filtered_tasks(&[], &["succeeded"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);

    let (response, code) = index.filtered_tasks(&[], &["succeeded"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);

    let (response, code) = index.filtered_tasks(&[], &["succeeded", "failed"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn list_tasks_type_filtered() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    index
        .add_documents(serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(), None)
        .await;

    let (response, code) = index.filtered_tasks(&["indexCreation"], &[], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);

    let (response, code) =
        index.filtered_tasks(&["indexCreation", "documentAdditionOrUpdate"], &[], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn list_tasks_invalid_canceled_by_filter() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    index
        .add_documents(serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(), None)
        .await;

    let (response, code) = index.filtered_tasks(&[], &[], &["0"]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 0);
}

#[actix_rt::test]
async fn list_tasks_status_and_type_filtered() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    index
        .add_documents(serde_json::from_str(include_str!("../assets/test_set.json")).unwrap(), None)
        .await;

    let (response, code) = index.filtered_tasks(&["indexCreation"], &["failed"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 0);

    let (response, code) = index
        .filtered_tasks(
            &["indexCreation", "documentAdditionOrUpdate"],
            &["succeeded", "processing", "enqueued"],
            &[],
        )
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

macro_rules! assert_valid_summarized_task {
    ($response:expr, $task_type:literal, $index:literal) => {{
        assert_eq!($response.as_object().unwrap().len(), 5);
        assert!($response["taskUid"].as_u64().is_some());
        assert_eq!($response["indexUid"], $index);
        assert_eq!($response["status"], "enqueued");
        assert_eq!($response["type"], $task_type);
        let date = $response["enqueuedAt"].as_str().expect("missing date");

        OffsetDateTime::parse(date, &Rfc3339).unwrap();
    }};
}

#[actix_web::test]
async fn test_summarized_task_view() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, _) = index.create(None).await;
    assert_valid_summarized_task!(response, "indexCreation", "test");

    let (response, _) = index.update(None).await;
    assert_valid_summarized_task!(response, "indexUpdate", "test");

    let (response, _) = index.update_settings(json!({})).await;
    assert_valid_summarized_task!(response, "settingsUpdate", "test");

    let (response, _) = index.update_documents(json!([{"id": 1}]), None).await;
    assert_valid_summarized_task!(response, "documentAdditionOrUpdate", "test");

    let (response, _) = index.add_documents(json!([{"id": 1}]), None).await;
    assert_valid_summarized_task!(response, "documentAdditionOrUpdate", "test");

    let (response, _) = index.delete_document(1).await;
    assert_valid_summarized_task!(response, "documentDeletion", "test");

    let (response, _) = index.clear_all_documents().await;
    assert_valid_summarized_task!(response, "documentDeletion", "test");

    let (response, _) = index.delete().await;
    assert_valid_summarized_task!(response, "indexDeletion", "test");
}

#[actix_web::test]
async fn test_summarized_document_addition_or_update() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) =
        index.add_documents(json!({ "id": 42, "content": "doggos & fluff" }), None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(0).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _status_code) =
        index.add_documents(json!({ "id": 42, "content": "doggos & fluff" }), Some("id")).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(1).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_delete_documents_by_batch() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.delete_batch(vec![1, 2, 3]).await;
    index.wait_task(task.uid()).await.failed();
    let (task, _) = index.get_task(0).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "failed",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 3,
        "deletedDocuments": 0,
        "originalFilter": null
      },
      "error": {
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    index.create(None).await;
    let (del_task, _status_code) = index.delete_batch(vec![42]).await;
    index.wait_task(del_task.uid()).await.succeeded();
    let (task, _) = index.get_task(del_task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 2,
      "batchUid": 2,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 1,
        "deletedDocuments": 0,
        "originalFilter": null
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_delete_documents_by_filter() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _status_code) =
        index.delete_document_by_filter(json!({ "filter": "doggo = bernese" })).await;
    index.wait_task(task.uid()).await.failed();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "failed",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "error": {
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    index.create(None).await;
    let (task, _status_code) =
        index.delete_document_by_filter(json!({ "filter": "doggo = bernese" })).await;
    index.wait_task(task.uid()).await.failed();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 2,
      "batchUid": 2,
      "indexUid": "test",
      "status": "failed",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "error": {
        "message": "Index `test`: Attribute `doggo` is not filterable. This index does not have configured filterable attributes.\n1:6 doggo = bernese",
        "code": "invalid_document_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    index.update_settings(json!({ "filterableAttributes": ["doggo"] })).await;
    let (task, _status_code) =
        index.delete_document_by_filter(json!({ "filter": "doggo = bernese" })).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 4,
      "batchUid": 4,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_delete_document_by_id() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.delete_document(1).await;
    index.wait_task(task.uid()).await.failed();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "failed",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 1,
        "deletedDocuments": 0,
        "originalFilter": null
      },
      "error": {
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    index.create(None).await;
    let (task, _status_code) = index.delete_document(42).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 2,
      "batchUid": 2,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 1,
        "deletedDocuments": 0,
        "originalFilter": null
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_settings_update() {
    let server = Server::new().await;
    let index = server.index("test");
    // here we should find my payload even in the failed task.
    let (response, code) = index.update_settings(json!({ "rankingRules": ["custom"] })).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value at `.rankingRules[0]`: `custom` ranking rule is invalid. Valid ranking rules are words, typo, sort, proximity, attribute, exactness and custom ranking rules.",
      "code": "invalid_settings_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_ranking_rules"
    }
    "###);

    let (task,_status_code) = index.update_settings(json!({ "displayedAttributes": ["doggos", "name"], "filterableAttributes": ["age", "nb_paw_pads"], "sortableAttributes": ["iq"] })).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "displayedAttributes": [
          "doggos",
          "name"
        ],
        "filterableAttributes": [
          "age",
          "nb_paw_pads"
        ],
        "sortableAttributes": [
          "iq"
        ]
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_index_creation() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "succeeded",
      "type": "indexCreation",
      "canceledBy": null,
      "details": {
        "primaryKey": null
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _status_code) = index.create(Some("doggos")).await;
    index.wait_task(task.uid()).await.failed();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "test",
      "status": "failed",
      "type": "indexCreation",
      "canceledBy": null,
      "details": {
        "primaryKey": "doggos"
      },
      "error": {
        "message": "Index `test` already exists.",
        "code": "index_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_already_exists"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_index_deletion() {
    let server = Server::new().await;
    let index = server.index("test");
    let (ret, _code) = index.delete().await;
    let task = index.wait_task(ret.uid()).await;
    snapshot!(task,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "failed",
      "type": "indexDeletion",
      "canceledBy": null,
      "details": {
        "deletedDocuments": 0
      },
      "error": {
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // is the details correctly set when documents are actually deleted.
    // /!\ We need to wait for the document addition to be processed otherwise, if the test runs too slow,
    // both tasks may get autobatched and the deleted documents count will be wrong.
    let (ret, _code) =
        index.add_documents(json!({ "id": 42, "content": "doggos & fluff" }), Some("id")).await;
    let task = index.wait_task(ret.uid()).await;
    snapshot!(task,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (ret, _code) = index.delete().await;
    let task = index.wait_task(ret.uid()).await;
    snapshot!(task,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "indexDeletion",
      "canceledBy": null,
      "details": {
        "deletedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // What happens when you delete an index that doesn't exists.
    let (ret, _code) = index.delete().await;
    let task = index.wait_task(ret.uid()).await;
    snapshot!(task,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "failed",
      "type": "indexDeletion",
      "canceledBy": null,
      "details": {
        "deletedDocuments": 0
      },
      "error": {
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_index_update() {
    let server = Server::new().await;
    let index = server.index("test");
    // If the index doesn't exist yet, we should get errors with or without the primary key.
    let (task, _status_code) = index.update(None).await;
    index.wait_task(task.uid()).await.failed();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "failed",
      "type": "indexUpdate",
      "canceledBy": null,
      "details": {
        "primaryKey": null
      },
      "error": {
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _status_code) = index.update(Some("bones")).await;
    index.wait_task(task.uid()).await.failed();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "test",
      "status": "failed",
      "type": "indexUpdate",
      "canceledBy": null,
      "details": {
        "primaryKey": "bones"
      },
      "error": {
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // And run the same two tests once the index do exists.
    index.create(None).await;

    let (task, _status_code) = index.update(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 3,
      "batchUid": 3,
      "indexUid": "test",
      "status": "succeeded",
      "type": "indexUpdate",
      "canceledBy": null,
      "details": {
        "primaryKey": null
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _status_code) = index.update(Some("bones")).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 4,
      "batchUid": 4,
      "indexUid": "test",
      "status": "succeeded",
      "type": "indexUpdate",
      "canceledBy": null,
      "details": {
        "primaryKey": "bones"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_index_swap() {
    let server = Server::new().await;
    let (task, _status_code) = server
        .index_swap(json!([
            { "indexes": ["doggos", "cattos"] }
        ]))
        .await;
    server.wait_task(task.uid()).await.failed();
    let (task, _) = server.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": null,
      "status": "failed",
      "type": "indexSwap",
      "canceledBy": null,
      "details": {
        "swaps": [
          {
            "indexes": [
              "doggos",
              "cattos"
            ]
          }
        ]
      },
      "error": {
        "message": "Indexes `cattos`, `doggos` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _code) = server.index("doggos").create(None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, _code) = server.index("cattos").create(None).await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, _code) = server
        .index_swap(json!([
            { "indexes": ["doggos", "cattos"] }
        ]))
        .await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, _) = server.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 3,
      "batchUid": 3,
      "indexUid": null,
      "status": "succeeded",
      "type": "indexSwap",
      "canceledBy": null,
      "details": {
        "swaps": [
          {
            "indexes": [
              "doggos",
              "cattos"
            ]
          }
        ]
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_task_cancelation() {
    let server = Server::new().await;
    let index = server.index("doggos");
    // to avoid being flaky we're only going to cancel an already finished task :(
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = server.cancel_tasks("uids=0").await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": null,
      "status": "succeeded",
      "type": "taskCancelation",
      "canceledBy": null,
      "details": {
        "matchedTasks": 1,
        "canceledTasks": 0,
        "originalFilter": "?uids=0"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_task_deletion() {
    let server = Server::new().await;
    let index = server.index("doggos");
    // to avoid being flaky we're only going to delete an already finished task :(
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = server.delete_tasks("uids=0").await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": null,
      "status": "succeeded",
      "type": "taskDeletion",
      "canceledBy": null,
      "details": {
        "matchedTasks": 1,
        "deletedTasks": 1,
        "originalFilter": "?uids=0"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_dump_creation() {
    let server = Server::new().await;
    let (task, _status_code) = server.create_dump().await;
    server.wait_task(task.uid()).await;
    let (task, _) = server.get_task(task.uid()).await;
    assert_json_snapshot!(task,
        { ".details.dumpUid" => "[dumpUid]", ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" },
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": null,
      "status": "succeeded",
      "type": "dumpCreation",
      "canceledBy": null,
      "details": {
        "dumpUid": "[dumpUid]"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}
