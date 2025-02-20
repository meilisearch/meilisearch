mod errors;

use meili_snap::insta::assert_json_snapshot;
use meili_snap::snapshot;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn error_get_unexisting_batch_status() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _coder) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_batch(1).await;

    let expected_response = json!({
        "message": "Batch `1` not found.",
        "code": "batch_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#batch_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}

#[actix_rt::test]
async fn get_batch_status() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (_response, code) = index.get_batch(0).await;
    assert_eq!(code, 200);
}

#[actix_rt::test]
async fn list_batches() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.list_batches().await;
    assert_eq!(code, 200);
    assert_eq!(
        response["results"].as_array().unwrap().len(),
        2,
        "{}",
        serde_json::to_string_pretty(&response).unwrap()
    );
}

#[actix_rt::test]
async fn list_batches_pagination_and_reverse() {
    let server = Server::new().await;
    // First of all we want to create a lot of batches very quickly. The fastest way is to delete a lot of unexisting indexes
    let mut last_batch = None;
    for i in 0..10 {
        let index = server.index(format!("test-{i}"));
        last_batch = Some(index.create(None).await.0.uid());
    }
    server.wait_task(last_batch.unwrap()).await;

    let (response, code) = server.batches_filter("limit=3").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let batch_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{batch_ids:?}"), @"[9, 8, 7]");

    let (response, code) = server.batches_filter("limit=3&from=1").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let batch_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{batch_ids:?}"), @"[1, 0]");

    // In reversed order

    let (response, code) = server.batches_filter("limit=3&reverse=true").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let batch_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{batch_ids:?}"), @"[0, 1, 2]");

    let (response, code) = server.batches_filter("limit=3&from=8&reverse=true").await;
    assert_eq!(code, 200);
    let results = response["results"].as_array().unwrap();
    let batch_ids: Vec<_> = results.iter().map(|ret| ret["uid"].as_u64().unwrap()).collect();
    snapshot!(format!("{batch_ids:?}"), @"[8, 9]");
}

#[actix_rt::test]
async fn list_batches_with_star_filters() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let index = server.index("test");
    let (task, _code) = index.create(None).await;
    index.wait_task(task.uid()).await.failed();

    let (response, code) = index.service.get("/batches?indexUids=test").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index.service.get("/batches?indexUids=*").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index.service.get("/batches?indexUids=*,pasteque").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index.service.get("/batches?types=*").await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) =
        index.service.get("/batches?types=*,documentAdditionOrUpdate&statuses=*").await;
    assert_eq!(code, 200, "{:?}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index
        .service
        .get("/batches?types=*,documentAdditionOrUpdate&statuses=*,failed&indexUids=test")
        .await;
    assert_eq!(code, 200, "{:?}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index
        .service
        .get("/batches?types=*,documentAdditionOrUpdate&statuses=*,failed&indexUids=test,*")
        .await;
    assert_eq!(code, 200, "{:?}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn list_batches_status_filtered() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.failed();

    let (response, code) = index.filtered_batches(&[], &["succeeded"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);

    let (response, code) = index.filtered_batches(&[], &["succeeded"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);

    let (response, code) = index.filtered_batches(&[], &["succeeded", "failed"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn list_batches_type_filtered() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _) = index.delete().await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.filtered_batches(&["indexCreation"], &[], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);

    let (response, code) =
        index.filtered_batches(&["indexCreation", "IndexDeletion"], &[], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);

    let (response, code) = index.filtered_batches(&["indexCreation"], &[], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 1);
}

#[actix_rt::test]
async fn list_batches_invalid_canceled_by_filter() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.filtered_batches(&[], &[], &["0"]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 0);
}

#[actix_rt::test]
async fn list_batches_status_and_type_filtered() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = index.update(Some("id")).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.filtered_batches(&["indexCreation"], &["failed"], &[]).await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 0);

    let (response, code) = index
        .filtered_batches(
            &["indexCreation", "IndexUpdate"],
            &["succeeded", "processing", "enqueued"],
            &[],
        )
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn list_batch_filter_error() {
    let server = Server::new().await;

    let (response, code) = server.batches_filter("lol=pied").await;
    assert_eq!(code, 400, "{}", response);
    meili_snap::snapshot!(meili_snap::json_string!(response), @r#"
    {
      "message": "Unknown parameter `lol`: expected one of `limit`, `from`, `reverse`, `batchUids`, `uids`, `canceledBy`, `types`, `statuses`, `indexUids`, `afterEnqueuedAt`, `beforeEnqueuedAt`, `afterStartedAt`, `beforeStartedAt`, `afterFinishedAt`, `beforeFinishedAt`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "#);

    let (response, code) = server.batches_filter("uids=pied").await;
    assert_eq!(code, 400, "{}", response);
    meili_snap::snapshot!(meili_snap::json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `uids`: could not parse `pied` as a positive integer",
      "code": "invalid_task_uids",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_uids"
    }
    "#);

    let (response, code) = server.batches_filter("from=pied").await;
    assert_eq!(code, 400, "{}", response);
    meili_snap::snapshot!(meili_snap::json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `from`: could not parse `pied` as a positive integer",
      "code": "invalid_task_from",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_from"
    }
    "#);

    let (response, code) = server.batches_filter("beforeStartedAt=pied").await;
    assert_eq!(code, 400, "{}", response);
    meili_snap::snapshot!(meili_snap::json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `beforeStartedAt`: `pied` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_before_started_at"
    }
    "#);
}

#[actix_web::test]
async fn test_summarized_document_addition_or_update() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) =
        index.add_documents(json!({ "id": 42, "content": "doggos & fluff" }), None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "documentAdditionOrUpdate": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]",
        "writeChannelCongestion": "[writeChannelCongestion]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _status_code) =
        index.add_documents(json!({ "id": 42, "content": "doggos & fluff" }), Some("id")).await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(1).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 1,
      "progress": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "documentAdditionOrUpdate": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]",
        "writeChannelCongestion": "[writeChannelCongestion]"
      },
      "duration": "[duration]",
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
    let (batch, _) = index.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
      "details": {
        "providedIds": 3,
        "deletedDocuments": 0
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "documentDeletion": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    index.create(None).await;
    let (task, _status_code) = index.delete_batch(vec![42]).await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(2).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 2,
      "progress": null,
      "details": {
        "providedIds": 1,
        "deletedDocuments": 0
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "documentDeletion": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
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
    let (batch, _) = index.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "documentDeletion": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    index.create(None).await;
    let (task, _status_code) =
        index.delete_document_by_filter(json!({ "filter": "doggo = bernese" })).await;
    index.wait_task(task.uid()).await.failed();
    let (batch, _) = index.get_batch(2).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 2,
      "progress": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "documentDeletion": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    index.update_settings(json!({ "filterableAttributes": ["doggo"] })).await;
    let (task, _status_code) =
        index.delete_document_by_filter(json!({ "filter": "doggo = bernese" })).await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(4).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r#"
    {
      "uid": 4,
      "progress": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "documentDeletion": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);
}

#[actix_web::test]
async fn test_summarized_delete_document_by_id() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _status_code) = index.delete_document(1).await;
    index.wait_task(task.uid()).await.failed();
    let (batch, _) = index.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".uid" => "[uid]",
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r#"
    {
      "uid": "[uid]",
      "progress": null,
      "details": {
        "providedIds": 1,
        "deletedDocuments": 0
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "documentDeletion": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    index.create(None).await;
    let (task, _status_code) = index.delete_document(42).await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(2).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r#"
    {
      "uid": 2,
      "progress": null,
      "details": {
        "providedIds": 1,
        "deletedDocuments": 0
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "documentDeletion": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);
}

#[actix_web::test]
async fn test_summarized_settings_update() {
    let server = Server::new().await;
    let index = server.index("test");
    // here we should find my payload even in the failed batch.
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
    let (batch, _) = index.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
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
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "settingsUpdate": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
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
    let (batch, _) = index.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
      "details": {},
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "indexCreation": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _status_code) = index.create(Some("doggos")).await;
    index.wait_task(task.uid()).await.failed();
    let (batch, _) = index.get_batch(1).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 1,
      "progress": null,
      "details": {
        "primaryKey": "doggos"
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "indexCreation": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
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
    let batch = index.wait_task(ret.uid()).await.failed();
    snapshot!(batch,
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
    // both batches may get autobatched and the deleted documents count will be wrong.
    let (ret, _code) =
        index.add_documents(json!({ "id": 42, "content": "doggos & fluff" }), Some("id")).await;
    let batch = index.wait_task(ret.uid()).await.succeeded();
    snapshot!(batch,
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
    let batch = index.wait_task(ret.uid()).await.succeeded();
    snapshot!(batch,
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
    let batch = index.wait_task(ret.uid()).await.failed();
    snapshot!(batch,
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
    let (batch, _) = index.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
      "details": {},
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "indexUpdate": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (task, _status_code) = index.update(Some("bones")).await;
    index.wait_task(task.uid()).await.failed();
    let (batch, _) = index.get_batch(1).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 1,
      "progress": null,
      "details": {
        "primaryKey": "bones"
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "indexUpdate": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // And run the same two tests once the index do exists.
    index.create(None).await;

    let (task, _status_code) = index.update(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(3).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r#"
    {
      "uid": 3,
      "progress": null,
      "details": {},
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "indexUpdate": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    let (task, _status_code) = index.update(Some("bones")).await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(4).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 4,
      "progress": null,
      "details": {
        "primaryKey": "bones"
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "indexUpdate": 1
        },
        "indexUids": {
          "test": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
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
    let (batch, _) = server.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
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
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "failed": 1
        },
        "types": {
          "indexSwap": 1
        },
        "indexUids": {},
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    server.index("doggos").create(None).await;
    let (task, _status_code) = server.index("cattos").create(None).await;
    server
        .index_swap(json!([
            { "indexes": ["doggos", "cattos"] }
        ]))
        .await;
    server.wait_task(task.uid()).await.succeeded();
    let (batch, _) = server.get_batch(1).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 1,
      "progress": null,
      "details": {},
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "indexCreation": 1
        },
        "indexUids": {
          "doggos": 1
        },
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_batch_cancelation() {
    let server = Server::new().await;
    let index = server.index("doggos");
    // to avoid being flaky we're only going to cancel an already finished batch :(
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = server.cancel_tasks("uids=0").await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(1).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 1,
      "progress": null,
      "details": {
        "matchedTasks": 1,
        "canceledTasks": 0,
        "originalFilter": "?uids=0"
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "taskCancelation": 1
        },
        "indexUids": {},
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_web::test]
async fn test_summarized_batch_deletion() {
    let server = Server::new().await;
    let index = server.index("doggos");
    // to avoid being flaky we're only going to delete an already finished batch :(
    let (task, _status_code) = index.create(None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (task, _status_code) = server.delete_tasks("uids=0").await;
    index.wait_task(task.uid()).await.succeeded();
    let (batch, _) = index.get_batch(1).await;
    assert_json_snapshot!(batch,
        {
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 1,
      "progress": null,
      "details": {
        "matchedTasks": 1,
        "deletedTasks": 1,
        "originalFilter": "?uids=0"
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "taskDeletion": 1
        },
        "indexUids": {},
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
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
    let (batch, _) = server.get_batch(0).await;
    assert_json_snapshot!(batch,
        {
            ".details.dumpUid" => "[dumpUid]",
            ".duration" => "[duration]",
            ".enqueuedAt" => "[date]",
            ".startedAt" => "[date]",
            ".finishedAt" => "[date]",
            ".stats.callTrace" => "[callTrace]",
            ".stats.writeChannelCongestion" => "[writeChannelCongestion]"
        },
        @r###"
    {
      "uid": 0,
      "progress": null,
      "details": {
        "dumpUid": "[dumpUid]"
      },
      "stats": {
        "totalNbTasks": 1,
        "status": {
          "succeeded": 1
        },
        "types": {
          "dumpCreation": 1
        },
        "indexUids": {},
        "callTrace": "[callTrace]"
      },
      "duration": "[duration]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}
