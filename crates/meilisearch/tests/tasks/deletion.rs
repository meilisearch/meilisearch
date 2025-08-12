use crate::common::Server;
use crate::json;
use crate::tasks::OffsetDateTime;
use meili_snap::{json_string, snapshot};
use time::format_description::well_known::Rfc3339;
use urlencoding::encode;

#[actix_rt::test]
async fn delete_task() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Add a document
    let (task, code) = index
        .add_documents(json!([{"id": 1, "free": "palestine", "asc_desc_rank": 1}]), Some("id"))
        .await;
    snapshot!(code, @r#"202 Accepted"#);
    let task_uid = task["taskUid"].as_u64().unwrap();
    server.wait_task(task).await.succeeded();

    // Delete tasks
    let (task, code) = server.delete_tasks(&format!("uids={task_uid}")).await;
    snapshot!(code, @"200 OK");
    let value = server.wait_task(task).await.succeeded();
    snapshot!(value, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
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
    "#);

    // Check that the task is deleted
    let (value, code) = index.list_tasks().await;
    snapshot!(code, @r#"200 OK"#);
    snapshot!(value, @r#"
    {
      "results": [],
      "total": 0,
      "limit": 20,
      "from": null,
      "next": null
    }
    "#);
}

async fn delete_tasks_time_bounds_inner(name: &str) {
    let server = Server::new().await;
    let index = server.unique_index();

    // Add documents
    for i in 0..2 {
        let (task, code) =
            index.add_documents(json!([{"id": i, "country": "taiwan"}]), Some("id")).await;
        snapshot!(code, @r#"202 Accepted"#);
        server.wait_task(task).await.succeeded();
    }

    let time1 = OffsetDateTime::now_utc();

    for i in 2..4 {
        let (task, code) =
            index.add_documents(json!([{"id": i, "country": "taiwan"}]), Some("id")).await;
        snapshot!(code, @r#"202 Accepted"#);
        server.wait_task(task).await.succeeded();
    }

    let time2 = OffsetDateTime::now_utc();

    for i in 4..6 {
        let (task, code) =
            index.add_documents(json!([{"id": i, "country": "taiwan"}]), Some("id")).await;
        snapshot!(code, @r#"202 Accepted"#);
        server.wait_task(task).await.succeeded();
    }

    // Delete tasks with before_enqueued and after_enqueued
    let (task, code) = server
        .delete_tasks(&format!(
            "before{name}={}&after{name}={}",
            encode(&time2.format(&Rfc3339).unwrap()),
            encode(&time1.format(&Rfc3339).unwrap()),
        ))
        .await;
    snapshot!(code, @"200 OK");
    let value = server.wait_task(task).await.succeeded();
    snapshot!(json_string!(value, {
        ".details.originalFilter" => "[ignored]",
        ".duration" => "[duration]",
        ".enqueuedAt" => "[date]",
        ".startedAt" => "[date]",
        ".finishedAt" => "[date]"
    }), @r#"
    {
      "uid": 6,
      "batchUid": 6,
      "indexUid": null,
      "status": "succeeded",
      "type": "taskDeletion",
      "canceledBy": null,
      "details": {
        "matchedTasks": 2,
        "deletedTasks": 2,
        "originalFilter": "[ignored]"
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    // Check that the task is deleted
    let (value, code) = server.tasks().await;
    snapshot!(code, @r#"200 OK"#);
    snapshot!(json_string!(value, {
        ".results[].duration" => "[duration]",
        ".results[].enqueuedAt" => "[date]",
        ".results[].startedAt" => "[date]",
        ".results[].finishedAt" => "[date]",
        ".results[].details.originalFilter" => "[ignored]"
    }), @r#"
    {
      "results": [
        {
          "uid": 6,
          "batchUid": 6,
          "indexUid": null,
          "status": "succeeded",
          "type": "taskDeletion",
          "canceledBy": null,
          "details": {
            "matchedTasks": 2,
            "deletedTasks": 2,
            "originalFilter": "[ignored]"
          },
          "error": null,
          "duration": "[duration]",
          "enqueuedAt": "[date]",
          "startedAt": "[date]",
          "finishedAt": "[date]"
        },
        {
          "uid": 5,
          "batchUid": 5,
          "indexUid": "[uuid]",
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
        },
        {
          "uid": 4,
          "batchUid": 4,
          "indexUid": "[uuid]",
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
        },
        {
          "uid": 1,
          "batchUid": 1,
          "indexUid": "[uuid]",
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
        },
        {
          "uid": 0,
          "batchUid": 0,
          "indexUid": "[uuid]",
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
      ],
      "total": 5,
      "limit": 20,
      "from": 6,
      "next": null
    }
    "#);
}

#[actix_rt::test]
async fn delete_tasks_enqueued() {
    delete_tasks_time_bounds_inner("EnqueuedAt").await;
}

#[actix_rt::test]
async fn delete_tasks_started() {
    delete_tasks_time_bounds_inner("StartedAt").await;
}

#[actix_rt::test]
async fn delete_tasks_finished() {
    delete_tasks_time_bounds_inner("FinishedAt").await;
}
