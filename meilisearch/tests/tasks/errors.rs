use meili_snap::*;
use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn task_bad_uids() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"uids": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task uid `doggo` is invalid. It should only contain numeric characters.",
      "code": "invalid_task_uids",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-uids"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"uids": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task uid `doggo` is invalid. It should only contain numeric characters.",
      "code": "invalid_task_uids",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-uids"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"uids": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task uid `doggo` is invalid. It should only contain numeric characters.",
      "code": "invalid_task_uids",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-uids"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_canceled_by() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"canceledBy": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task canceledBy `doggo` is invalid. It should only contains numeric characters separated by `,` character.",
      "code": "invalid_task_canceled_by",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-canceled-by"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"canceledBy": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task canceledBy `doggo` is invalid. It should only contains numeric characters separated by `,` character.",
      "code": "invalid_task_canceled_by",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-canceled-by"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"canceledBy": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task canceledBy `doggo` is invalid. It should only contains numeric characters separated by `,` character.",
      "code": "invalid_task_canceled_by",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-canceled-by"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_types() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"types": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task type `doggo` is invalid. Available task types are `documentAdditionOrUpdate`, `documentDeletion`, `settingsUpdate`, `indexCreation`, `indexDeletion`, `indexUpdate`, `indexSwap`, `taskCancelation`, `taskDeletion`, `dumpCreation`, `snapshotCreation`",
      "code": "invalid_task_types",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-types"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"types": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task type `doggo` is invalid. Available task types are `documentAdditionOrUpdate`, `documentDeletion`, `settingsUpdate`, `indexCreation`, `indexDeletion`, `indexUpdate`, `indexSwap`, `taskCancelation`, `taskDeletion`, `dumpCreation`, `snapshotCreation`",
      "code": "invalid_task_types",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-types"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"types": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task type `doggo` is invalid. Available task types are `documentAdditionOrUpdate`, `documentDeletion`, `settingsUpdate`, `indexCreation`, `indexDeletion`, `indexUpdate`, `indexSwap`, `taskCancelation`, `taskDeletion`, `dumpCreation`, `snapshotCreation`",
      "code": "invalid_task_types",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-types"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_statuses() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"statuses": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task status `doggo` is invalid. Available task statuses are `enqueued`, `processing`, `succeeded`, `failed`, `canceled`.",
      "code": "invalid_task_statuses",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-statuses"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"statuses": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task status `doggo` is invalid. Available task statuses are `enqueued`, `processing`, `succeeded`, `failed`, `canceled`.",
      "code": "invalid_task_statuses",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-statuses"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"statuses": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task status `doggo` is invalid. Available task statuses are `enqueued`, `processing`, `succeeded`, `failed`, `canceled`.",
      "code": "invalid_task_statuses",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-statuses"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_index_uids() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"indexUids": "the good doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "the good doggo is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-index-uid"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"indexUids": "the good doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "the good doggo is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-index-uid"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"indexUids": "the good doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "the good doggo is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-index-uid"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_limit() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"limit": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Query deserialize error: invalid digit found in string",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad-request"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"limit": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Query deserialize error: unknown field `limit`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad-request"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"limit": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Query deserialize error: unknown field `limit`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad-request"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_from() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"from": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Query deserialize error: invalid digit found in string",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad-request"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"from": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Query deserialize error: unknown field `from`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad-request"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"from": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Query deserialize error: unknown field `from`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad-request"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_after_enqueued_at() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"afterEnqueuedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterEnqueuedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-enqueued-at"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"afterEnqueuedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterEnqueuedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-enqueued-at"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"afterEnqueuedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterEnqueuedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-enqueued-at"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_before_enqueued_at() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"beforeEnqueuedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeEnqueuedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-enqueued-at"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"beforeEnqueuedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeEnqueuedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-enqueued-at"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"beforeEnqueuedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeEnqueuedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-enqueued-at"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_after_started_at() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"afterStartedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterStartedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-started-at"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"afterStartedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterStartedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-started-at"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"afterStartedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterStartedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-started-at"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_before_started_at() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"beforeStartedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeStartedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-started-at"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"beforeStartedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeStartedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-started-at"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"beforeStartedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeStartedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-started-at"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_after_finished_at() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"afterFinishedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterFinishedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-finished-at"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"afterFinishedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterFinishedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-finished-at"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"afterFinishedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `afterFinishedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-after-finished-at"
    }
    "###);
}

#[actix_rt::test]
async fn task_bad_before_finished_at() {
    let server = Server::new().await;

    let (response, code) = server.tasks_filter(json!({"beforeFinishedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeFinishedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-finished-at"
    }
    "###);

    let (response, code) = server.cancel_tasks(json!({"beforeFinishedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeFinishedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-finished-at"
    }
    "###);

    let (response, code) = server.delete_tasks(json!({"beforeFinishedAt": "doggo"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Task `beforeFinishedAt` `doggo` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid-task-before-finished-at"
    }
    "###);
}
