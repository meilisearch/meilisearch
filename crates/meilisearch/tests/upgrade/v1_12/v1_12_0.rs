// This test is the first test of the dumpless upgrade.
// It must test pretty much all the features of meilisearch because the other tests will only tests
// the new features they introduced.

use insta::assert_json_snapshot;
use manifest_dir_macros::exist_relative_path;
use meili_snap::{json_string, snapshot};
use meilisearch::Opt;

use crate::common::{default_settings, Server, Value};
use crate::json;
use crate::upgrade::copy_dir_all;

#[actix_rt::test]
async fn import_v1_12_0() {
    let temp = tempfile::tempdir().unwrap();
    let original_db_path = exist_relative_path!("tests/upgrade/v1_12/v1_12_0.ms");
    let options = Opt {
        experimental_dumpless_upgrade: true,
        master_key: Some("kefir".to_string()),
        ..default_settings(temp.path())
    };
    copy_dir_all(original_db_path, &options.db_path).unwrap();
    let mut server = Server::new_with_options(options).await.unwrap();
    server.use_api_key("kefir");

    check_the_keys(&server).await;
    check_the_index_scheduler(&server).await;
    check_the_index_features(&server).await;
}

/// We must ensure that the keys database is still working:
/// 1. Check its content
/// 2. Ensure we can still query the keys
/// 3. Ensure we can still update the keys
async fn check_the_keys(server: &Server) {
    // All the api keys are still present
    let (keys, _) = server.list_api_keys("").await;
    snapshot!(json_string!(keys, { ".results[].updatedAt" => "[date]" }), name: "list_all_keys");

    // We can still query the keys
    let (by_uid, _) = server.get_api_key("9a77a636-e4e2-4f1a-93ac-978c368fd596").await;
    let (by_key, _) = server
        .get_api_key("760c6345918b5ab1d251c1a3e8f9666547628a710d91f6b1d558ba944ef15746")
        .await;

    assert_eq!(by_uid, by_key);
    snapshot!(json_string!(by_uid, { ".updatedAt" => "[date]" }), @r#"
    {
      "name": "Kefir",
      "description": "My little kefirino key",
      "key": "760c6345918b5ab1d251c1a3e8f9666547628a710d91f6b1d558ba944ef15746",
      "uid": "9a77a636-e4e2-4f1a-93ac-978c368fd596",
      "actions": [
        "stats.get",
        "documents.*"
      ],
      "indexes": [
        "kefir"
      ],
      "expiresAt": null,
      "createdAt": "2025-01-16T14:43:20.863318893Z",
      "updatedAt": "[date]"
    }
    "#);

    // Remove a key
    let (_value, status) = server.delete_api_key("9a77a636-e4e2-4f1a-93ac-978c368fd596").await;
    snapshot!(status, @"204 No Content");

    // Update a key
    let (value, _) = server
        .patch_api_key(
            "dc699ff0-a053-4956-a46a-912e51b3316b",
            json!({ "name": "kefir", "description": "the patou" }),
        )
        .await;
    snapshot!(json_string!(value, { ".updatedAt" => "[date]" }), @r#"
    {
      "name": "kefir",
      "description": "the patou",
      "key": "4d9376547ed779a05dde416148e7e98bd47530e28c500be674c9e60b2accb814",
      "uid": "dc699ff0-a053-4956-a46a-912e51b3316b",
      "actions": [
        "search"
      ],
      "indexes": [
        "*"
      ],
      "expiresAt": null,
      "createdAt": "2025-01-16T14:24:46.264041777Z",
      "updatedAt": "[date]"
    }
    "#);

    // Everything worked
    let (keys, _) = server.list_api_keys("").await;
    snapshot!(json_string!(keys, { ".results[].updatedAt" => "[date]" }), name: "list_all_keys_after_removing_kefir");
}

/// We must ensure the index-scheduler database is still working:
/// 1. We can query the indexes and their metadata
/// 2. The upgrade task has been spawned and has been processed (wait for it to finish or it'll be flaky)
/// 3. Snapshot the whole queue, the tasks and batches should always be the same after update
/// 4. Query the batches and tasks on all filters => the databases should still works
/// 5. Ensure we can still update the queue
///    5.1. Delete tasks until a batch is removed
///    5.2. Enqueue a new task
///    5.3. Create an index
async fn check_the_index_scheduler(server: &Server) {
    // All the indexes are still present
    let (indexes, _) = server.list_indexes(None, None).await;
    snapshot!(indexes, @r#"
    {
      "results": [
        {
          "uid": "kefir",
          "createdAt": "2025-01-16T16:45:16.020663157Z",
          "updatedAt": "2025-01-23T11:36:22.634859166Z",
          "primaryKey": "id"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "#);
    // And their metadata are still right
    let (stats, _) = server.stats().await;
    assert_json_snapshot!(stats, {
        ".databaseSize" => "[bytes]",
        ".usedDatabaseSize" => "[bytes]"
    },
    @r###"
    {
      "databaseSize": "[bytes]",
      "usedDatabaseSize": "[bytes]",
      "lastUpdate": "2025-01-23T11:36:22.634859166Z",
      "indexes": {
        "kefir": {
          "numberOfDocuments": 1,
          "isIndexing": false,
          "numberOfEmbeddings": 0,
          "numberOfEmbeddedDocuments": 0,
          "fieldDistribution": {
            "age": 1,
            "description": 1,
            "id": 1,
            "name": 1,
            "surname": 1
          }
        }
      }
    }
    "###);

    // Wait until the upgrade has been applied to all indexes to avoid flakyness
    let (tasks, _) = server.tasks_filter("types=upgradeDatabase&limit=1").await;
    server.wait_task(Value(tasks["results"][0].clone()).uid()).await.succeeded();

    // Tasks and batches should still work
    // We rewrite the first task for all calls because it may be the upgrade database with unknown dates and duration.
    // The other tasks should NOT change
    let (tasks, _) = server.tasks_filter("limit=1000").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "the_whole_task_queue_once_everything_has_been_processed");
    let (batches, _) = server.batches_filter("limit=1000").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "the_whole_batch_queue_once_everything_has_been_processed");

    // Tests all the tasks query parameters
    let (tasks, _) = server.tasks_filter("uids=10").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_uids_equal_10");
    let (tasks, _) = server.tasks_filter("batchUids=10").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_batchUids_equal_10");
    let (tasks, _) = server.tasks_filter("statuses=canceled").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_statuses_equal_canceled");
    // types has already been tested above to retrieve the upgrade database
    let (tasks, _) = server.tasks_filter("canceledBy=19").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_canceledBy_equal_19");
    let (tasks, _) = server.tasks_filter("beforeEnqueuedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_beforeEnqueuedAt_equal_2025-01-16T16_47_41");
    let (tasks, _) = server.tasks_filter("afterEnqueuedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_afterEnqueuedAt_equal_2025-01-16T16_47_41");
    let (tasks, _) = server.tasks_filter("beforeStartedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_beforeStartedAt_equal_2025-01-16T16_47_41");
    let (tasks, _) = server.tasks_filter("afterStartedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_afterStartedAt_equal_2025-01-16T16_47_41");
    let (tasks, _) = server.tasks_filter("beforeFinishedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_beforeFinishedAt_equal_2025-01-16T16_47_41");
    let (tasks, _) = server.tasks_filter("afterFinishedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(tasks, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]" }), name: "tasks_filter_afterFinishedAt_equal_2025-01-16T16_47_41");

    // Tests all the batches query parameters
    let (batches, _) = server.batches_filter("uids=10").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_uids_equal_10");
    let (batches, _) = server.batches_filter("batchUids=10").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_batchUids_equal_10");
    let (batches, _) = server.batches_filter("statuses=canceled").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_statuses_equal_canceled");
    // types has already been tested above to retrieve the upgrade database
    let (batches, _) = server.batches_filter("canceledBy=19").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_canceledBy_equal_19");
    let (batches, _) = server.batches_filter("beforeEnqueuedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_beforeEnqueuedAt_equal_2025-01-16T16_47_41");
    let (batches, _) = server.batches_filter("afterEnqueuedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_afterEnqueuedAt_equal_2025-01-16T16_47_41");
    let (batches, _) = server.batches_filter("beforeStartedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_beforeStartedAt_equal_2025-01-16T16_47_41");
    let (batches, _) = server.batches_filter("afterStartedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_afterStartedAt_equal_2025-01-16T16_47_41");
    let (batches, _) = server.batches_filter("beforeFinishedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_beforeFinishedAt_equal_2025-01-16T16_47_41");
    let (batches, _) = server.batches_filter("afterFinishedAt=2025-01-16T16:47:41Z").await;
    snapshot!(json_string!(batches, { ".results[0].duration" => "[duration]", ".results[0].enqueuedAt" => "[date]", ".results[0].startedAt" => "[date]", ".results[0].finishedAt" => "[date]", ".results[0].stats.callTrace" => "[callTrace]", ".results[0].stats.writeChannelCongestion" => "[writeChannelCongestion]" }), name: "batches_filter_afterFinishedAt_equal_2025-01-16T16_47_41");

    let (stats, _) = server.stats().await;
    assert_json_snapshot!(stats, {
        ".databaseSize" => "[bytes]",
        ".usedDatabaseSize" => "[bytes]"
    },
    @r###"
    {
      "databaseSize": "[bytes]",
      "usedDatabaseSize": "[bytes]",
      "lastUpdate": "2025-01-23T11:36:22.634859166Z",
      "indexes": {
        "kefir": {
          "numberOfDocuments": 1,
          "isIndexing": false,
          "numberOfEmbeddings": 0,
          "numberOfEmbeddedDocuments": 0,
          "fieldDistribution": {
            "age": 1,
            "description": 1,
            "id": 1,
            "name": 1,
            "surname": 1
          }
        }
      }
    }
    "###);
    let index = server.index("kefir");
    let (stats, _) = index.stats().await;
    snapshot!(stats, @r###"
    {
      "numberOfDocuments": 1,
      "isIndexing": false,
      "numberOfEmbeddings": 0,
      "numberOfEmbeddedDocuments": 0,
      "fieldDistribution": {
        "age": 1,
        "description": 1,
        "id": 1,
        "name": 1,
        "surname": 1
      }
    }
    "###);

    // Delete all the tasks of a specific batch
    let (task, _) = server.delete_tasks("batchUids=10").await;
    server.wait_task(task.uid()).await.succeeded();

    let (tasks, _) = server.tasks_filter("batchUids=10").await;
    snapshot!(tasks, name: "task_by_batchUids_after_deletion");
    let (tasks, _) = server.batches_filter("batchUids=10").await;
    snapshot!(tasks, name: "batch_by_batchUids_after_deletion");

    let index = server.index("kefirausaurus");
    let (task, _) = index.create(Some("kefid")).await;
    server.wait_task(task.uid()).await.succeeded();
}

/// Ensuring the index roughly works with filter and sort.
/// More specific test will be made for the next versions everytime they updates a feature
async fn check_the_index_features(server: &Server) {
    let kefir = server.index("kefir");

    let (settings, _) = kefir.settings().await;
    snapshot!(settings, name: "kefir_settings");

    let (results, _status) =
        kefir.search_post(json!({ "sort": ["age:asc"], "filter": "surname = kefirounet" })).await;
    snapshot!(results, name: "search_with_sort_and_filter");
}
