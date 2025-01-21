use std::time::Instant;

use big_s::S;
use maplit::btreeset;
use meili_snap::snapshot;
use meilisearch_types::milli::obkv_to_json;
use meilisearch_types::milli::update::IndexDocumentsMethod::*;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::tasks::KindWithContent;

use crate::insta_snapshot::snapshot_index_scheduler;
use crate::test_utils::Breakpoint::*;
use crate::test_utils::{index_creation_task, read_json, FailureLocation};
use crate::IndexScheduler;

#[test]
fn fail_in_process_batch_for_index_creation() {
    let (index_scheduler, mut handle) =
        IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

    let kind = index_creation_task("catto", "mouse");

    let _task = index_scheduler.register(kind, None, false).unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_register");

    handle.advance_one_failed_batch();

    // Still in the first iteration
    assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
}

#[test]
fn fail_in_process_batch_for_document_addition() {
    let (index_scheduler, mut handle) =
        IndexScheduler::test(true, vec![(1, FailureLocation::InsideProcessBatch)]);

    let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0).unwrap();
    let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
    file.persist().unwrap();
    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
    handle.advance_till([Start, BatchCreated]);

    snapshot!(
        snapshot_index_scheduler(&index_scheduler),
        name: "document_addition_batch_created"
    );

    handle.advance_till([ProcessBatchFailed, AfterProcessing]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_failed");
}

#[test]
fn fail_in_update_task_after_process_batch_success_for_document_addition() {
    let (index_scheduler, mut handle) = IndexScheduler::test(
        true,
        vec![(1, FailureLocation::UpdatingTaskAfterProcessBatchSuccess { task_uid: 0 })],
    );

    let content = r#"
        {
            "id": 1,
            "doggo": "bob"
        }"#;

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0).unwrap();
    let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
    file.persist().unwrap();
    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    handle.advance_till([Start]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "document_addition_succeeded_but_index_scheduler_not_updated");

    handle.advance_till([BatchCreated, InsideProcessBatch, ProcessBatchSucceeded]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_batch_succeeded");

    // At this point the next time the scheduler will try to progress it should encounter
    // a critical failure and have to wait for 1s before retrying anything.

    let before_failure = Instant::now();
    handle.advance_till([Start]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_failing_to_commit");
    let failure_duration = before_failure.elapsed();
    assert!(failure_duration.as_millis() >= 1000);

    handle.advance_till([BatchCreated, InsideProcessBatch, ProcessBatchSucceeded, AfterProcessing]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "task_successfully_processed");
}

#[test]
fn fail_in_process_batch_for_document_deletion() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    use meilisearch_types::settings::{Settings, Unchecked};
    let mut new_settings: Box<Settings<Unchecked>> = Box::default();
    new_settings.filterable_attributes = Setting::Set(btreeset!(S("catto")));

    index_scheduler
        .register(
            KindWithContent::SettingsUpdate {
                index_uid: S("doggos"),
                new_settings,
                is_deletion: false,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();

    let content = r#"[
            { "id": 1, "doggo": "jean bob" },
            { "id": 2, "catto": "jorts" },
            { "id": 3, "doggo": "bork" }
        ]"#;

    let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(0).unwrap();
    let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
    file.persist().unwrap();
    index_scheduler
        .register(
            KindWithContent::DocumentAdditionOrUpdate {
                index_uid: S("doggos"),
                primary_key: Some(S("id")),
                method: ReplaceDocuments,
                content_file: uuid,
                documents_count,
                allow_index_creation: true,
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_setting_and_document_addition");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_adding_the_settings");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_adding_the_documents");

    index_scheduler
        .register(
            KindWithContent::DocumentDeletion {
                index_uid: S("doggos"),
                documents_ids: vec![S("1")],
            },
            None,
            false,
        )
        .unwrap();
    // This one should not be catched by Meilisearch but it's still nice to handle it because if one day we break the filters it could happens
    index_scheduler
        .register(
            KindWithContent::DocumentDeletionByFilter {
                index_uid: S("doggos"),
                filter_expr: serde_json::json!(true),
            },
            None,
            false,
        )
        .unwrap();
    // Should fail because the ids are not filterable
    index_scheduler
        .register(
            KindWithContent::DocumentDeletionByFilter {
                index_uid: S("doggos"),
                filter_expr: serde_json::json!("id = 2"),
            },
            None,
            false,
        )
        .unwrap();
    index_scheduler
        .register(
            KindWithContent::DocumentDeletionByFilter {
                index_uid: S("doggos"),
                filter_expr: serde_json::json!("catto EXISTS"),
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_document_deletions");

    // Everything should be batched together
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_removing_the_documents");

    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents_remaining_should_only_be_bork");
}

#[test]
fn panic_in_process_batch_for_index_creation() {
    let (index_scheduler, mut handle) =
        IndexScheduler::test(true, vec![(1, FailureLocation::PanicInsideProcessBatch)]);

    let kind = index_creation_task("catto", "mouse");

    let _task = index_scheduler.register(kind, None, false).unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

    handle.advance_till([Start, BatchCreated, ProcessBatchFailed, AfterProcessing]);

    // Still in the first iteration
    assert_eq!(*index_scheduler.run_loop_iteration.read().unwrap(), 1);
    // No matter what happens in process_batch, the index_scheduler should be internally consistent
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "index_creation_failed");
}
