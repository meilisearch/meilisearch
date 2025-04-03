use big_s::S;
use meili_snap::snapshot;
use meilisearch_types::milli::obkv_to_json;
use meilisearch_types::milli::update::IndexDocumentsMethod::*;
use meilisearch_types::tasks::KindWithContent;

use crate::insta_snapshot::snapshot_index_scheduler;
use crate::test_utils::read_json;
use crate::test_utils::Breakpoint::*;
use crate::IndexScheduler;

#[test]
fn document_addition() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

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
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_register");

    handle.advance_till([Start, BatchCreated]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_the_batch_creation");

    handle.advance_till([InsideProcessBatch, ProcessBatchSucceeded, AfterProcessing]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "once_everything_is_processed");
}

#[test]
fn document_addition_and_document_deletion() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

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
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
    index_scheduler
        .register(
            KindWithContent::DocumentDeletion {
                index_uid: S("doggos"),
                documents_ids: vec![S("1"), S("2")],
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

    handle.advance_one_successful_batch(); // The addition AND deletion should've been batched together
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_batch");

    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn document_deletion_and_document_addition() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);
    index_scheduler
        .register(
            KindWithContent::DocumentDeletion {
                index_uid: S("doggos"),
                documents_ids: vec![S("1"), S("2")],
            },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");

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
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_second_task");

    // The deletion should have failed because it can't create an index
    handle.advance_one_failed_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_failing_the_deletion");

    // The addition should works
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_last_successful_addition");

    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_replace() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler));

    // everything should be batched together.
    handle.advance_n_successful_batches(1);
    snapshot!(snapshot_index_scheduler(&index_scheduler));

    // has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_update() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        file.persist().unwrap();
        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: UpdateDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler));

    // everything should be batched together.
    handle.advance_n_successful_batches(1);
    snapshot!(snapshot_index_scheduler(&index_scheduler));

    // has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_mixed_document_addition() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for i in 0..10 {
        let method = if i % 2 == 0 { UpdateDocuments } else { ReplaceDocuments };

        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        file.persist().unwrap();
        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // All tasks should've been batched and processed together since any indexing task (updates with replacements) can be batched together
    handle.advance_n_successful_batches(1);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

    // has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_replace_without_autobatching() {
    let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // Nothing should be batched thus half of the tasks are processed.
    handle.advance_n_successful_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

    // Everything is processed.
    handle.advance_n_successful_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

    // has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_update_without_autobatching() {
    let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        file.persist().unwrap();
        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S("id")),
                    method: UpdateDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // Nothing should be batched thus half of the tasks are processed.
    handle.advance_n_successful_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

    // Everything is processed.
    handle.advance_n_successful_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

    // has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_cant_create_index_without_index() {
    // We're going to autobatch multiple document addition that don't have
    // the right to create an index while there is no index currently.
    // Thus, everything should be batched together and a IndexDoesNotExists
    // error should be throwed.
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
                    allow_index_creation: false,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // Everything should be batched together.
    handle.advance_till([
        Start,
        BatchCreated,
        InsideProcessBatch,
        ProcessBatchFailed,
        AfterProcessing,
    ]);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_10_tasks");

    // The index should not exist.
    snapshot!(matches!(index_scheduler.index_exists("doggos"), Ok(true)), @"false");
}

#[test]
fn test_document_addition_cant_create_index_without_index_without_autobatching() {
    // We're going to execute multiple document addition that don't have
    // the right to create an index while there is no index currently.
    // Since the auto-batching is disabled, every task should be processed
    // sequentially and throw an IndexDoesNotExists.
    let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
                    allow_index_creation: false,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // Nothing should be batched thus half of the tasks are processed.
    handle.advance_n_failed_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

    // Everything is processed.
    handle.advance_n_failed_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

    // The index should not exist.
    snapshot!(matches!(index_scheduler.index_exists("doggos"), Ok(true)), @"false");
}

#[test]
fn test_document_addition_cant_create_index_with_index() {
    // We're going to autobatch multiple document addition that don't have
    // the right to create an index while there is already an index.
    // Thus, everything should be batched together and no error should be
    // throwed.
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    // Create the index.
    index_scheduler
        .register(
            KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
                    allow_index_creation: false,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // Everything should be batched together.
    handle.advance_n_successful_batches(1);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_processing_the_10_tasks");

    // Has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_cant_create_index_with_index_without_autobatching() {
    // We're going to execute multiple document addition that don't have
    // the right to create an index while there is no index currently.
    // Since the autobatching is disabled, every tasks should be processed
    // sequentially and throw an IndexDoesNotExists.
    let (index_scheduler, mut handle) = IndexScheduler::test(false, vec![]);

    // Create the index.
    index_scheduler
        .register(
            KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
                    allow_index_creation: false,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // Nothing should be batched thus half of the tasks are processed.
    handle.advance_n_successful_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "five_tasks_processed");

    // Everything is processed.
    handle.advance_n_successful_batches(5);
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

    // Has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_mixed_rights_with_index() {
    // We're going to autobatch multiple document addition.
    // - The index already exists
    // - The first document addition don't have the right to create an index
    //   can it batch with the other one?
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    // Create the index.
    index_scheduler
        .register(
            KindWithContent::IndexCreation { index_uid: S("doggos"), primary_key: None },
            None,
            false,
        )
        .unwrap();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "registered_the_first_task");
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "processed_the_first_task");

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );
        let allow_index_creation = i % 2 != 0;

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
                    allow_index_creation,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // Everything should be batched together.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

    // Has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_mixed_right_without_index_starts_with_cant_create() {
    // We're going to autobatch multiple document addition.
    // - The index does not exists
    // - The first document addition don't have the right to create an index
    // - The second do. They should not batch together.
    // - The second should batch with everything else as it's going to create an index.
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for i in 0..10 {
        let content = format!(
            r#"{{
                    "id": {},
                    "doggo": "bob {}"
                }}"#,
            i, i
        );
        let allow_index_creation = i % 2 != 0;

        let (uuid, mut file) = index_scheduler.queue.create_update_file_with_uuid(i).unwrap();
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
                    allow_index_creation,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_10_tasks");

    // A first batch should be processed with only the first documentAddition that's going to fail.
    handle.advance_one_failed_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_failed");

    // Everything else should be batched together.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_tasks_processed");

    // Has everything being pushed successfully in milli?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_with_multiple_primary_key() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for (id, primary_key) in ["id", "bork", "bloup"].iter().enumerate() {
        let content = format!(
            r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
        );
        let (uuid, mut file) =
            index_scheduler.queue.create_update_file_with_uuid(id as u128).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        assert_eq!(documents_count, 1);
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S(primary_key)),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_3_tasks");

    // A first batch should be processed with only the first documentAddition.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_succeed");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_fails");

    // Is the primary key still what we expect?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
    snapshot!(primary_key, @"id");

    // Is the document still the one we expect?.
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_with_multiple_primary_key_batch_wrong_key() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for (id, primary_key) in ["id", "bork", "bork"].iter().enumerate() {
        let content = format!(
            r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
        );
        let (uuid, mut file) =
            index_scheduler.queue.create_update_file_with_uuid(id as u128).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        assert_eq!(documents_count, 1);
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S(primary_key)),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_3_tasks");

    // A first batch should be processed with only the first documentAddition.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "only_first_task_succeed");

    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_and_third_tasks_fails");

    // Is the primary key still what we expect?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
    snapshot!(primary_key, @"id");

    // Is the document still the one we expect?.
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_with_bad_primary_key() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for (id, primary_key) in ["bork", "bork", "id", "bork", "id"].iter().enumerate() {
        let content = format!(
            r#"{{
                    "id": {id},
                    "doggo": "jean bob"
                }}"#,
        );
        let (uuid, mut file) =
            index_scheduler.queue.create_update_file_with_uuid(id as u128).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        assert_eq!(documents_count, 1);
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: Some(S(primary_key)),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_5_tasks");

    // A first batch should be processed with only the first two documentAddition.
    // it should fails because the documents don't contains any `bork` field.
    // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_and_second_task_fails");

    // The primary key should be set to none since we failed the batch.
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap();
    snapshot!(primary_key.is_none(), @"true");

    // The second batch should succeed and only contains one task.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

    // The primary key should be set to `id` since this batch succeeded.
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
    snapshot!(primary_key, @"id");

    // We're trying to `bork` again, but now there is already a primary key set for this index.
    // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fourth_task_fails");

    // Finally the last task should succeed since its primary key is the same as the valid one.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "fifth_task_succeeds");

    // Is the primary key still what we expect?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
    snapshot!(primary_key, @"id");

    // Is the document still the one we expect?.
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_with_set_and_null_primary_key() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for (id, primary_key) in
        [None, Some("bork"), Some("paw"), None, None, Some("paw")].into_iter().enumerate()
    {
        let content = format!(
            r#"{{
                    "paw": {id},
                    "doggo": "jean bob"
                }}"#,
        );
        let (uuid, mut file) =
            index_scheduler.queue.create_update_file_with_uuid(id as u128).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        assert_eq!(documents_count, 1);
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: primary_key.map(|pk| pk.to_string()),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_6_tasks");

    // A first batch should contains only one task that fails because we can't infer the primary key.
    // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_fails");

    // The second batch should contains only one task that fails because we bork is not a valid primary key.
    // NOTE: it's marked as successful because the batch didn't fails, it's the individual tasks that failed.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

    // No primary key should be set at this point.
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap();
    snapshot!(primary_key.is_none(), @"true");

    // The third batch should succeed and only contains one task.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

    // The primary key should be set to `id` since this batch succeeded.
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
    snapshot!(primary_key, @"paw");

    // We should be able to batch together the next two tasks that don't specify any primary key
    // + the last task that matches the current primary-key. Everything should succeed.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_other_tasks_succeeds");

    // Is the primary key still what we expect?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
    snapshot!(primary_key, @"paw");

    // Is the document still the one we expect?.
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}

#[test]
fn test_document_addition_with_set_and_null_primary_key_inference_works() {
    let (index_scheduler, mut handle) = IndexScheduler::test(true, vec![]);

    for (id, primary_key) in
        [None, Some("bork"), Some("doggoid"), None, None, Some("doggoid")].into_iter().enumerate()
    {
        let content = format!(
            r#"{{
                    "doggoid": {id},
                    "doggo": "jean bob"
                }}"#,
        );
        let (uuid, mut file) =
            index_scheduler.queue.create_update_file_with_uuid(id as u128).unwrap();
        let documents_count = read_json(content.as_bytes(), &mut file).unwrap();
        assert_eq!(documents_count, 1);
        file.persist().unwrap();

        index_scheduler
            .register(
                KindWithContent::DocumentAdditionOrUpdate {
                    index_uid: S("doggos"),
                    primary_key: primary_key.map(|pk| pk.to_string()),
                    method: ReplaceDocuments,
                    content_file: uuid,
                    documents_count,
                    allow_index_creation: true,
                },
                None,
                false,
            )
            .unwrap();
        index_scheduler.assert_internally_consistent();
    }

    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "after_registering_the_6_tasks");

    // A first batch should contains only one task that succeed and sets the primary key to `doggoid`.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "first_task_succeed");

    // Checking the primary key.
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap();
    snapshot!(primary_key.is_none(), @"false");

    // The second batch should contains only one task that fails because it tries to update the primary key to `bork`.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "second_task_fails");

    // The third batch should succeed and only contains one task.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "third_task_succeeds");

    // We should be able to batch together the next two tasks that don't specify any primary key
    // + the last task that matches the current primary-key. Everything should succeed.
    handle.advance_one_successful_batch();
    snapshot!(snapshot_index_scheduler(&index_scheduler), name: "all_other_tasks_succeeds");

    // Is the primary key still what we expect?
    let index = index_scheduler.index("doggos").unwrap();
    let rtxn = index.read_txn().unwrap();
    let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
    snapshot!(primary_key, @"doggoid");

    // Is the document still the one we expect?.
    let field_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let field_ids = field_ids_map.ids().collect::<Vec<_>>();
    let documents = index
        .all_documents(&rtxn)
        .unwrap()
        .map(|ret| obkv_to_json(&field_ids, &field_ids_map, ret.unwrap().1).unwrap())
        .collect::<Vec<_>>();
    snapshot!(serde_json::to_string_pretty(&documents).unwrap(), name: "documents");
}
