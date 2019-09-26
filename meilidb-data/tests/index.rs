mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use serde_json::json;

#[test]
fn insert_delete_document() {
    let index = common::simple_index();
    let as_been_updated = Arc::new(AtomicBool::new(false));

    let as_been_updated_clone = as_been_updated.clone();
    index.set_update_callback(move |_| as_been_updated_clone.store(true, Relaxed));

    let doc1 = json!({ "objectId": 123, "title": "hello" });

    let mut addition = index.documents_addition();
    addition.update_document(&doc1);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());
    assert_eq!(index.number_of_documents(), 1);

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(index.document(None, docs[0].id).unwrap().as_ref(), Some(&doc1));

    let mut deletion = index.documents_deletion();
    deletion.delete_document(&doc1).unwrap();
    let update_id = deletion.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());
    assert_eq!(index.number_of_documents(), 0);

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 0);
}

#[test]
fn replace_document() {
    let index = common::simple_index();
    let as_been_updated = Arc::new(AtomicBool::new(false));

    let as_been_updated_clone = as_been_updated.clone();
    index.set_update_callback(move |_| as_been_updated_clone.store(true, Relaxed));

    let doc1 = json!({ "objectId": 123, "title": "hello" });
    let doc2 = json!({ "objectId": 123, "title": "coucou" });

    let mut addition = index.documents_addition();
    addition.update_document(&doc1);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());
    assert_eq!(index.number_of_documents(), 1);

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(index.document(None, docs[0].id).unwrap().as_ref(), Some(&doc1));

    let mut addition = index.documents_addition();
    addition.update_document(&doc2);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());
    assert_eq!(index.number_of_documents(), 1);

    let docs = index.query_builder().query("hello", 0..10).unwrap();
    assert_eq!(docs.len(), 0);

    let docs = index.query_builder().query("coucou", 0..10).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(index.document(None, docs[0].id).unwrap().as_ref(), Some(&doc2));
}

#[test]
fn documents_ids() {
    let index = common::simple_index();

    let doc1 = json!({ "objectId": 123, "title": "hello" });
    let doc2 = json!({ "objectId": 456, "title": "world" });
    let doc3 = json!({ "objectId": 789 });

    let mut addition = index.documents_addition();
    addition.update_document(&doc1);
    addition.update_document(&doc2);
    addition.update_document(&doc3);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(status.result.is_ok());

    let documents_ids_count = index.documents_ids().unwrap().count();
    assert_eq!(documents_ids_count, 3);
}

#[test]
fn current_update_id() {
    let index = common::simple_index();
    let update_id = Arc::new(AtomicU64::new(0));

    let update_id_cloned = update_id.clone();
    let index_cloned = index.clone();
    index.set_update_callback(move |_| {
        let current_update_id = index_cloned.current_update_id().unwrap().unwrap();
        assert_eq!(current_update_id, update_id_cloned.load(Relaxed));
    });

    let doc1 = json!({ "objectId": 123, "title": "hello" });
    let mut addition = index.documents_addition();
    addition.update_document(&doc1);
    update_id.store(addition.finalize().unwrap(), Relaxed);
}

#[test]
fn nest_updates_in_queue() {
    let index = common::simple_index();

    index.set_update_callback(move |_| {
        std::thread::sleep(std::time::Duration::from_secs(15));
    });

    let doc1 = json!({ "objectId": 123, "title": "hello" });
    let doc2 = json!({ "objectId": 456, "title": "world" });
    let doc3 = json!({ "objectId": 789 });

    let mut addition = index.documents_addition();
    addition.update_document(&doc1);
    let _ = addition.finalize().unwrap();

    let mut addition = index.documents_addition();
    addition.update_document(&doc2);
    let _ = addition.finalize().unwrap();

    let mut addition = index.documents_addition();
    addition.update_document(&doc3);
    let _ = addition.finalize().unwrap();

    let should_have_in_queue_updates = vec![1, 2, 3];

    let in_queue_updates = index.enqueued_updates_ids().unwrap();
    assert_eq!(in_queue_updates, should_have_in_queue_updates);

}
