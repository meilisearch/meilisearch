#[macro_use] extern crate maplit;

mod common;

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;

use big_s::S;
use serde_json::json;

#[test]
fn database_stats() {
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
    let stats = index.stats().unwrap();
    let repartition = hashmap!{
        S("objectId") => 1u64,
        S("title") => 1u64,
    };
    assert_eq!(stats.number_of_documents, 1);
    assert_eq!(stats.documents_fields_repartition, repartition);

    let doc2 = json!({ "objectId": 456, "title": "world" });

    let mut addition = index.documents_addition();
    addition.update_document(&doc2);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());
    let stats = index.stats().unwrap();
    let repartition = hashmap!{
        S("objectId") => 2u64,
        S("title") => 2u64,
    };
    assert_eq!(stats.number_of_documents, 2);
    assert_eq!(stats.documents_fields_repartition, repartition);


    let doc3 = json!({ "objectId": 789 });

    let mut addition = index.documents_addition();
    addition.update_document(&doc3);
    let update_id = addition.finalize().unwrap();
    let status = index.update_status_blocking(update_id).unwrap();
    assert!(as_been_updated.swap(false, Relaxed));
    assert!(status.result.is_ok());
    let stats = index.stats().unwrap();
    let repartition = hashmap!{
        S("objectId") => 3u64,
        S("title") => 2u64,
    };
    assert_eq!(stats.number_of_documents, 3);
    assert_eq!(stats.documents_fields_repartition, repartition);
}
