#[macro_use] extern crate maplit;

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;

use big_s::S;
use serde_json::json;
use meilidb_data::{Database, RankingOrdering};
use meilidb_schema::{Schema, SchemaBuilder, DISPLAYED, INDEXED};

fn simple_schema() -> Schema {
    let mut builder = SchemaBuilder::with_identifier("objectId");
    builder.new_attribute("objectId", DISPLAYED | INDEXED);
    builder.new_attribute("title", DISPLAYED | INDEXED);
    builder.build()
}

#[test]
fn insert_delete_document() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::open(&tmp_dir).unwrap();

    let as_been_updated = Arc::new(AtomicBool::new(false));

    let schema = simple_schema();
    let index = database.create_index("hello", schema).unwrap();

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
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::open(&tmp_dir).unwrap();

    let as_been_updated = Arc::new(AtomicBool::new(false));

    let schema = simple_schema();
    let index = database.create_index("hello", schema).unwrap();

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
fn database_stats() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::open(&tmp_dir).unwrap();

    let as_been_updated = Arc::new(AtomicBool::new(false));

    let schema = simple_schema();
    let index = database.create_index("hello", schema).unwrap();

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

#[test]
fn custom_settings() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let database = Database::open(&tmp_dir).unwrap();

    let schema = simple_schema();
    let index = database.create_index("hello", schema).unwrap();

    let stop_words = hashset!{ S("le"), S("la"), S("les"), };
    let ranking_order = vec![S("SumOfTypos"), S("NumberOfWords"), S("WordsProximity"), S("SumOfWordsAttribute"), S("SumOfWordsPosition"), S("Exact"), S("DocumentId")];
    let distinct_field = S("title");
    let ranking_rules = hashmap!{ S("objectId") => RankingOrdering::Asc };

    index.custom_settings().set_stop_words(&stop_words).unwrap();
    index.custom_settings().set_ranking_order(&ranking_order).unwrap();
    index.custom_settings().set_distinct_field(&distinct_field).unwrap();
    index.custom_settings().set_ranking_rules(&ranking_rules).unwrap();

    let ret_stop_words = index.custom_settings().get_stop_words().unwrap().unwrap();
    let ret_ranking_orderer = index.custom_settings().get_ranking_order().unwrap().unwrap();
    let ret_distinct_field = index.custom_settings().get_distinct_field().unwrap().unwrap();
    let ret_ranking_rules = index.custom_settings().get_ranking_rules().unwrap().unwrap();

    assert_eq!(ret_stop_words, stop_words);
    assert_eq!(ret_ranking_orderer, ranking_order);
    assert_eq!(ret_distinct_field, distinct_field);
    assert_eq!(ret_ranking_rules, ranking_rules);
}
