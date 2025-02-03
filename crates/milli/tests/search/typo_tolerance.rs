use std::collections::BTreeSet;

use bumpalo::Bump;
use heed::EnvOpenOptions;
use milli::documents::mmap_from_objects;
use milli::progress::Progress;
use milli::update::new::indexer;
use milli::update::{IndexerConfig, Settings};
use milli::vector::EmbeddingConfigs;
use milli::{Criterion, Index, Object, Search, TermsMatchingStrategy};
use serde_json::from_value;
use tempfile::tempdir;
use ureq::json;
use Criterion::*;

#[test]
fn test_typo_tolerance_one_typo() {
    let criteria = [Typo];
    let index = super::setup_search_index_with_criteria(&criteria);

    // basic typo search with default typo settings
    {
        let txn = index.read_txn().unwrap();

        let mut search = Search::new(&txn, &index);
        search.query("zeal");
        search.limit(10);

        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);

        let mut search = Search::new(&txn, &index);
        search.query("zean");
        search.limit(10);

        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 0);
    }

    let mut txn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut txn, &index, &config);
    builder.set_min_word_len_one_typo(4);
    builder.execute(|_| (), || false).unwrap();

    // typo is now supported for 4 letters words
    let mut search = Search::new(&txn, &index);
    search.query("zean");
    search.limit(10);

    search.terms_matching_strategy(TermsMatchingStrategy::default());

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
}

#[test]
fn test_typo_tolerance_two_typo() {
    let criteria = [Typo];
    let index = super::setup_search_index_with_criteria(&criteria);

    // basic typo search with default typo settings
    {
        let txn = index.read_txn().unwrap();

        let mut search = Search::new(&txn, &index);
        search.query("zealand");
        search.limit(10);

        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);

        let mut search = Search::new(&txn, &index);
        search.query("zealemd");
        search.limit(10);

        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 0);
    }

    let mut txn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut txn, &index, &config);
    builder.set_min_word_len_two_typos(7);
    builder.execute(|_| (), || false).unwrap();

    // typo is now supported for 4 letters words
    let mut search = Search::new(&txn, &index);
    search.query("zealemd");
    search.limit(10);

    search.terms_matching_strategy(TermsMatchingStrategy::default());

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
}

#[test]
fn test_typo_disabled_on_word() {
    let tmp = tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(4096 * 100);
    let index = Index::new(options, tmp.path(), true).unwrap();

    let doc1: Object = from_value(json!({ "id": 1usize, "data": "zealand" })).unwrap();
    let doc2: Object = from_value(json!({ "id": 2usize, "data": "zearand" })).unwrap();
    let documents = mmap_from_objects(vec![doc1, doc2]);

    let mut wtxn = index.write_txn().unwrap();
    let rtxn = index.read_txn().unwrap();
    let config = IndexerConfig::default();

    let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
    let mut new_fields_ids_map = db_fields_ids_map.clone();
    let embedders = EmbeddingConfigs::default();
    let mut indexer = indexer::DocumentOperation::new();

    indexer.replace_documents(&documents).unwrap();

    let indexer_alloc = Bump::new();
    let (document_changes, _operation_stats, primary_key) = indexer
        .into_changes(
            &indexer_alloc,
            &index,
            &rtxn,
            None,
            &mut new_fields_ids_map,
            &|| false,
            Progress::default(),
        )
        .unwrap();

    indexer::index(
        &mut wtxn,
        &index,
        &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
        config.grenad_parameters(),
        &db_fields_ids_map,
        new_fields_ids_map,
        primary_key,
        &document_changes,
        embedders,
        &|| false,
        &Progress::default(),
    )
    .unwrap();

    wtxn.commit().unwrap();

    // basic typo search with default typo settings
    {
        let txn = index.read_txn().unwrap();

        let mut search = Search::new(&txn, &index);
        search.query("zealand");
        search.limit(10);

        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);
    }

    let mut txn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut txn, &index, &config);
    let mut exact_words = BTreeSet::new();
    // `zealand` doesn't allow typos anymore
    exact_words.insert("zealand".to_string());
    builder.set_exact_words(exact_words);
    builder.execute(|_| (), || false).unwrap();

    let mut search = Search::new(&txn, &index);
    search.query("zealand");
    search.limit(10);

    search.terms_matching_strategy(TermsMatchingStrategy::default());

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
}

#[test]
fn test_disable_typo_on_attribute() {
    let criteria = [Typo];
    let index = super::setup_search_index_with_criteria(&criteria);

    // basic typo search with default typo settings
    {
        let txn = index.read_txn().unwrap();

        let mut search = Search::new(&txn, &index);
        // typo in `antebel(l)um`
        search.query("antebelum");
        search.limit(10);

        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);
    }

    let mut txn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut txn, &index, &config);
    // disable typos on `description`
    builder.set_exact_attributes(vec!["description".to_string()].into_iter().collect());
    builder.execute(|_| (), || false).unwrap();

    let mut search = Search::new(&txn, &index);
    search.query("antebelum");
    search.limit(10);

    search.terms_matching_strategy(TermsMatchingStrategy::default());

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 0);
}
