use std::collections::BTreeSet;

use heed::EnvOpenOptions;
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use milli::{Criterion, Index, Search, TermsMatchingStrategy};
use serde_json::json;
use tempfile::tempdir;
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
        search.authorize_typos(true);
        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);

        let mut search = Search::new(&txn, &index);
        search.query("zean");
        search.limit(10);
        search.authorize_typos(true);
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
    search.authorize_typos(true);
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
        search.authorize_typos(true);
        search.terms_matching_strategy(TermsMatchingStrategy::default());

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);

        let mut search = Search::new(&txn, &index);
        search.query("zealemd");
        search.limit(10);
        search.authorize_typos(true);
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
    search.authorize_typos(true);
    search.terms_matching_strategy(TermsMatchingStrategy::default());

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
}

#[test]
fn test_typo_disabled_on_word() {
    let tmp = tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(4096 * 100);
    let index = Index::new(options, tmp.path()).unwrap();

    let mut builder = milli::documents::DocumentsBatchBuilder::new(Vec::new());
    let doc1 = json!({
        "id": 1usize,
        "data": "zealand",
    });

    let doc2 = json!({
        "id": 2usize,
        "data": "zearand",
    });

    builder.append_json_object(doc1.as_object().unwrap()).unwrap();
    builder.append_json_object(doc2.as_object().unwrap()).unwrap();
    let vector = builder.into_inner().unwrap();

    let documents =
        milli::documents::DocumentsBatchReader::from_reader(std::io::Cursor::new(vector)).unwrap();

    let mut txn = index.write_txn().unwrap();
    let config = IndexerConfig::default();
    let indexing_config = IndexDocumentsConfig::default();
    let builder =
        IndexDocuments::new(&mut txn, &index, &config, indexing_config, |_| (), || false).unwrap();

    let (builder, user_error) = builder.add_documents(documents).unwrap();
    user_error.unwrap();
    builder.execute().unwrap();
    txn.commit().unwrap();

    // basic typo search with default typo settings
    {
        let txn = index.read_txn().unwrap();

        let mut search = Search::new(&txn, &index);
        search.query("zealand");
        search.limit(10);
        search.authorize_typos(true);
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
    search.authorize_typos(true);
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
        search.authorize_typos(true);
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
    search.authorize_typos(true);
    search.terms_matching_strategy(TermsMatchingStrategy::default());

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 0);
}
