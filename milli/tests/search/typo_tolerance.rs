use std::collections::BTreeSet;

use heed::EnvOpenOptions;
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use milli::{Criterion, Index, Search};
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
        search.optional_words(true);

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);

        let mut search = Search::new(&txn, &index);
        search.query("zean");
        search.limit(10);
        search.authorize_typos(true);
        search.optional_words(true);

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 0);
    }

    let mut txn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut txn, &index, &config);
    builder.set_min_word_len_one_typo(4);
    builder.execute(|_| ()).unwrap();

    // typo is now supported for 4 letters words
    let mut search = Search::new(&txn, &index);
    search.query("zean");
    search.limit(10);
    search.authorize_typos(true);
    search.optional_words(true);

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
        search.optional_words(true);

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 1);

        let mut search = Search::new(&txn, &index);
        search.query("zealemd");
        search.limit(10);
        search.authorize_typos(true);
        search.optional_words(true);

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 0);
    }

    let mut txn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut txn, &index, &config);
    builder.set_min_word_len_two_typos(7);
    builder.execute(|_| ()).unwrap();

    // typo is now supported for 4 letters words
    let mut search = Search::new(&txn, &index);
    search.query("zealemd");
    search.limit(10);
    search.authorize_typos(true);
    search.optional_words(true);

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
}

#[test]
fn test_typo_disabled_on_word() {
    let tmp = tempdir().unwrap();
    let mut options = EnvOpenOptions::new();
    options.map_size(4096 * 100);
    let index = Index::new(options, tmp.path()).unwrap();

    let documents = json!([
        {
            "id": 1usize,
            "data": "zealand",
        },
        {
            "id": 2usize,
            "data": "zearand",
        },
    ]);

    let mut writer = std::io::Cursor::new(Vec::new());
    let mut builder = milli::documents::DocumentBatchBuilder::new(&mut writer).unwrap();
    let documents = serde_json::to_vec(&documents).unwrap();
    builder.extend_from_json(std::io::Cursor::new(documents)).unwrap();
    builder.finish().unwrap();

    writer.set_position(0);

    let documents = milli::documents::DocumentBatchReader::from_reader(writer).unwrap();

    let mut txn = index.write_txn().unwrap();
    let config = IndexerConfig::default();
    let indexing_config = IndexDocumentsConfig::default();
    let mut builder = IndexDocuments::new(&mut txn, &index, &config, indexing_config, |_| ());

    builder.add_documents(documents).unwrap();

    builder.execute().unwrap();
    txn.commit().unwrap();

    // basic typo search with default typo settings
    {
        let txn = index.read_txn().unwrap();

        let mut search = Search::new(&txn, &index);
        search.query("zealand");
        search.limit(10);
        search.authorize_typos(true);
        search.optional_words(true);

        let result = search.execute().unwrap();
        assert_eq!(result.documents_ids.len(), 2);
    }

    let mut txn = index.write_txn().unwrap();

    let config = IndexerConfig::default();
    let mut builder = Settings::new(&mut txn, &index, &config);
    let mut exact_words = BTreeSet::new();
    // sealand doesn't allow typos anymore
    exact_words.insert("zealand".to_string());
    builder.set_exact_words(exact_words);
    builder.execute(|_| ()).unwrap();

    let mut search = Search::new(&txn, &index);
    search.query("zealand");
    search.limit(10);
    search.authorize_typos(true);
    search.optional_words(true);

    let result = search.execute().unwrap();
    assert_eq!(result.documents_ids.len(), 1);
}
