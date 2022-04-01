use milli::{
    update::{IndexerConfig, Settings},
    Criterion, Search,
};
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
