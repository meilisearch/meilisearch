use milli::update::{IndexerConfig, Settings};
use milli::{Index, Search, TermsMatchingStrategy};

fn set_stop_words(index: &Index, stop_words: &[&str]) {
    let mut wtxn = index.write_txn().unwrap();
    let config = IndexerConfig::default();

    let mut builder = Settings::new(&mut wtxn, &index, &config);
    let stop_words = stop_words.into_iter().map(|s| s.to_string()).collect();
    builder.set_stop_words(stop_words);
    builder.execute(|_| ()).unwrap();
    wtxn.commit().unwrap();
}

#[test]
fn test_phrase_search_with_stop_words() {
    let criteria = [];
    let index = super::setup_search_index_with_criteria(&criteria);

    // Add stop_words
    set_stop_words(&index, &["a", "an", "the", "of"]);

    // Phrase search containing stop words
    let txn = index.read_txn().unwrap();

    let mut search = Search::new(&txn, &index);
    search.query("\"the use of force\"");
    search.limit(10);
    search.authorize_typos(false);
    search.terms_matching_strategy(TermsMatchingStrategy::All);

    let result = search.execute().unwrap();
    // 1 document should match
    assert_eq!(result.documents_ids.len(), 1);
}
