/*!
This module tests the following properties about stop words:
- they are not indexed
- they are not searchable
- they are case sensitive
- they are ignored in phrases
- If a query consists only of stop words, a placeholder query is used instead
- A prefix word is never ignored, even if the prefix is a stop word
- Phrases consisting only of stop words are ignored
*/

use std::{collections::BTreeSet, iter::FromIterator};

use crate::{db_snap, index::tests::TempIndex, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["title".to_owned()]);
            s.set_stop_words(BTreeSet::from_iter([
                "to".to_owned(),
                "The".to_owned(),
                "xyz".to_owned(),
            ]));
        })
        .unwrap();

    index
        .add_documents(documents!([
        {
            "id": 0,
            "title": "Shazam!",
        },
        {
            "id": 1,
            "title": "Captain Marvel",
        },
        {
            "id": 2,
            "title": "Escape Room",
        },
        {
            "id": 3,
            "title": "How to Train Your Dragon: The Hidden World",
        },
        {
            "id": 4,
            "title": "Gläss",
        },
        {
            "id": 5,
            "title": "How to Attempt to Train Your Dragon",
        },
        {
            "id": 6,
            "title": "How to Train Your Dragon: the Hidden World",
        },
        ]))
        .unwrap();
    index
}

#[test]
fn test_stop_words_not_indexed() {
    let index = create_index();
    db_snap!(index, word_docids, @"6288f9d7db3703b02c57025eb4a69264");
}

#[test]
fn test_ignore_stop_words() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    // `the` is treated as a prefix here, so it's not ignored
    let mut s = Search::new(&txn, &index);
    s.query("xyz to the");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");

    // `xyz` is treated as a prefix here, so it's not ignored
    let mut s = Search::new(&txn, &index);
    s.query("to the xyz");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");

    // `xyz` is not treated as a prefix anymore because of the trailing space, so it's ignored
    let mut s = Search::new(&txn, &index);
    s.query("to the xyz ");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");

    let mut s = Search::new(&txn, &index);
    s.query("to the dragon xyz");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
}

#[test]
fn test_stop_words_in_phrase() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.query("\"how to train your dragon\"");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 6]");

    let mut s = Search::new(&txn, &index);
    s.query("how \"to\" train \"the");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");

    let mut s = Search::new(&txn, &index);
    s.query("how \"to\" train \"The dragon");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 6, 5]");

    let mut s = Search::new(&txn, &index);
    s.query("\"to\"");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3, 4, 5, 6]");
}
