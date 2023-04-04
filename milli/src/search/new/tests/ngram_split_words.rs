/*!
This module tests the following properties:

1. Two consecutive words from a query can be combined into a "2gram"
2. Three consecutive words from a query can be combined into a "3gram"
3. A word from the query can be split into two consecutive words (split words)
4. A 2gram can be split into two words
5. A 3gram cannot be split into two words
6. 2grams can contain up to 1 typo
7. 3grams cannot have typos
8. 2grams and 3grams can be prefix tolerant
9. Disabling typo tolerance also disable the split words feature
10. Disabling typo tolerance does not disable prefix tolerance
11. Disabling typo tolerance does not disable ngram tolerance
12. Prefix tolerance is disabled for the last word if a space follows it
13. Ngrams cannot be formed by combining a phrase and a word or two phrases
*/

use crate::{index::tests::TempIndex, Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Words]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "the sun flowers are pretty"
            },
            {
                "id": 1,
                "text": "the sun flower is tall"
            },
            {
                "id": 2,
                "text": "the sunflowers are pretty"
            },
            {
                "id": 3,
                "text": "the sunflower is tall"
            }
        ]))
        .unwrap();
    index
}

#[test]
fn test_2gram_simple() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_autorize_typos(false);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sun flower");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    // will also match documents with "sun flower"
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3]");
}
#[test]
fn test_3gram_simple() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_autorize_typos(false);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sun flower s are");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 2]");
}

#[test]
fn test_2gram_typo() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sun flawer");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3]");
}

#[test]
fn test_no_disable_ngrams() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_autorize_typos(false);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sun flower ");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    // documents containing `sunflower`
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 3]");
}

#[test]
fn test_2gram_prefix() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_autorize_typos(false);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sun flow");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    // documents containing words beginning with `sunflow`
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3]");
}

#[test]
fn test_3gram_prefix() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_autorize_typos(false);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("su nf l");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // documents containing a word beginning with sunfl
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 3]");
}

#[test]
fn test_split_words() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunflower ");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // all the documents with either `sunflower` or `sun flower`
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 2, 3]");
}

#[test]
fn test_disable_split_words() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_autorize_typos(false);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunflower ");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    // no document containing `sun flower`
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3]");
}

#[test]
fn test_2gram_split_words() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunf lower");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // all the documents with "sunflower", "sun flower", or (sunflower + 1 typo)
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 2, 3]");
}

#[test]
fn test_3gram_no_split_words() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunf lo wer");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // no document with `sun flower`
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 3]");
}

#[test]
fn test_3gram_no_typos() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunf la wer");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[]");
}

#[test]
fn test_no_ngram_phrases() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("\"sun\" flower");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1]");

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("\"sun\" \"flower\"");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1]");
}
