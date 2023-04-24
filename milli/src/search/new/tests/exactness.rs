/*!
This module tests the following properties about the exactness ranking rule:

- it sorts documents as follows:
    1. documents which have an attribute which is equal to the whole query
    2. documents which have an attribute which start with the whole query
    3. documents which contain the most exact words from the query

- the set of all candidates when `exactness` precedes `word` is the union of:
    1. the same set of candidates that would be returned normally
    2. the set of documents that contain at least one exact word from the query

- if it is placed after `word`, then it will only sort documents by:
    1. those that have an attribute which is equal to the whole remaining query, if this query does not have any "gap"
    2. those that have an attribute which start with the whole remaining query, if this query does not have any "gap"
    3. those that contain the most exact words from the remaining query
*/

use crate::{
    index::tests::TempIndex, search::new::tests::collect_field_values, Criterion, Search,
    SearchResult, TermsMatchingStrategy,
};

fn create_index_exact_words_simple_ordered() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Exactness]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "",
            },
            {
                "id": 1,
                "text": "the",
            },
            {
                "id": 2,
                "text": "the quick",
            },
            {
                "id": 3,
                "text": "the quick brown",
            },
            {
                "id": 4,
                "text": "the quick brown fox",
            },
            {
                "id": 5,
                "text": "the quick brown fox jumps",
            },

            {
                "id": 6,
                "text": "the quick brown fox jumps over",
            },
            {
                "id": 7,
                "text": "the quick brown fox jumps over the",
            },
            {
                "id": 8,
                "text": "the quick brown fox jumps over the lazy",
            },
            {
                "id": 9,
                "text": "the quick brown fox jumps over the lazy dog",
            },
        ]))
        .unwrap();
    index
}

fn create_index_exact_words_simple_reversed() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Exactness]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "",
            },
            {
                "id": 1,
                "text": "dog",
            },
            {
                "id": 2,
                "text": "lazy dog",
            },
            {
                "id": 3,
                "text": "the lazy dog",
            },
            {
                "id": 4,
                "text": "over the lazy dog",
            },
            {
                "id": 5,
                "text": "jumps over the lazy dog",
            },
            {
                "id": 6,
                "text": "fox jumps over the lazy dog",
            },
            {
                "id": 7,
                "text": "brown fox jumps over the lazy dog",
            },
            {
                "id": 8,
                "text": "quick brown fox jumps over the lazy dog",
            },
            {
                "id": 9,
                "text": "the quick brown fox jumps over the lazy dog",
            }
        ]))
        .unwrap();
    index
}

fn create_index_exact_words_simple_random() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Exactness]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "",
            },
            {
                "id": 1,
                "text": "over",
            },
            {
                "id": 2,
                "text": "jump dog",
            },
            {
                "id": 3,
                "text": "brown the lazy",
            },
            {
                "id": 4,
                "text": "jump dog quick the",
            },
            {
                "id": 5,
                "text": "fox the lazy dog brown",
            },
            {
                "id": 6,
                "text": "jump fox quick lazy the dog",
            },
            {
                "id": 7,
                "text": "the dog brown over jumps quick lazy",
            },
            {
                "id": 8,
                "text": "the jumps dog quick over brown lazy fox",
            }
        ]))
        .unwrap();
    index
}

fn create_index_attribute_starts_with() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Exactness]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "what a lovely view from this balcony, I love it",
            },
            {
                "id": 1,
                "text": "this balcony is overlooking the sea",
            },
            {
                "id": 2,
                "text": "this balcony",
            },
            {
                "id": 3,
                "text": "over looking the sea is a beautiful balcony",
            },
            {
                "id": 4,
                "text": "a beautiful balcony is overlooking the sea",
            },
            {
                "id": 5,
                "text": "overlooking the sea is a beautiful balcony, I love it",
            },
            {
                "id": 6,
                "text": "overlooking the sea is a beautiful balcony",
            },
            {
                "id": 7,
                "text": "overlooking",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_exactness_simple_ordered() {
    let index = create_index_exact_words_simple_ordered();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[9, 8, 6, 7, 5, 4, 3, 2, 1]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over\"",
        "\"the quick brown fox jumps over the\"",
        "\"the quick brown fox jumps\"",
        "\"the quick brown fox\"",
        "\"the quick brown\"",
        "\"the quick\"",
        "\"the\"",
    ]
    "###);
}

#[test]
fn test_exactness_simple_reversed() {
    let index = create_index_exact_words_simple_reversed();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[9, 8, 7, 6, 5, 4, 3, 2, 1]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"quick brown fox jumps over the lazy dog\"",
        "\"brown fox jumps over the lazy dog\"",
        "\"fox jumps over the lazy dog\"",
        "\"jumps over the lazy dog\"",
        "\"over the lazy dog\"",
        "\"the lazy dog\"",
        "\"lazy dog\"",
        "\"dog\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[9, 8, 7, 6, 5, 4, 3, 2, 1]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"quick brown fox jumps over the lazy dog\"",
        "\"brown fox jumps over the lazy dog\"",
        "\"fox jumps over the lazy dog\"",
        "\"jumps over the lazy dog\"",
        "\"over the lazy dog\"",
        "\"the lazy dog\"",
        "\"lazy dog\"",
        "\"dog\"",
    ]
    "###);
}

#[test]
fn test_exactness_simple_random() {
    let index = create_index_exact_words_simple_random();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[8, 7, 5, 6, 3, 4, 1, 2]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the jumps dog quick over brown lazy fox\"",
        "\"the dog brown over jumps quick lazy\"",
        "\"fox the lazy dog brown\"",
        "\"jump fox quick lazy the dog\"",
        "\"brown the lazy\"",
        "\"jump dog quick the\"",
        "\"over\"",
        "\"jump dog\"",
    ]
    "###);
}

#[test]
fn test_exactness_attribute_starts_with_simple() {
    let index = create_index_attribute_starts_with();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("this balcony");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 1, 0, 3, 4, 5, 6]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"this balcony\"",
        "\"this balcony is overlooking the sea\"",
        "\"what a lovely view from this balcony, I love it\"",
        "\"over looking the sea is a beautiful balcony\"",
        "\"a beautiful balcony is overlooking the sea\"",
        "\"overlooking the sea is a beautiful balcony, I love it\"",
        "\"overlooking the sea is a beautiful balcony\"",
    ]
    "###);
}

#[test]
fn test_exactness_attribute_starts_with_phrase() {
    let index = create_index_attribute_starts_with();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("\"overlooking the sea\" is a beautiful balcony");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[5, 6, 4, 3, 1, 0, 2]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // TODO: this is incorrect, the first document returned here should actually be the second one
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"overlooking the sea is a beautiful balcony, I love it\"",
        "\"overlooking the sea is a beautiful balcony\"",
        "\"a beautiful balcony is overlooking the sea\"",
        "\"over looking the sea is a beautiful balcony\"",
        "\"this balcony is overlooking the sea\"",
        "\"what a lovely view from this balcony, I love it\"",
        "\"this balcony\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("overlooking the sea is a beautiful balcony");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6, 5, 4, 3, 1, 0, 2, 7]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // TODO: this is correct, so the exactness ranking rule probably has a bug in the handling of phrases
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"overlooking the sea is a beautiful balcony\"",
        "\"overlooking the sea is a beautiful balcony, I love it\"",
        "\"a beautiful balcony is overlooking the sea\"",
        "\"over looking the sea is a beautiful balcony\"",
        "\"this balcony is overlooking the sea\"",
        "\"what a lovely view from this balcony, I love it\"",
        "\"this balcony\"",
        "\"overlooking\"",
    ]
    "###);
}

#[test]
fn test_exactness_all_candidates_with_typo() {
    let index = create_index_attribute_starts_with();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("overlocking the sea is a beautiful balcony");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 4, 5, 6, 1, 0, 2, 7]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // "overlooking" is returned here because the term matching strategy allows it
    // but it has the worst exactness score (0 exact words)
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"over looking the sea is a beautiful balcony\"",
        "\"a beautiful balcony is overlooking the sea\"",
        "\"overlooking the sea is a beautiful balcony, I love it\"",
        "\"overlooking the sea is a beautiful balcony\"",
        "\"this balcony is overlooking the sea\"",
        "\"what a lovely view from this balcony, I love it\"",
        "\"this balcony\"",
        "\"overlooking\"",
    ]
    "###);
}
