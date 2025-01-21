/*!
This module tests the following properties about the exactness ranking rule:

- it sorts documents as follows:
    1. documents which have an attribute which is equal to the whole query
    2. documents which have an attribute which start with the whole query
    3. documents which contain the most exact words from the query

- the `exactness` ranking rule must be preceded by the `words` ranking rule

- if `words` has already removed terms from the query, then exactness will sort documents as follows:
    1. those that have an attribute which is equal to the whole remaining query, if this query does not have any "gap"
    2. those that have an attribute which start with the whole remaining query, if this query does not have any "gap"
    3. those that contain the most exact words from the remaining query

- if it is followed by other graph-based ranking rules (`typo`, `proximity`, `attribute`).
  Then these rules will only work with
    1. the exact terms selected by `exactness
    2. the full query term otherwise
*/

use crate::index::tests::TempIndex;
use crate::search::new::tests::collect_field_values;
use crate::{Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_index_simple_ordered() -> TempIndex {
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

fn create_index_simple_reversed() -> TempIndex {
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

fn create_index_simple_random() -> TempIndex {
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

fn create_index_simple_ordered_with_typos() -> TempIndex {
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
                "text": "the quack",
            },
            {
                "id": 3,
                "text": "the quack briwn",
            },
            {
                "id": 4,
                "text": "the quack briwn fox",
            },
            {
                "id": 5,
                "text": "the quack briwn fox jlmps",
            },
            {
                "id": 6,
                "text": "the quack briwn fox jlmps over",
            },
            {
                "id": 7,
                "text": "the quack briwn fox jlmps over the",
            },
            {
                "id": 8,
                "text": "the quack briwn fox jlmps over the lazy",
            },
            {
                "id": 9,
                "text": "the quack briwn fox jlmps over the lazy dog",
            },
            {
                "id": 10,
                "text": "",
            },
            {
                "id": 11,
                "text": "the",
            },
            {
                "id": 12,
                "text": "the quick",
            },
            {
                "id": 13,
                "text": "the quick brown",
            },
            {
                "id": 14,
                "text": "the quick brown fox",
            },
            {
                "id": 15,
                "text": "the quick brown fox jumps",
            },

            {
                "id": 16,
                "text": "the quick brown fox jumps over",
            },
            {
                "id": 17,
                "text": "the quick brown fox jumps over the",
            },
            {
                "id": 18,
                "text": "the quick brown fox jumps over the lazy",
            },
            {
                "id": 19,
                "text": "the quick brown fox jumps over the lazy dog",
            },
        ]))
        .unwrap();
    index
}

fn create_index_with_varying_proximities() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Exactness, Criterion::Words, Criterion::Proximity]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "lazy jumps dog brown quick the over fox the",
            },
            {
                "id": 1,
                "text": "the quick brown fox jumps over the very lazy dog"
            },
            {
                "id": 2,
                "text": "the quick brown fox jumps over the lazy dog",
            },
            {
                "id": 3,
                "text": "dog brown quick the over fox the lazy",
            },
            {
                "id": 4,
                "text": "the quick brown fox over the very lazy dog"
            },
            {
                "id": 5,
                "text": "the quick brown fox over the lazy dog",
            },
            {
                "id": 6,
                "text": "brown quick the over fox",
            },
            {
                "id": 7,
                "text": "the very quick brown fox over"
            },
            {
                "id": 8,
                "text": "the quick brown fox over",
            },
        ]))
        .unwrap();
    index
}

fn create_index_with_typo_and_prefix() -> TempIndex {
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
                "text": "expraordinarily quick brown fox",
            },
            {
                "id": 1,
                "text": "extraordinarily quick brown fox",
            },
            {
                "id": 2,
                "text": "extra quick brown fox",
            },
            {
                "id": 3,
                "text": "expraordinarily quack brown fox",
            },
            {
                "id": 4,
                "text": "expraordinapily quick brown fox",
            }
        ]))
        .unwrap();
    index
}

fn create_index_all_equal_except_proximity_between_ignored_terms() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Exactness, Criterion::Words, Criterion::Proximity]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "lazy jumps dog brown quick the over fox the"
            },
            {
                "id": 1,
                "text": "lazy jumps dog brown quick the over fox the. quack briwn jlmps",
            },
            {
                "id": 2,
                "text": "lazy jumps dog brown quick the over fox the. quack briwn jlmps overt",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_exactness_simple_ordered() {
    let index = create_index_simple_ordered();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the\"",
        "\"the quick brown fox jumps over\"",
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
    let index = create_index_simple_reversed();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"quick brown fox jumps over the lazy dog\"",
        "\"the lazy dog\"",
        "\"over the lazy dog\"",
        "\"jumps over the lazy dog\"",
        "\"fox jumps over the lazy dog\"",
        "\"brown fox jumps over the lazy dog\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"quick brown fox jumps over the lazy dog\"",
        "\"the lazy dog\"",
        "\"over the lazy dog\"",
        "\"jumps over the lazy dog\"",
        "\"fox jumps over the lazy dog\"",
        "\"brown fox jumps over the lazy dog\"",
    ]
    "###);
}

#[test]
fn test_exactness_simple_random() {
    let index = create_index_simple_random();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the jumps dog quick over brown lazy fox\"",
        "\"the dog brown over jumps quick lazy\"",
        "\"jump dog quick the\"",
        "\"jump fox quick lazy the dog\"",
        "\"brown the lazy\"",
        "\"fox the lazy dog brown\"",
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
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"this balcony\"",
        "\"this balcony is overlooking the sea\"",
        "\"what a lovely view from this balcony, I love it\"",
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
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"overlooking the sea is a beautiful balcony\"",
        "\"overlooking the sea is a beautiful balcony, I love it\"",
        "\"a beautiful balcony is overlooking the sea\"",
        "\"this balcony is overlooking the sea\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("overlooking the sea is a beautiful balcony");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"overlooking the sea is a beautiful balcony\"",
        "\"overlooking the sea is a beautiful balcony, I love it\"",
        "\"a beautiful balcony is overlooking the sea\"",
        "\"over looking the sea is a beautiful balcony\"",
        "\"this balcony is overlooking the sea\"",
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
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // "overlooking" is returned here because the term matching strategy allows it
    // but it has the worst exactness score (0 exact words)
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"a beautiful balcony is overlooking the sea\"",
        "\"overlooking the sea is a beautiful balcony, I love it\"",
        "\"overlooking the sea is a beautiful balcony\"",
        "\"this balcony is overlooking the sea\"",
        "\"overlooking\"",
    ]
    "###);
}

#[test]
fn test_exactness_after_words() {
    let index = create_index_simple_ordered_with_typos();

    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Words, Criterion::Exactness]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quack briwn fox jlmps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quack briwn fox jlmps over the lazy\"",
        "\"the quick brown fox jumps over the\"",
        "\"the quick brown fox jumps over\"",
        "\"the quack briwn fox jlmps over\"",
        "\"the quack briwn fox jlmps over the\"",
        "\"the quick brown fox jumps\"",
        "\"the quack briwn fox jlmps\"",
        "\"the quick brown fox\"",
        "\"the quack briwn fox\"",
        "\"the quick brown\"",
        "\"the quack briwn\"",
        "\"the quick\"",
        "\"the quack\"",
        "\"the\"",
        "\"the\"",
    ]
    "###);
}

#[test]
fn test_words_after_exactness() {
    let index = create_index_simple_ordered_with_typos();

    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Exactness, Criterion::Words]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[19, 9, 18, 8, 17, 16, 6, 7, 15, 5, 14, 4, 13, 3, 12, 2, 1, 11]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quack briwn fox jlmps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quack briwn fox jlmps over the lazy\"",
        "\"the quick brown fox jumps over the\"",
        "\"the quick brown fox jumps over\"",
        "\"the quack briwn fox jlmps over\"",
        "\"the quack briwn fox jlmps over the\"",
        "\"the quick brown fox jumps\"",
        "\"the quack briwn fox jlmps\"",
        "\"the quick brown fox\"",
        "\"the quack briwn fox\"",
        "\"the quick brown\"",
        "\"the quack briwn\"",
        "\"the quick\"",
        "\"the quack\"",
        "\"the\"",
        "\"the\"",
    ]
    "###);
}

#[test]
fn test_proximity_after_exactness() {
    let index = create_index_with_varying_proximities();

    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Exactness, Criterion::Words, Criterion::Proximity]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 1, 0, 4, 5, 8, 7, 3, 6]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the very lazy dog\"",
        "\"lazy jumps dog brown quick the over fox the\"",
        "\"the quick brown fox over the very lazy dog\"",
        "\"the quick brown fox over the lazy dog\"",
        "\"the quick brown fox over\"",
        "\"the very quick brown fox over\"",
        "\"dog brown quick the over fox the lazy\"",
        "\"brown quick the over fox\"",
    ]
    "###);

    let index = create_index_all_equal_except_proximity_between_ignored_terms();

    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Exactness, Criterion::Words, Criterion::Proximity]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"lazy jumps dog brown quick the over fox the\"",
        "\"lazy jumps dog brown quick the over fox the. quack briwn jlmps\"",
        "\"lazy jumps dog brown quick the over fox the. quack briwn jlmps overt\"",
    ]
    "###);
}

#[test]
fn test_exactness_followed_by_typo_prefer_no_typo_prefix() {
    let index = create_index_with_typo_and_prefix();

    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Exactness, Criterion::Words, Criterion::Typo]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("quick brown fox extra");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 1, 0, 4, 3]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"extra quick brown fox\"",
        "\"extraordinarily quick brown fox\"",
        "\"expraordinarily quick brown fox\"",
        "\"expraordinapily quick brown fox\"",
        "\"expraordinarily quack brown fox\"",
    ]
    "###);
}

#[test]
fn test_typo_followed_by_exactness() {
    let index = create_index_with_typo_and_prefix();

    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Words, Criterion::Typo, Criterion::Exactness]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("extraordinarily quick brown fox");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 0, 4, 3]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);

    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"extraordinarily quick brown fox\"",
        "\"expraordinarily quick brown fox\"",
        "\"expraordinapily quick brown fox\"",
        "\"expraordinarily quack brown fox\"",
    ]
    "###);
}
