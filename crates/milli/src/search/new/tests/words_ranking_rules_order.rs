/*!
This module tests the following property:

When `AttributeRank` or `WordPosition` ranking rules are placed before the `Words`
ranking rule in the ranking rules list, the `Words` rule should still be
automatically inserted so that `TermsMatchingStrategy` works correctly and
documents matching fewer query words are still returned.

This is a regression test for a bug where placing `AttributeRank` or `WordPosition`
before `Words` caused the search to only return documents matching ALL query words,
instead of also returning partial matches.
*/

use crate::index::tests::TempIndex;
use crate::search::new::tests::collect_field_values;
use crate::{Criterion, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec![
                "title".to_owned(),
                "category".to_owned(),
                "description".to_owned(),
            ]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "title": "albert einstein",
                "category": "physics",
                "description": "famous physicist who developed the theory of relativity",
            },
            {
                "id": 1,
                "title": "albert abraham michelson",
                "category": "physics",
                "description": "measured the speed of light with great precision",
            },
            {
                "id": 2,
                "title": "albert camus",
                "category": "literature",
                "description": "french author and philosopher known for absurdism",
            },
            {
                "id": 3,
                "title": "albert schweitzer",
                "category": "medicine",
                "description": "physician and philosopher in africa",
            },
            {
                "id": 4,
                "title": "physics of fluids",
                "category": "physics",
                "description": "a journal about fluid dynamics and related topics",
            },
            {
                "id": 5,
                "title": "richard feynman",
                "category": "physics",
                "description": "albert einstein once inspired this quantum physicist",
            },
        ]))
        .unwrap();
    index
}

/// Test that with default ranking rules (Words first), we get all partial matches.
#[test]
fn test_default_ranking_rules_returns_all_matches() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![
                Criterion::Words,
                Criterion::Typo,
                Criterion::Proximity,
                Criterion::Attribute,
            ]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();
    let mut s = index.search(&txn);
    s.query("albert physics");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // Should return documents matching "albert" (partial matches allowed via Last strategy)
    // "physics" is removed first (last word), so doc 4 (only "physics") is not returned
    let texts = collect_field_values(&index, &txn, "title", &documents_ids);
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 5, 3, 2]");
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"albert einstein\"",
        "\"albert abraham michelson\"",
        "\"richard feynman\"",
        "\"albert schweitzer\"",
        "\"albert camus\"",
    ]
    "###);
}

/// Regression test: AttributeRank before Words should still auto-insert Words
/// and return the same number of hits.
#[test]
fn test_attribute_rank_before_words_returns_all_matches() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![
                Criterion::AttributeRank,
                Criterion::Words,
                Criterion::Typo,
                Criterion::Proximity,
            ]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();
    let mut s = index.search(&txn);
    s.query("albert physics");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // Must return the same 5 documents as default ordering, not just the 3 with category=physics
    let texts = collect_field_values(&index, &txn, "title", &documents_ids);
    // The order may differ due to different ranking rule priority, but the COUNT must be the same
    assert_eq!(documents_ids.len(), 5, "Expected 5 hits but got {}: {:?}", documents_ids.len(), texts);
    insta::assert_snapshot!(format!("{documents_ids:?}"));
    insta::assert_debug_snapshot!(texts);
}

/// Regression test: WordPosition before Words should still auto-insert Words
/// and return the same number of hits.
#[test]
fn test_word_position_before_words_returns_all_matches() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![
                Criterion::WordPosition,
                Criterion::Words,
                Criterion::Typo,
                Criterion::Proximity,
            ]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();
    let mut s = index.search(&txn);
    s.query("albert physics");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // Must return the same 5 documents as default ordering
    let texts = collect_field_values(&index, &txn, "title", &documents_ids);
    assert_eq!(documents_ids.len(), 5, "Expected 5 hits but got {}: {:?}", documents_ids.len(), texts);
    insta::assert_snapshot!(format!("{documents_ids:?}"));
    insta::assert_debug_snapshot!(texts);
}

/// Test that both AttributeRank AND WordPosition before Words still works.
#[test]
fn test_both_attribute_rank_and_word_position_before_words() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![
                Criterion::AttributeRank,
                Criterion::WordPosition,
                Criterion::Words,
                Criterion::Typo,
                Criterion::Proximity,
            ]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();
    let mut s = index.search(&txn);
    s.query("albert physics");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    let texts = collect_field_values(&index, &txn, "title", &documents_ids);
    assert_eq!(documents_ids.len(), 5, "Expected 5 hits but got {}: {:?}", documents_ids.len(), texts);
    insta::assert_snapshot!(format!("{documents_ids:?}"));
    insta::assert_debug_snapshot!(texts);
}

/// Test with TermsMatchingStrategy::All to verify it still correctly restricts
/// to only full matches regardless of ranking rule order.
#[test]
fn test_attribute_rank_before_words_with_tms_all() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![
                Criterion::AttributeRank,
                Criterion::Words,
                Criterion::Typo,
                Criterion::Proximity,
            ]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();
    let mut s = index.search(&txn);
    s.query("albert physics");
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // With TMS::All, only documents containing BOTH "albert" and "physics" should be returned
    let texts = collect_field_values(&index, &txn, "title", &documents_ids);
    insta::assert_snapshot!(format!("{documents_ids:?}"));
    insta::assert_debug_snapshot!(texts);
}
