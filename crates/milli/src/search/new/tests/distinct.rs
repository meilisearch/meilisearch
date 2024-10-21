/*!
This module tests the "distinct attribute" feature, and its
interaction with other ranking rules.

1. no duplicate distinct attributes are ever returned
2. only the best document (according to the search rules) for each distinct value appears in the result
3. if a document does not have a distinct attribute, then the distinct rule does not apply to it

It doesn't test properly:
- combination of distinct + exhaustive_nbr_hits (because we know it's incorrect)
- distinct attributes with arrays (because we know it's incorrect as well)
*/

use std::collections::HashSet;

use big_s::S;
use heed::RoTxn;
use maplit::hashset;

use super::collect_field_values;
use crate::index::tests::TempIndex;
use crate::{AscDesc, Criterion, Index, Member, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_sortable_fields(hashset! { S("rank1"), S("letter") });
            s.set_distinct_field("letter".to_owned());
            s.set_criteria(vec![Criterion::Words]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "letter": "A",
                "rank1": 0,
                "text": "the quick brown fox jamps over the lazy dog",
            },
            {
                "id": 1,
                "letter": "A",
                "rank1": 1,
                "text": "the quick brown fox jumpes over the lazy dog",
            },
            {
                "id": 2,
                "letter": "B",
                "rank1": 0,
                "text": "the quick brown foxjumps over the lazy dog",
            },
            {
                "id": 3,
                "letter": "B",
                "rank1": 1,
                "text": "the quick brown fox jumps over the lazy dog",
            },
            {
                "id": 4,
                "letter": "B",
                "rank1": 2,
                "text": "the quick brown fox jumps over the lazy",
            },
            {
                "id": 5,
                "letter": "C",
                "rank1": 0,
                "text": "the quickbrownfox jumps over the lazy",
            },
            {
                "id": 6,
                "letter": "C",
                "rank1": 1,
                "text": "the quick brown fox jumpss over the lazy",
            },
            {
                "id": 7,
                "letter": "C",
                "rank1": 2,
                "text": "the quick brown fox jumps over the lazy",
            },
            {
                "id": 8,
                "letter": "D",
                "rank1": 0,
                "text": "the quick brown fox jumps over the lazy",
            },
            {
                "id": 9,
                "letter": "E",
                "rank1": 0,
                "text": "the quick brown fox jumps over the lazy",
            },
            {
                "id": 10,
                "letter": "E",
                "rank1": 1,
                "text": "the quackbrown foxjunps over",
            },
            {
                "id": 11,
                "letter": "E",
                "rank1": 2,
                "text": "the quicko browno fox junps over",
            },
            {
                "id": 12,
                "letter": "E",
                "rank1": 3,
                "text": "the quicko browno fox jumps over",
            },
            {
                "id": 13,
                "letter": "E",
                "rank1": 4,
                "text": "the quick brewn fox jumps over",
            },
            {
                "id": 14,
                "letter": "E",
                "rank1": 5,
                "text": "the quick brown fox jumps over",
            },
            {
                "id": 15,
                "letter": "F",
                "rank1": 0,
                "text": "the quick brownf fox jumps over",
            },
            {
                "id": 16,
                "letter": "F",
                "rank1": 1,
                "text": "the quic brown fox jamps over",
            },
            {
                "id": 17,
                "letter": "F",
                "rank1": 2,
                "text": "thequick browns fox jimps",
            },
            {
                "id": 18,
                "letter": "G",
                "rank1": 0,
                "text": "the qick brown fox jumps",
            },
            {
                "id": 19,
                "letter": "G",
                "rank1": 1,
                "text": "the quick brownfoxjumps",
            },
            {
                "id": 20,
                "letter": "H",
                "rank1": 0,
                "text": "the quick brow fox jumps",
            },
            {
                "id": 21,
                "letter": "I",
                "rank1": 0,
                "text": "the quick brown fox jpmps",
            },
            {
                "id": 22,
                "letter": "I",
                "rank1": 1,
                "text": "the quick brown fox jumps",
            },
            {
                "id": 23,
                "letter": "I",
                "rank1": 2,
                "text": "the quick",
            },
            {
                "id": 24,
                "rank1": 0,
                "text": "the quick",
            },
            {
                "id": 25,
                "rank1": 1,
                "text": "the quick brown",
            },
            {
                "id": 26,
                "rank1": 2,
                "text": "the quick brown fox",
            },
            {
                "id": 26,
                "rank1": 3,
                "text": "the quick brown fox jumps over the lazy dog",
            },
        ]))
        .unwrap();
    index
}

fn verify_distinct(
    index: &Index,
    txn: &RoTxn<'_>,
    distinct: Option<&str>,
    docids: &[u32],
) -> Vec<String> {
    let vs = collect_field_values(
        index,
        txn,
        distinct.or_else(|| index.distinct_field(txn).unwrap()).unwrap(),
        docids,
    );

    let mut unique = HashSet::new();
    for v in vs.iter() {
        if v == "__does_not_exist__" {
            continue;
        }
        assert!(unique.insert(v.clone()));
    }

    vs
}

#[test]
fn test_distinct_placeholder_no_ranking_rules() {
    let index = create_index();

    // Set the letter as filterable and unset the distinct attribute.
    index
        .update_settings(|s| {
            s.set_filterable_fields(hashset! { S("letter") });
            s.reset_distinct_field();
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.distinct(S("letter"));
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 2, 5, 8, 9, 15, 18, 20, 21, 24, 25, 26]");
    let distinct_values = verify_distinct(&index, &txn, Some("letter"), &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"A\"",
        "\"B\"",
        "\"C\"",
        "\"D\"",
        "\"E\"",
        "\"F\"",
        "\"G\"",
        "\"H\"",
        "\"I\"",
        "__does_not_exist__",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);
}

#[test]
fn test_distinct_at_search_placeholder_no_ranking_rules() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let s = Search::new(&txn, &index);
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 2, 5, 8, 9, 15, 18, 20, 21, 24, 25, 26]");
    let distinct_values = verify_distinct(&index, &txn, None, &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"A\"",
        "\"B\"",
        "\"C\"",
        "\"D\"",
        "\"E\"",
        "\"F\"",
        "\"G\"",
        "\"H\"",
        "\"I\"",
        "__does_not_exist__",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);
}

#[test]
fn test_distinct_placeholder_sort() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Sort]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.sort_criteria(vec![AscDesc::Desc(Member::Field(S("rank1")))]);

    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[14, 26, 4, 7, 17, 23, 1, 19, 25, 8, 20, 24]");
    let distinct_values = verify_distinct(&index, &txn, None, &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"E\"",
        "__does_not_exist__",
        "\"B\"",
        "\"C\"",
        "\"F\"",
        "\"I\"",
        "\"A\"",
        "\"G\"",
        "__does_not_exist__",
        "\"D\"",
        "\"H\"",
        "__does_not_exist__",
    ]
    "###);
    let rank_values = collect_field_values(&index, &txn, "rank1", &documents_ids);
    insta::assert_debug_snapshot!(rank_values, @r###"
    [
        "5",
        "3",
        "2",
        "2",
        "2",
        "2",
        "1",
        "1",
        "1",
        "0",
        "0",
        "0",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.sort_criteria(vec![AscDesc::Desc(Member::Field(S("letter")))]);

    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[21, 20, 18, 15, 9, 8, 5, 2, 0, 24, 25, 26]");
    let distinct_values = verify_distinct(&index, &txn, None, &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"I\"",
        "\"H\"",
        "\"G\"",
        "\"F\"",
        "\"E\"",
        "\"D\"",
        "\"C\"",
        "\"B\"",
        "\"A\"",
        "__does_not_exist__",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);
    let rank_values = collect_field_values(&index, &txn, "rank1", &documents_ids);
    insta::assert_debug_snapshot!(rank_values, @r###"
    [
        "0",
        "0",
        "0",
        "0",
        "0",
        "0",
        "0",
        "0",
        "0",
        "0",
        "1",
        "3",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.sort_criteria(vec![
        AscDesc::Desc(Member::Field(S("letter"))),
        AscDesc::Desc(Member::Field(S("rank1"))),
    ]);

    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[23, 20, 19, 17, 14, 8, 7, 4, 1, 26, 25, 24]");
    let distinct_values = verify_distinct(&index, &txn, None, &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"I\"",
        "\"H\"",
        "\"G\"",
        "\"F\"",
        "\"E\"",
        "\"D\"",
        "\"C\"",
        "\"B\"",
        "\"A\"",
        "__does_not_exist__",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);
    let rank_values = collect_field_values(&index, &txn, "rank1", &documents_ids);
    insta::assert_debug_snapshot!(rank_values, @r###"
    [
        "2",
        "0",
        "1",
        "2",
        "5",
        "0",
        "2",
        "2",
        "1",
        "3",
        "1",
        "0",
    ]
    "###);
}

#[test]
fn test_distinct_words() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Words]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");

    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 2, 26, 5, 8, 9, 15, 18, 20, 21, 25, 24]");
    let distinct_values = verify_distinct(&index, &txn, None, &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"A\"",
        "\"B\"",
        "__does_not_exist__",
        "\"C\"",
        "\"D\"",
        "\"E\"",
        "\"F\"",
        "\"G\"",
        "\"H\"",
        "\"I\"",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);
    let text_values = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(text_values, @r###"
    [
        "\"the quick brown fox jamps over the lazy dog\"",
        "\"the quick brown foxjumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quickbrownfox jumps over the lazy\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brownf fox jumps over\"",
        "\"the qick brown fox jumps\"",
        "\"the quick brow fox jumps\"",
        "\"the quick brown fox jpmps\"",
        "\"the quick brown\"",
        "\"the quick\"",
    ]
    "###);
}

#[test]
fn test_distinct_sort_words() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Sort, Criterion::Words, Criterion::Desc(S("rank1"))]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.query("the quick brown fox jumps over the lazy dog");
    s.sort_criteria(vec![AscDesc::Desc(Member::Field(S("letter")))]);

    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[22, 20, 19, 16, 9, 8, 7, 3, 1, 26, 25, 24]");
    let distinct_values = verify_distinct(&index, &txn, None, &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"I\"",
        "\"H\"",
        "\"G\"",
        "\"F\"",
        "\"E\"",
        "\"D\"",
        "\"C\"",
        "\"B\"",
        "\"A\"",
        "__does_not_exist__",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);

    let rank_values = collect_field_values(&index, &txn, "rank1", &documents_ids);
    insta::assert_debug_snapshot!(rank_values, @r###"
    [
        "1",
        "0",
        "1",
        "1",
        "0",
        "0",
        "2",
        "1",
        "1",
        "3",
        "1",
        "0",
    ]
    "###);

    let text_values = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(text_values, @r###"
    [
        "\"the quick brown fox jumps\"",
        "\"the quick brow fox jumps\"",
        "\"the quick brownfoxjumps\"",
        "\"the quic brown fox jamps over\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumpes over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown\"",
        "\"the quick\"",
    ]
    "###);
}

#[test]
fn test_distinct_all_candidates() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Sort]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.sort_criteria(vec![AscDesc::Desc(Member::Field(S("rank1")))]);
    s.exhaustive_number_hits(true);

    let SearchResult { documents_ids, candidates, .. } = s.execute().unwrap();
    let candidates = candidates.iter().collect::<Vec<_>>();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[14, 26, 4, 7, 17, 23, 1, 19, 25, 8, 20, 24]");
    // This is incorrect, but unfortunately impossible to do better efficiently.
    insta::assert_snapshot!(format!("{candidates:?}"), @"[1, 4, 7, 8, 14, 17, 19, 20, 23, 24, 25, 26]");
}

#[test]
fn test_distinct_typo() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Words, Criterion::Typo]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.query("the quick brown fox jumps over the lazy dog");
    s.terms_matching_strategy(TermsMatchingStrategy::Last);

    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 26, 0, 7, 8, 9, 15, 22, 18, 20, 25, 24]");

    let distinct_values = verify_distinct(&index, &txn, None, &documents_ids);
    insta::assert_debug_snapshot!(distinct_values, @r###"
    [
        "\"B\"",
        "__does_not_exist__",
        "\"A\"",
        "\"C\"",
        "\"D\"",
        "\"E\"",
        "\"F\"",
        "\"I\"",
        "\"G\"",
        "\"H\"",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);

    let text_values = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(text_values, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jamps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brownf fox jumps over\"",
        "\"the quick brown fox jumps\"",
        "\"the qick brown fox jumps\"",
        "\"the quick brow fox jumps\"",
        "\"the quick brown\"",
        "\"the quick\"",
    ]
    "###);
}
