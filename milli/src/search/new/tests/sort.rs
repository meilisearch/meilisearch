/*!
This module tests the `sort` ranking rule:

1. an error is returned if the sort ranking rule exists but no fields-to-sort were given at search time
2. an error is returned if the fields-to-sort are not sortable
3. it is possible to add multiple fields-to-sort at search time
4. custom sort ranking rules can be added to the settings, they interact with the generic `sort` ranking rule as expected
5. numbers appear before strings
6. documents with either: (1) no value, (2) null, or (3) an object for the field-to-sort appear at the end of the bucket
7. boolean values are translated to strings
8. if a field contains an array, it is sorted by the best value in the array according to the sort rule
*/

use big_s::S;
use maplit::hashset;
use meili_snap::insta;

use crate::index::tests::TempIndex;
use crate::search::new::tests::collect_field_values;
use crate::{
    score_details, AscDesc, Criterion, Member, Search, SearchResult, TermsMatchingStrategy,
};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_sortable_fields(hashset! { S("rank"), S("vague"), S("letter") });
            s.set_criteria(vec![Criterion::Sort]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "letter": "A",
                "rank": 0,
                "vague": 0,
            },
            {
                "id": 1,
                "letter": "A",
                "rank": 1,
                "vague": "0",
            },
            {
                "id": 2,
                "letter": "B",
                "rank": 0,
                "vague": 1,
            },
            {
                "id": 3,
                "letter": "B",
                "rank": 1,
                "vague": "1",
            },
            {
                "id": 4,
                "letter": "B",
                "rank": 2,
                "vague": [1, 2],
            },
            {
                "id": 5,
                "letter": "C",
                "rank": 0,
                "vague": [1, "2"],
            },
            {
                "id": 6,
                "letter": "C",
                "rank": 1,
            },
            {
                "id": 7,
                "letter": "C",
                "rank": 2,
                "vague": null,
            },
            {
                "id": 8,
                "letter": "D",
                "rank": 0,
                "vague": [null, null, ""]
            },
            {
                "id": 9,
                "letter": "E",
                "rank": 0,
                "vague": ""
            },
            {
                "id": 10,
                "letter": "E",
                "rank": 1,
                "vague": {
                    "sub": 0,
                }
            },
            {
                "id": 11,
                "letter": "E",
                "rank": 2,
                "vague": true,
            },
            {
                "id": 12,
                "letter": "E",
                "rank": 3,
                "vague": false,
            },
            {
                "id": 13,
                "letter": "E",
                "rank": 4,
                "vague": 1.5673,
            },
            {
                "id": 14,
                "letter": "E",
                "rank": 5,
            },
            {
                "id": 15,
                "letter": "F",
                "rank": 0,
            },
            {
                "id": 16,
                "letter": "F",
                "rank": 1,
            },
            {
                "id": 17,
                "letter": "F",
                "rank": 2,
            },
            {
                "id": 18,
                "letter": "G",
                "rank": 0,
            },
            {
                "id": 19,
                "letter": "G",
                "rank": 1,
            },
            {
                "id": 20,
                "letter": "H",
                "rank": 0,
                "vague": true,
            },
            {
                "id": 21,
                "letter": "I",
                "rank": 0,
                "vague": false,
            },
            {
                "id": 22,
                "letter": "I",
                "rank": 1,
                "vague": [1.1367, "help", null]
            },
            {
                "id": 23,
                "letter": "I",
                "rank": 2,
                "vague": [1.2367, "hello"]
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_sort() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![AscDesc::Desc(Member::Field(S("letter")))]);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[21, 22, 23, 20, 18, 19, 15, 16, 17, 9, 10, 11, 12, 13, 14, 8, 5, 6, 7, 2]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));

    let letter_values = collect_field_values(&index, &txn, "letter", &documents_ids);
    insta::assert_debug_snapshot!(letter_values, @r###"
    [
        "\"I\"",
        "\"I\"",
        "\"I\"",
        "\"H\"",
        "\"G\"",
        "\"G\"",
        "\"F\"",
        "\"F\"",
        "\"F\"",
        "\"E\"",
        "\"E\"",
        "\"E\"",
        "\"E\"",
        "\"E\"",
        "\"E\"",
        "\"D\"",
        "\"C\"",
        "\"C\"",
        "\"C\"",
        "\"B\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![AscDesc::Desc(Member::Field(S("rank")))]);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[14, 13, 12, 4, 7, 11, 17, 23, 1, 3, 6, 10, 16, 19, 22, 0, 2, 5, 8, 9]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));

    let rank_values = collect_field_values(&index, &txn, "rank", &documents_ids);
    insta::assert_debug_snapshot!(rank_values, @r###"
    [
        "5",
        "4",
        "3",
        "2",
        "2",
        "2",
        "2",
        "2",
        "1",
        "1",
        "1",
        "1",
        "1",
        "1",
        "1",
        "0",
        "0",
        "0",
        "0",
        "0",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![AscDesc::Asc(Member::Field(S("vague")))]);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 2, 4, 5, 22, 23, 13, 1, 3, 12, 21, 11, 20, 6, 7, 8, 9, 10, 14, 15]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));

    let vague_values = collect_field_values(&index, &txn, "vague", &documents_ids);
    insta::assert_debug_snapshot!(vague_values, @r###"
    [
        "0",
        "1",
        "[1,2]",
        "[1,\"2\"]",
        "[1.1367,\"help\",null]",
        "[1.2367,\"hello\"]",
        "1.5673",
        "\"0\"",
        "\"1\"",
        "false",
        "false",
        "true",
        "true",
        "__does_not_exist__",
        "null",
        "[null,null,\"\"]",
        "\"\"",
        "{\"sub\":0}",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![AscDesc::Desc(Member::Field(S("vague")))]);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[4, 13, 23, 22, 2, 5, 0, 11, 20, 12, 21, 3, 1, 6, 7, 8, 9, 10, 14, 15]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));

    let vague_values = collect_field_values(&index, &txn, "vague", &documents_ids);
    insta::assert_debug_snapshot!(vague_values, @r###"
    [
        "[1,2]",
        "1.5673",
        "[1.2367,\"hello\"]",
        "[1.1367,\"help\",null]",
        "1",
        "[1,\"2\"]",
        "0",
        "true",
        "true",
        "false",
        "false",
        "\"1\"",
        "\"0\"",
        "__does_not_exist__",
        "null",
        "[null,null,\"\"]",
        "\"\"",
        "{\"sub\":0}",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);
}

#[test]
fn test_redacted() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_displayed_fields(vec!["text".to_owned(), "vague".to_owned()]);
            s.set_sortable_fields(hashset! { S("rank"), S("vague"), S("letter") });
            s.set_criteria(vec![Criterion::Sort]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![
        AscDesc::Asc(Member::Field(S("vague"))),
        AscDesc::Asc(Member::Field(S("letter"))),
    ]);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    let document_scores_json: Vec<_> = document_scores
        .iter()
        .map(|scores| score_details::ScoreDetails::to_json_map(scores.iter()))
        .collect();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 2, 4, 5, 22, 23, 13, 1, 3, 12, 21, 11, 20, 6, 7, 8, 9, 10, 14, 15]");
    insta::assert_json_snapshot!(document_scores_json);
}
