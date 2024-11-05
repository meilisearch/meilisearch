/*!
This module tests the interactions between the proximity and typo ranking rules.

The proximity ranking rule should transform the query graph such that it
only contains the word pairs that it used to compute its bucket, but this is not currently
implemented.
*/

use crate::index::tests::TempIndex;
use crate::search::new::tests::collect_field_values;
use crate::{Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Words, Criterion::Proximity, Criterion::Typo]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            // Basic trap.
            //
            // We have one document with the perfect word pair: `sommer - holiday`
            // and another with the perfect word pair: `sommer holidty`.
            //
            // The proximity ranking rule will put them both in the same bucket, and it
            // should minify the query graph to make it represent:
            // EITHER:
            //    sommer + holiday
            // OR:
            //    sommer + holidty
            //
            // Such that the child typo ranking rule does not find any match
            // for its zero-typo bucket `summer + holiday`, even though both documents
            // contain these two exact words.
            {
                "id": 0,
                "text": "summer. holiday. sommer holidty"
            },
            {
                "id": 1,
                "text": "summer. holiday. sommer holiday"
            },

        ]))
        .unwrap();
    index
}

#[test]
fn test_trap_basic() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("summer holiday");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [
            Proximity(
                Rank {
                    rank: 4,
                    max_rank: 4,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 2,
                },
            ),
        ],
        [
            Proximity(
                Rank {
                    rank: 4,
                    max_rank: 4,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 2,
                },
            ),
        ],
    ]
    "###);
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    // This is incorrect, 1 should come before 0
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"summer. holiday. sommer holidty\"",
        "\"summer. holiday. sommer holiday\"",
    ]
    "###);
}
