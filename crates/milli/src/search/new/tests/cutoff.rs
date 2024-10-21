//! This module test the search cutoff and ensure a few things:
//! 1. A basic test works and mark the search as degraded
//! 2. A test that ensure the filters are affectively applied even with a cutoff of 0
//! 3. A test that ensure the cutoff works well with the ranking scores

use std::time::Duration;

use big_s::S;
use maplit::hashset;
use meili_snap::snapshot;

use crate::index::tests::TempIndex;
use crate::score_details::{ScoreDetails, ScoringStrategy};
use crate::{Criterion, Filter, Search, TimeBudget};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_filterable_fields(hashset! { S("id") });
            s.set_criteria(vec![Criterion::Words, Criterion::Typo]);
        })
        .unwrap();

    // reverse the ID / insertion order so we see better what was sorted from what got the insertion order ordering
    index
        .add_documents(documents!([
            {
                "id": 4,
                "text": "hella puppo kefir",
            },
            {
                "id": 3,
                "text": "hella puppy kefir",
            },
            {
                "id": 2,
                "text": "hello",
            },
            {
                "id": 1,
                "text": "hello puppy",
            },
            {
                "id": 0,
                "text": "hello puppy kefir",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn basic_degraded_search() {
    let index = create_index();
    let rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&rtxn, &index);
    search.query("hello puppy kefir");
    search.limit(3);
    search.time_budget(TimeBudget::new(Duration::from_millis(0)));

    let result = search.execute().unwrap();
    assert!(result.degraded);
}

#[test]
fn degraded_search_cannot_skip_filter() {
    let index = create_index();
    let rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&rtxn, &index);
    search.query("hello puppy kefir");
    search.limit(100);
    search.time_budget(TimeBudget::new(Duration::from_millis(0)));
    let filter_condition = Filter::from_str("id > 2").unwrap().unwrap();
    search.filter(filter_condition);

    let result = search.execute().unwrap();
    assert!(result.degraded);
    snapshot!(format!("{:?}\n{:?}", result.candidates, result.documents_ids), @r###"
    RoaringBitmap<[0, 1]>
    [0, 1]
    "###);
}

#[test]
#[allow(clippy::format_collect)] // the test is already quite big
fn degraded_search_and_score_details() {
    let index = create_index();
    let rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&rtxn, &index);
    search.query("hello puppy kefir");
    search.limit(4);
    search.scoring_strategy(ScoringStrategy::Detailed);
    search.time_budget(TimeBudget::max());

    let result = search.execute().unwrap();
    snapshot!(format!("IDs: {:?}\nScores: {}\nScore Details:\n{:#?}", result.documents_ids, result.document_scores.iter().map(|scores| format!("{:.4} ", ScoreDetails::global_score(scores.iter()))).collect::<String>(), result.document_scores), @r###"
    IDs: [4, 1, 0, 3]
    Scores: 1.0000 0.9167 0.8333 0.6667 
    Score Details:
    [
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 1,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 2,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 2,
                    max_matching_words: 3,
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

    // Do ONE loop iteration. Not much can be deduced, almost everyone matched the words first bucket.
    search.time_budget(TimeBudget::max().with_stop_after(1));

    let result = search.execute().unwrap();
    snapshot!(format!("IDs: {:?}\nScores: {}\nScore Details:\n{:#?}", result.documents_ids, result.document_scores.iter().map(|scores| format!("{:.4} ", ScoreDetails::global_score(scores.iter()))).collect::<String>(), result.document_scores), @r###"
    IDs: [0, 1, 4, 2]
    Scores: 0.6667 0.6667 0.6667 0.0000 
    Score Details:
    [
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Skipped,
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Skipped,
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Skipped,
        ],
        [
            Skipped,
        ],
    ]
    "###);

    // Do TWO loop iterations. The first document should be entirely sorted
    search.time_budget(TimeBudget::max().with_stop_after(2));

    let result = search.execute().unwrap();
    snapshot!(format!("IDs: {:?}\nScores: {}\nScore Details:\n{:#?}", result.documents_ids, result.document_scores.iter().map(|scores| format!("{:.4} ", ScoreDetails::global_score(scores.iter()))).collect::<String>(), result.document_scores), @r###"
    IDs: [4, 0, 1, 2]
    Scores: 1.0000 0.6667 0.6667 0.0000 
    Score Details:
    [
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Skipped,
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Skipped,
        ],
        [
            Skipped,
        ],
    ]
    "###);

    // Do THREE loop iterations. The second document should be entirely sorted as well
    search.time_budget(TimeBudget::max().with_stop_after(3));

    let result = search.execute().unwrap();
    snapshot!(format!("IDs: {:?}\nScores: {}\nScore Details:\n{:#?}", result.documents_ids, result.document_scores.iter().map(|scores| format!("{:.4} ", ScoreDetails::global_score(scores.iter()))).collect::<String>(), result.document_scores), @r###"
    IDs: [4, 1, 0, 2]
    Scores: 1.0000 0.9167 0.6667 0.0000 
    Score Details:
    [
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 1,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Skipped,
        ],
        [
            Skipped,
        ],
    ]
    "###);

    // Do FOUR loop iterations. The third document should be entirely sorted as well
    // The words bucket have still not progressed thus the last document doesn't have any info yet.
    search.time_budget(TimeBudget::max().with_stop_after(4));

    let result = search.execute().unwrap();
    snapshot!(format!("IDs: {:?}\nScores: {}\nScore Details:\n{:#?}", result.documents_ids, result.document_scores.iter().map(|scores| format!("{:.4} ", ScoreDetails::global_score(scores.iter()))).collect::<String>(), result.document_scores), @r###"
    IDs: [4, 1, 0, 2]
    Scores: 1.0000 0.9167 0.8333 0.0000 
    Score Details:
    [
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 1,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 2,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Skipped,
        ],
    ]
    "###);

    // After SIX loop iteration. The words ranking rule gave us a new bucket.
    // Since we reached the limit we were able to early exit without checking the typo ranking rule.
    search.time_budget(TimeBudget::max().with_stop_after(6));

    let result = search.execute().unwrap();
    snapshot!(format!("IDs: {:?}\nScores: {}\nScore Details:\n{:#?}", result.documents_ids, result.document_scores.iter().map(|scores| format!("{:.4} ", ScoreDetails::global_score(scores.iter()))).collect::<String>(), result.document_scores), @r###"
    IDs: [4, 1, 0, 3]
    Scores: 1.0000 0.9167 0.8333 0.3333 
    Score Details:
    [
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 1,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
            Typo(
                Typo {
                    typo_count: 2,
                    max_typo_count: 3,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 2,
                    max_matching_words: 3,
                },
            ),
            Skipped,
        ],
    ]
    "###);
}
