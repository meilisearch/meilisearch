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

use std::collections::BTreeSet;
use std::iter::FromIterator;

use crate::index::tests::TempIndex;
use crate::{SearchResult, TermsMatchingStrategy};

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
#[cfg(not(feature = "swedish-recomposition"))]
fn test_stop_words_not_indexed() {
    let index = create_index();
    crate::db_snap!(index, word_docids, @"6288f9d7db3703b02c57025eb4a69264");
}

#[test]
fn test_ignore_stop_words() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    // `the` is treated as a prefix here, so it's not ignored
    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("xyz to the");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [
            Words(
                Words {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 1,
                },
            ),
            Proximity(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Fid(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Position(
                Rank {
                    rank: 9,
                    max_rank: 11,
                },
            ),
            ExactAttribute(
                NoExactMatch,
            ),
            ExactWords(
                ExactWords {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
        ],
    ]
    "###);

    // `xyz` is treated as a prefix here, so it's not ignored
    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("to the xyz");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [
            Words(
                Words {
                    matching_words: 1,
                    max_matching_words: 2,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 1,
                },
            ),
            Proximity(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Fid(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Position(
                Rank {
                    rank: 9,
                    max_rank: 11,
                },
            ),
            ExactAttribute(
                NoExactMatch,
            ),
            ExactWords(
                ExactWords {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
        ],
    ]
    "###);

    // `xyz` is not treated as a prefix anymore because of the trailing space, so it's ignored
    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("to the xyz ");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [
            Words(
                Words {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 1,
                },
            ),
            Proximity(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Fid(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Position(
                Rank {
                    rank: 9,
                    max_rank: 11,
                },
            ),
            ExactAttribute(
                NoExactMatch,
            ),
            ExactWords(
                ExactWords {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
        ],
    ]
    "###);

    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("to the dragon xyz");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
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
            Proximity(
                Rank {
                    rank: 3,
                    max_rank: 4,
                },
            ),
            Fid(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Position(
                Rank {
                    rank: 17,
                    max_rank: 21,
                },
            ),
            ExactAttribute(
                NoExactMatch,
            ),
            ExactWords(
                ExactWords {
                    matching_words: 2,
                    max_matching_words: 2,
                },
            ),
        ],
    ]
    "###);
}

#[test]
fn test_stop_words_in_phrase() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("\"how to train your dragon\"");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 6]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [
            Words(
                Words {
                    matching_words: 4,
                    max_matching_words: 4,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 0,
                },
            ),
            Proximity(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Fid(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Position(
                Rank {
                    rank: 11,
                    max_rank: 11,
                },
            ),
            ExactAttribute(
                MatchesStart,
            ),
            ExactWords(
                ExactWords {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 4,
                    max_matching_words: 4,
                },
            ),
            Typo(
                Typo {
                    typo_count: 0,
                    max_typo_count: 0,
                },
            ),
            Proximity(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Fid(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Position(
                Rank {
                    rank: 11,
                    max_rank: 11,
                },
            ),
            ExactAttribute(
                MatchesStart,
            ),
            ExactWords(
                ExactWords {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
        ],
    ]
    "###);

    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("how \"to\" train \"the");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
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
                    max_typo_count: 2,
                },
            ),
            Proximity(
                Rank {
                    rank: 2,
                    max_rank: 4,
                },
            ),
            Fid(
                Rank {
                    rank: 1,
                    max_rank: 1,
                },
            ),
            Position(
                Rank {
                    rank: 29,
                    max_rank: 31,
                },
            ),
            ExactAttribute(
                NoExactMatch,
            ),
            ExactWords(
                ExactWords {
                    matching_words: 3,
                    max_matching_words: 3,
                },
            ),
        ],
    ]
    "###);

    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("how \"to\" train \"The dragon");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 6, 5]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));

    let s = index.search(&txn, |builder| {
        builder.terms_matching_strategy(TermsMatchingStrategy::Last);
        builder.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
        builder.query("\"to\"");
    });
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3, 4, 5, 6]");
    // The search is handled as a placeholder search because it doesn't have any non-stop words in it.
    // As a result the scores are empty lists
    insta::assert_snapshot!(format!("{document_scores:#?}"));
}
