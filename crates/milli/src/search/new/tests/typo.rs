/*!
This module tests the following properties:

1. The `words` ranking rule is typo-tolerant
2. Typo-tolerance handles missing letters, extra letters, replaced letters, and swapped letters (at least)
3. Words which are < `min_word_len_one_typo` are not typo tolerant
4. Words which are >= `min_word_len_one_typo` but < `min_word_len_two_typos` can have one typo
5. Words which are >= `min_word_len_two_typos` can have two typos
6. A typo on the first letter of a word counts as two typos
7. Phrases are not typo tolerant
8. 2grams can have 1 typo if they are larger than `min_word_len_two_typos`
9. 3grams are not typo tolerant (but they can be split into two words)
10. The `typo` ranking rule assumes the role of the `words` ranking rule implicitly
    if `words` doesn't exist before it.
11. The `typo` ranking rule places documents with the same number of typos in the same bucket
12. Prefix tolerance costs nothing according to the typo ranking rule
13. Split words cost 1 typo according to the typo ranking rule
14. Synonyms cost nothing according to the typo ranking rule
*/

use std::collections::BTreeMap;

use crate::index::tests::TempIndex;
use crate::search::new::tests::collect_field_values;
use crate::{Criterion, Search, SearchResult, TermsMatchingStrategy};

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
                "text": "the quick brown fox jumps over the lazy dog"
            },
            {
                "id": 1,
                "text": "the quick brown foxes jump over the lazy dog"
            },
            {
                "id": 2,
                "text": "the quick brown fax sends a letter to the dog"
            },
            {
                "id": 3,
                "text": "the quickest brownest fox jumps over the laziest dog"
            },
            {
                "id": 4,
                "text": "a fox doesn't quack, that crown goes to the duck."
            },
            {
                "id": 5,
                "text": "the quicker browner fox jumped over the lazier dog"
            },
            {
                "id": 6,
                "text": "the extravagant fox skyrocketed over the languorous dog" // thanks thesaurus
            },
            {
                "id": 7,
                "text": "the quick brown fox jumps over the lazy"
            },
            {
                "id": 8,
                "text": "the quick brown fox jumps over the"
            },
            {
                "id": 9,
                "text": "the quick brown fox jumps over"
            },
            {
                "id": 10,
                "text": "the quick brown fox jumps"
            },
            {
                "id": 11,
                "text": "the quick brown fox"
            },
            {
                "id": 12,
                "text": "the quick brown"
            },
            {
                "id": 13,
                "text": "the quick"
            },
            {
                "id": 14,
                "text": "netwolk interconections sunflawar"
            },
            {
                "id": 15,
                "text": "network interconnections sunflawer"
            },
            {
                "id": 16,
                "text": "network interconnection sunflower"
            },
            {
                "id": 17,
                "text": "network interconnection sun flower"
            },
            {
                "id": 18,
                "text": "network interconnection sunflowering"
            },
            {
                "id": 19,
                "text": "network interconnection sun flowering"
            },
            {
                "id": 20,
                "text": "network interconnection sunflowar"
            },
            {
                "id": 21,
                "text": "the fast brownish fox jumps over the lackadaisical dog"
            },
            {
                "id": 22,
                "text": "the quick brown fox jumps over the lackadaisical dog"
            },
            {
                "id": 23,
                "text": "the quivk brown fox jumps over the lazy dog"
            },
            {
                "id": 24,
                "tolerant_text": "the quick brown fox jumps over the lazy dog",
            },
            {
                "id": 25,
                "tolerant_text": "the quivk brown fox jumps over the lazy dog",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_no_typo() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_autorize_typos(false);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[[]]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
    ]
    "###);
}

#[test]
fn test_default_typo() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let ot = index.min_word_len_one_typo(&txn).unwrap();
    let tt = index.min_word_len_two_typos(&txn).unwrap();
    insta::assert_debug_snapshot!(ot, @"5");
    insta::assert_debug_snapshot!(tt, @"9");

    // 0 typo
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 23]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [],
        [],
    ]
    "###);
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quivk brown fox jumps over the lazy dog\"",
    ]
    "###);

    // 1 typo on one word, replaced letter
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quack brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[[]]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
    ]
    "###);

    // 1 typo on one word, missing letter, extra letter
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quicest brownest fox jummps over the laziest dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[[]]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quickest brownest fox jumps over the laziest dog\"",
    ]
    "###);
}

#[test]
fn test_phrase_no_typo_allowed() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the \"quick brewn\" fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @"[]");
}

#[test]
fn test_typo_exact_word() {
    let index = create_index();

    index
        .update_settings(|s| {
            s.set_exact_words(
                ["quick", "quack", "sunflower"].iter().map(ToString::to_string).collect(),
            )
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let ot = index.min_word_len_one_typo(&txn).unwrap();
    let tt = index.min_word_len_two_typos(&txn).unwrap();
    insta::assert_debug_snapshot!(ot, @"5");
    insta::assert_debug_snapshot!(tt, @"9");

    // don't match quivk
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[[]]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
    ]
    "###);

    // Don't match quick
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quack brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[]");

    // words not in exact_words (quicest, jummps) have normal typo handling
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quicest brownest fox jummps over the laziest dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[[]]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quickest brownest fox jumps over the laziest dog\"",
    ]
    "###);

    // exact words do not disable prefix (sunflowering OK, but no sunflowar)
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("network interconnection sunflower");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[16, 17, 18]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"network interconnection sunflower\"",
        "\"network interconnection sun flower\"",
        "\"network interconnection sunflowering\"",
    ]
    "###);
}

#[test]
fn test_typo_exact_attribute() {
    let index = create_index();

    index
        .update_settings(|s| {
            s.set_exact_attributes(["text"].iter().map(ToString::to_string).collect());
            s.set_searchable_fields(
                ["text", "tolerant_text"].iter().map(ToString::to_string).collect(),
            );
            s.set_exact_words(["quivk"].iter().map(ToString::to_string).collect())
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let ot = index.min_word_len_one_typo(&txn).unwrap();
    let tt = index.min_word_len_two_typos(&txn).unwrap();
    insta::assert_debug_snapshot!(ot, @"5");
    insta::assert_debug_snapshot!(tt, @"9");

    // Exact match returns both exact attributes and tolerant ones.
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 24, 25]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "__does_not_exist__",
        "__does_not_exist__",
    ]
    "###);
    let texts = collect_field_values(&index, &txn, "tolerant_text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "__does_not_exist__",
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quivk brown fox jumps over the lazy dog\"",
    ]
    "###);

    // 1 typo only returns the tolerant attribute
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the quidk brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[24, 25]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [],
        [],
    ]
    "###);
    let texts = collect_field_values(&index, &txn, "tolerant_text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quivk brown fox jumps over the lazy dog\"",
    ]
    "###);

    // combine with exact words
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the quivk brown fox jumps over the lazy dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[23, 25]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @r###"
    [
        [],
        [],
    ]
    "###);
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quivk brown fox jumps over the lazy dog\"",
        "__does_not_exist__",
    ]
    "###);
    let texts = collect_field_values(&index, &txn, "tolerant_text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "__does_not_exist__",
        "\"the quivk brown fox jumps over the lazy dog\"",
    ]
    "###);

    // No result in tolerant attribute
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the quicest brownest fox jummps over the laziest dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[]");
}

#[test]
fn test_ngram_typos() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the extra lagant fox skyrocketed over the languorous dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[[]]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the extravagant fox skyrocketed over the languorous dog\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the ex tra lagant fox skyrocketed over the languorous dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[]");
    insta::assert_snapshot!(format!("{document_scores:#?}"), @"[]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @"[]");
}
#[test]
fn test_typo_ranking_rule_not_preceded_by_words_ranking_rule() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Typo]);
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids: ids_1, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{ids_1:?}"), @"[0, 23, 7, 8, 9, 22, 10, 11, 1, 2, 12, 13, 4, 3, 5, 6, 21]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &ids_1);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quivk brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lazy\"",
        "\"the quick brown fox jumps over the\"",
        "\"the quick brown fox jumps over\"",
        "\"the quick brown fox jumps over the lackadaisical dog\"",
        "\"the quick brown fox jumps\"",
        "\"the quick brown fox\"",
        "\"the quick brown foxes jump over the lazy dog\"",
        "\"the quick brown fax sends a letter to the dog\"",
        "\"the quick brown\"",
        "\"the quick\"",
        "\"a fox doesn't quack, that crown goes to the duck.\"",
        "\"the quickest brownest fox jumps over the laziest dog\"",
        "\"the quicker browner fox jumped over the lazier dog\"",
        "\"the extravagant fox skyrocketed over the languorous dog\"",
        "\"the fast brownish fox jumps over the lackadaisical dog\"",
    ]
    "###);

    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Words, Criterion::Typo]);
        })
        .unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::Last);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the quick brown fox jumps over the lazy dog");
    let SearchResult { documents_ids: ids_2, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{ids_2:?}"), @"[0, 23, 7, 8, 9, 22, 10, 11, 1, 2, 12, 13, 4, 3, 5, 6, 21]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));

    assert_eq!(ids_1, ids_2);
}

#[test]
fn test_typo_bucketing() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    // First do the search with just the Words ranking rule
    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("network interconnection sunflower");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[14, 15, 16, 17, 18, 20]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"netwolk interconections sunflawar\"",
        "\"network interconnections sunflawer\"",
        "\"network interconnection sunflower\"",
        "\"network interconnection sun flower\"",
        "\"network interconnection sunflowering\"",
        "\"network interconnection sunflowar\"",
    ]
    "###);

    // Then with the typo ranking rule
    drop(txn);
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Typo]);
        })
        .unwrap();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("network interconnection sunflower");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[16, 18, 17, 20, 15, 14]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"network interconnection sunflower\"",
        "\"network interconnection sunflowering\"",
        "\"network interconnection sun flower\"",
        "\"network interconnection sunflowar\"",
        "\"network interconnections sunflawer\"",
        "\"netwolk interconections sunflawar\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("network interconnection sun flower");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[17, 19, 16, 18, 20, 15]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"network interconnection sun flower\"",
        "\"network interconnection sun flowering\"",
        "\"network interconnection sunflower\"",
        "\"network interconnection sunflowering\"",
        "\"network interconnection sunflowar\"",
        "\"network interconnections sunflawer\"",
    ]
    "###);
}

#[test]
fn test_typo_synonyms() {
    let index = create_index();
    index
        .update_settings(|s| {
            s.set_criteria(vec![Criterion::Typo]);

            let mut synonyms = BTreeMap::new();
            synonyms.insert("lackadaisical".to_owned(), vec!["lazy".to_owned()]);
            synonyms.insert("fast brownish".to_owned(), vec!["quick brown".to_owned()]);

            s.set_synonyms(synonyms);
        })
        .unwrap();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the quick brown fox jumps over the lackadaisical dog");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 22, 23]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lackadaisical dog\"",
        "\"the quivk brown fox jumps over the lazy dog\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("the fast brownish fox jumps over the lackadaisical dog");

    // The interaction of ngrams + synonyms means that the multi-word synonyms end up having a typo cost.
    // This is probably not what we want.
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[21, 0, 22]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the fast brownish fox jumps over the lackadaisical dog\"",
        "\"the quick brown fox jumps over the lazy dog\"",
        "\"the quick brown fox jumps over the lackadaisical dog\"",
    ]
    "###);
}
