/*!
This module tests the following properties:

1. Two consecutive words from a query can be combined into a "2gram"
2. Three consecutive words from a query can be combined into a "3gram"
3. A word from the query can be split into two consecutive words (split words), no matter how short it is
4. A 2gram can be split into two words
5. A 3gram can be split into two words
6. 2grams can contain up to 1 typo
7. 3grams cannot have typos
8. 2grams and 3grams can be prefix tolerant
9. Disabling typo tolerance also disable the split words feature
10. Disabling typo tolerance does not disable prefix tolerance
11. Disabling typo tolerance does not disable ngram tolerance
12. Prefix tolerance is disabled for the last word if a space follows it
13. Ngrams cannot be formed by combining a phrase and a word or two phrases
14. Split words are not disabled by the `disableOnAttribute` or `disableOnWords` typo settings
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
            },
            {
                "id": 4,
                "text": "the sunflawer is tall"
            },
            {
                "id": 5,
                "text": "sunflowering is not a verb"
            },
            {
                "id": 6,
                "text": "xy z"
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
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.query("sun flower");
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    // will also match documents with "sunflower" + prefix tolerance
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3, 5]");
    // scores are empty because the only rule is Words with All matching strategy
    insta::assert_snapshot!(format!("{document_scores:?}"), @"[[], [], [], [], []]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flowers are pretty\"",
        "\"the sun flower is tall\"",
        "\"the sunflowers are pretty\"",
        "\"the sunflower is tall\"",
        "\"sunflowering is not a verb\"",
    ]
    "###);
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
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flowers are pretty\"",
        "\"the sunflowers are pretty\"",
    ]
    "###);
}

#[test]
fn test_2gram_typo() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sun flawer");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3, 4, 5]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flowers are pretty\"",
        "\"the sun flower is tall\"",
        "\"the sunflowers are pretty\"",
        "\"the sunflower is tall\"",
        "\"the sunflawer is tall\"",
        "\"sunflowering is not a verb\"",
    ]
    "###);
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
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flower is tall\"",
        "\"the sunflower is tall\"",
    ]
    "###);
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
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2, 3, 5]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flowers are pretty\"",
        "\"the sun flower is tall\"",
        "\"the sunflowers are pretty\"",
        "\"the sunflower is tall\"",
        "\"sunflowering is not a verb\"",
    ]
    "###);
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
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[2, 3, 4, 5]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sunflowers are pretty\"",
        "\"the sunflower is tall\"",
        "\"the sunflawer is tall\"",
        "\"sunflowering is not a verb\"",
    ]
    "###);
}

#[test]
fn test_split_words() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunflower ");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // all the documents with either `sunflower` or `sun flower` + eventual typo
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 2, 3, 4]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flower is tall\"",
        "\"the sunflowers are pretty\"",
        "\"the sunflower is tall\"",
        "\"the sunflawer is tall\"",
    ]
    "###);
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
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 3]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flower is tall\"",
        "\"the sunflower is tall\"",
    ]
    "###);
}

#[test]
fn test_2gram_split_words() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunf lower");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    // all the documents with "sunflower", "sun flower", (sunflower + 1 typo), or (sunflower as prefix)
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 2, 3, 4, 5]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flower is tall\"",
        "\"the sunflowers are pretty\"",
        "\"the sunflower is tall\"",
        "\"the sunflawer is tall\"",
        "\"sunflowering is not a verb\"",
    ]
    "###);
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
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 2, 3, 5]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flower is tall\"",
        "\"the sunflowers are pretty\"",
        "\"the sunflower is tall\"",
        "\"sunflowering is not a verb\"",
    ]
    "###);
}

#[test]
fn test_3gram_no_typos() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("sunf la wer");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[4]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sunflawer is tall\"",
    ]
    "###);
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
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flowers are pretty\"",
        "\"the sun flower is tall\"",
    ]
    "###);

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("\"sun\" \"flower\"");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flower is tall\"",
    ]
    "###);
}

#[test]
fn test_short_split_words() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("xyz");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"xy z\"",
    ]
    "###);
}

#[test]
fn test_split_words_never_disabled() {
    let index = create_index();

    index
        .update_settings(|s| {
            s.set_exact_words(["sunflower"].iter().map(ToString::to_string).collect());
            s.set_exact_attributes(["text"].iter().map(ToString::to_string).collect());
        })
        .unwrap();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the sunflower is tall");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();

    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 3]");
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"the sun flower is tall\"",
        "\"the sunflower is tall\"",
    ]
    "###);
}
