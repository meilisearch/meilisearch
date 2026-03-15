use charabia::TokenizerBuilder;

use crate::search::new::matches::matching_words::tests::temp_index_with_documents;
use crate::search::new::matches::*;
use crate::index::tests::TempIndex;
use crate::progress::Progress;
use crate::{execute_search, filtered_universe, Deadline, SearchContext};

impl<'a> MatcherBuilder<'a> {
    fn new_test(rtxn: &'a heed::RoTxn<'a>, index: &'a TempIndex, query: &str) -> Self {
        let progress = Progress::default();
        let mut ctx = SearchContext::new(index, rtxn).unwrap();
        let universe = filtered_universe(ctx.index, ctx.txn, &None, &progress).unwrap();
        let crate::search::PartialSearchResult { located_query_terms, .. } = execute_search(
            &mut ctx,
            Some(query),
            crate::TermsMatchingStrategy::default(),
            crate::score_details::ScoringStrategy::Skip,
            false,
            None,
            universe,
            &None,
            &None,
            crate::search::new::GeoSortParameter::default(),
            0,
            100,
            Some(10),
            &mut crate::DefaultSearchLogger,
            &mut crate::DefaultSearchLogger,
            Deadline::never(),
            None,
            None,
            &progress,
        )
        .unwrap();

        // consume context and located_query_terms to build MatchingWords.
        let matching_words = match located_query_terms {
            Some(located_query_terms) => MatchingWords::new(ctx, located_query_terms),
            None => MatchingWords::default(),
        };

        MatcherBuilder::new(matching_words, TokenizerBuilder::default().into_tokenizer())
    }
}

#[test]
fn format_identity() {
    let temp_index = temp_index_with_documents();
    let rtxn = temp_index.read_txn().unwrap();
    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");

    let format_options = FormatOptions { highlight: false, crop: None };

    // Text without any match.
    let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
    let mut matcher = builder.build(text, None);
    // no crop and no highlight should return complete text.
    assert_eq!(&matcher.format(format_options), &text);

    // Text containing all matches.
    let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
    let mut matcher = builder.build(text, None);
    // no crop and no highlight should return complete text.
    assert_eq!(&matcher.format(format_options), &text);

    // Text containing some matches.
    let text = "Natalie risk her future to build a world with the boy she loves.";
    let mut matcher = builder.build(text, None);
    // no crop and no highlight should return complete text.
    assert_eq!(&matcher.format(format_options), &text);
}

#[test]
fn format_highlight() {
    let temp_index = temp_index_with_documents();
    let rtxn = temp_index.read_txn().unwrap();
    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");

    let format_options = FormatOptions { highlight: true, crop: None };

    // empty text.
    let text = "";
    let mut matcher = builder.build(text, None);
    assert_eq!(&matcher.format(format_options), "");

    // text containing only separators.
    let text = ":-)";
    let mut matcher = builder.build(text, None);
    assert_eq!(&matcher.format(format_options), ":-)");

    // Text without any match.
    let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
    let mut matcher = builder.build(text, None);
    // no crop should return complete text, because there is no matches.
    assert_eq!(&matcher.format(format_options), &text);

    // Text containing all matches.
    let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
    let mut matcher = builder.build(text, None);
    // no crop should return complete text with highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>."
    );

    // Text containing some matches.
    let text = "Natalie risk her future to build a world with the boy she loves.";
    let mut matcher = builder.build(text, None);
    // no crop should return complete text with highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves."
    );
}

#[test]
fn highlight_unicode() {
    let temp_index = temp_index_with_documents();
    let rtxn = temp_index.read_txn().unwrap();
    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "world");
    let format_options = FormatOptions { highlight: true, crop: None };

    // Text containing prefix match.
    let text = "Ŵôřlḑôle";
    let mut matcher = builder.build(text, None);
    // no crop should return complete text with highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"<em>Ŵôřlḑ</em>ôle"
    );

    // Text containing unicode match.
    let text = "Ŵôřlḑ";
    let mut matcher = builder.build(text, None);
    // no crop should return complete text with highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"<em>Ŵôřlḑ</em>"
    );

    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "westfali");
    let format_options = FormatOptions { highlight: true, crop: None };

    // Text containing unicode match.
    let text = "Westfália";
    let mut matcher = builder.build(text, None);
    // no crop should return complete text with highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"<em>Westfáli</em>a"
    );
}

#[test]
fn format_crop() {
    let temp_index = temp_index_with_documents();
    let rtxn = temp_index.read_txn().unwrap();
    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");

    let format_options = FormatOptions { highlight: false, crop: Some(10) };

    // empty text.
    let text = "";
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @""
    );

    // text containing only separators.
    let text = ":-)";
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @":-)"
    );

    // Text without any match.
    let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
    let mut matcher = builder.build(text, None);
    // no highlight should return 10 first words with a marker at the end.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"A quick brown fox can not jump 32 feet, right…"
    );

    // Text without any match starting by a separator.
    let text = "(A quick brown fox can not jump 32 feet, right? Brr, it is cold!)";
    let mut matcher = builder.build(text, None);
    // no highlight should return 10 first words with a marker at the end.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"(A quick brown fox can not jump 32 feet, right…"
    );

    // Test phrase propagation
    let text = "Natalie risk her future. Split The World is a book written by Emily Henry. I never read it.";
    let mut matcher = builder.build(text, None);
    // should crop the phrase instead of croping around the match.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…Split The World is a book written by Emily Henry…"
    );

    // Text containing some matches.
    let text = "Natalie risk her future to build a world with the boy she loves.";
    let mut matcher = builder.build(text, None);
    // no highlight should return 10 last words with a marker at the start.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…future to build a world with the boy she loves…"
    );

    // Text containing all matches.
    let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
    let mut matcher = builder.build(text, None);
    // no highlight should return 10 last words with a marker at the start.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…she loves. Emily Henry: The Love That Split The World."
    );

    // Text containing a match unordered and a match ordered.
    let text = "The world split void void void void void void void void void split the world void void";
    let mut matcher = builder.build(text, None);
    // crop should return 10 last words with a marker at the start.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…void void void void void split the world void void"
    );

    // Text containing matches with different density.
    let text = "split void the void void world void void void void void void void void void void split the world void void";
    let mut matcher = builder.build(text, None);
    // crop should return 10 last words with a marker at the start.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…void void void void void split the world void void"
    );

    // Text containing matches with same word.
    let text = "split split split split split split void void void void void void void void void void split the world void void";
    let mut matcher = builder.build(text, None);
    // crop should return 10 last words with a marker at the start.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…void void void void void split the world void void"
    );
}

#[test]
fn format_highlight_crop() {
    let temp_index = temp_index_with_documents();
    let rtxn = temp_index.read_txn().unwrap();
    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");

    let format_options = FormatOptions { highlight: true, crop: Some(10) };

    // empty text.
    let text = "";
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @""
    );

    // text containing only separators.
    let text = ":-)";
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @":-)"
    );

    // Text without any match.
    let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
    let mut matcher = builder.build(text, None);
    // both should return 10 first words with a marker at the end.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"A quick brown fox can not jump 32 feet, right…"
    );

    // Text containing some matches.
    let text = "Natalie risk her future to build a world with the boy she loves.";
    let mut matcher = builder.build(text, None);
    // both should return 10 last words with a marker at the start and highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…future to build a <em>world</em> with <em>the</em> boy she loves…"
    );

    // Text containing all matches.
    let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
    let mut matcher = builder.build(text, None);
    // both should return 10 last words with a marker at the start and highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>."
    );

    // Text containing a match unordered and a match ordered.
    let text = "The world split void void void void void void void void void split the world void void";
    let mut matcher = builder.build(text, None);
    // crop should return 10 last words with a marker at the start.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…void void void void void <em>split</em> <em>the</em> <em>world</em> void void"
    );
}

#[test]
fn format_highlight_crop_phrase_query() {
    //! testing: https://github.com/meilisearch/meilisearch/issues/3975
    let temp_index = TempIndex::new();

    let text = "The groundbreaking invention had the power to split the world between those who embraced progress and those who resisted change!";
    temp_index
        .add_documents(documents!([
            { "id": 1, "text": text }
        ]))
        .unwrap();

    let rtxn = temp_index.read_txn().unwrap();

    let format_options = FormatOptions { highlight: true, crop: Some(10) };

    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "\"the world\"");
    let mut matcher = builder.build(text, None);
    // should return 10 words with a marker at the start as well the end, and the highlighted matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…the power to split <em>the world</em> between those who embraced…"
    );

    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "those \"and those\"");
    let mut matcher = builder.build(text, None);
    // should highlight "those" and the phrase "and those".
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…world between <em>those</em> who embraced progress <em>and those</em> who resisted…"
    );

    let builder = MatcherBuilder::new_test(
        &rtxn,
        &temp_index,
        "\"The groundbreaking invention had the power to split the world\"",
    );
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"<em>The groundbreaking invention had the power to split the world</em>…"
    );

    let builder = MatcherBuilder::new_test(
        &rtxn,
        &temp_index,
        "\"The groundbreaking invention had the power to split the world between those\"",
    );
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"<em>The groundbreaking invention had the power to split the world</em>…"
    );

    let builder = MatcherBuilder::new_test(
        &rtxn,
        &temp_index,
        "\"The groundbreaking invention\" \"embraced progress and those who resisted change!\"",
    );
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        // TODO: Should include exclamation mark without crop markers
        @"…between those who <em>embraced progress and those who resisted change</em>…"
    );

    let builder = MatcherBuilder::new_test(
        &rtxn,
        &temp_index,
        "\"groundbreaking invention\" \"split the world between\"",
    );
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…<em>groundbreaking invention</em> had the power to <em>split the world between</em>…"
    );

    let builder = MatcherBuilder::new_test(
        &rtxn,
        &temp_index,
        "\"groundbreaking invention\" \"had the power to split the world between those\"",
    );
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…<em>invention</em> <em>had the power to split the world between those</em>…"
    );
}

#[test]
fn smaller_crop_size() {
    //! testing: https://github.com/meilisearch/specifications/pull/120#discussion_r836536295
    let temp_index = temp_index_with_documents();
    let rtxn = temp_index.read_txn().unwrap();
    let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");

    let text = "void void split the world void void.";

    // set a smaller crop size
    let format_options = FormatOptions { highlight: false, crop: Some(2) };
    let mut matcher = builder.build(text, None);
    // because crop size < query size, partially format matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…split the…"
    );

    // set a smaller crop size
    let format_options = FormatOptions { highlight: false, crop: Some(1) };
    let mut matcher = builder.build(text, None);
    // because crop size < query size, partially format matches.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"…split…"
    );

    // set  crop size to 0
    let format_options = FormatOptions { highlight: false, crop: Some(0) };
    let mut matcher = builder.build(text, None);
    // because crop size is 0, crop is ignored.
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"void void split the world void void."
    );
}

#[test]
fn partial_matches() {
    let temp_index = temp_index_with_documents();
    let rtxn = temp_index.read_txn().unwrap();
    let mut builder =
        MatcherBuilder::new_test(&rtxn, &temp_index, "the \"t he\" door \"do or\"");
    builder.highlight_prefix("_".to_string());
    builder.highlight_suffix("_".to_string());

    let format_options = FormatOptions { highlight: true, crop: None };

    let text = "the do or die can't be he do and or isn't he";
    let mut matcher = builder.build(text, None);
    insta::assert_snapshot!(
        matcher.format(format_options),
        @"_the_ _do or_ die can't be he do and or isn'_t he_"
    );
}
