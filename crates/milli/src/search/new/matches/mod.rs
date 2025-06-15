mod adjust_indices;
mod best_match_range;
mod r#match;
mod match_bounds;
mod matching_words;

use charabia::{Language, Token, Tokenizer};
pub use match_bounds::MatchBounds;
pub use matching_words::MatchingWords;
use matching_words::QueryPosition;
use r#match::Match;

pub struct MarkerOptions {
    pub highlight_pre_tag: String,
    pub highlight_post_tag: String,
    pub crop_marker: String,
}

/// Structure used to build a Matcher allowing to customize formatting tags.
pub struct MatcherBuilder<'a> {
    matching_words: MatchingWords,
    tokenizer: Tokenizer<'a>,
    marker_options: MarkerOptions,
}

impl<'a> MatcherBuilder<'a> {
    pub fn new(
        matching_words: MatchingWords,
        tokenizer: Tokenizer<'a>,
        marker_options: MarkerOptions,
    ) -> Self {
        Self { matching_words, tokenizer, marker_options }
    }

    pub fn build<'t, 'lang>(
        &self,
        text: &'t str,
        locales: Option<&'lang [Language]>,
    ) -> Matcher<'t, 'a, '_, 'lang> {
        Matcher {
            text,
            matching_words: &self.matching_words,
            tokenizer: &self.tokenizer,
            marker_options: &self.marker_options,
            tokens_matches_and_query_positions: None,
            locales,
        }
    }
}

#[derive(Copy, Clone, Default)]
pub struct FormatOptions {
    pub highlight: bool,
    pub crop: Option<usize>,
}

impl FormatOptions {
    pub fn merge(self, other: Self) -> Self {
        Self { highlight: self.highlight || other.highlight, crop: self.crop.or(other.crop) }
    }

    pub fn should_format(&self) -> bool {
        self.highlight || self.crop.is_some()
    }
}

/// Structure used to analyze a string, compute words that match,
/// and format the source string, returning a highlighted and cropped sub-string.
pub struct Matcher<'t, 'tokenizer, 'b, 'lang> {
    text: &'t str,
    matching_words: &'b MatchingWords,
    tokenizer: &'b Tokenizer<'tokenizer>,
    locales: Option<&'lang [Language]>,
    marker_options: &'b MarkerOptions,
    tokens_matches_and_query_positions: Option<((Vec<Match>, Vec<QueryPosition>), Vec<Token<'t>>)>,
}

impl Matcher<'_, '_, '_, '_> {
    /// TODO: description
    pub fn get_match_bounds(
        &mut self,
        // TODO: Add option to count UTF-16 segments, or whatever JS works with when slicing strings
        // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String#utf-16_characters_unicode_code_points_and_grapheme_clusters
        // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/slice
        format_options: Option<FormatOptions>,
    ) -> Option<MatchBounds> {
        if self.text.is_empty() {
            return None;
        }

        let ((matches, query_positions), tokens) =
            self.tokens_matches_and_query_positions.get_or_insert_with(|| {
                let tokens = self
                    .tokenizer
                    .tokenize_with_allow_list(self.text, self.locales)
                    .collect::<Vec<_>>();

                (self.matching_words.get_matches_and_query_positions(&tokens, self.text), tokens)
            });

        MatchBounds::try_new(tokens, matches, query_positions, format_options.unwrap_or_default())
    }

    pub fn get_formatted_text(&mut self, format_options: Option<FormatOptions>) -> Option<String> {
        let MatchBounds { mut highlight_toggle, ref indices } =
            self.get_match_bounds(format_options)?;

        let MarkerOptions { highlight_pre_tag, highlight_post_tag, crop_marker } =
            &self.marker_options;

        let mut formatted_text = Vec::new();

        let mut indices_iter = indices.iter();
        let mut previous_index = indices_iter.next().expect("TODO");

        // push crop marker if it's not the start of the text
        if !crop_marker.is_empty() && *previous_index != 0 {
            formatted_text.push(crop_marker.as_str());
        }

        for index in indices_iter {
            if highlight_toggle {
                formatted_text.push(highlight_pre_tag.as_str());
            }

            formatted_text.push(&self.text[*previous_index..*index]);

            if highlight_toggle {
                formatted_text.push(highlight_post_tag.as_str());
            }

            highlight_toggle = !highlight_toggle;
            previous_index = index;
        }

        // push crop marker if it's not the end of the text
        if !crop_marker.is_empty() && *previous_index < self.text.len() {
            formatted_text.push(crop_marker.as_str());
        }

        if formatted_text.len() == 1 {
            // avoid concatenating if there is only one element
            return Some(formatted_text[0].to_string());
        }

        Some(formatted_text.concat())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::tests::TempIndex;
    use crate::{execute_search, filtered_universe, SearchContext, TimeBudget};
    use charabia::TokenizerBuilder;

    impl<'a> MatcherBuilder<'a> {
        fn new_test(rtxn: &'a heed::RoTxn<'a>, index: &'a TempIndex, query: &str) -> Self {
            let mut ctx = SearchContext::new(index, rtxn).unwrap();
            let universe = filtered_universe(ctx.index, ctx.txn, &None).unwrap();
            let crate::search::PartialSearchResult { located_query_terms, .. } = execute_search(
                &mut ctx,
                Some(query),
                crate::TermsMatchingStrategy::default(),
                crate::score_details::ScoringStrategy::Skip,
                false,
                universe,
                &None,
                &None,
                crate::search::new::GeoSortParameter::default(),
                0,
                100,
                Some(10),
                &mut crate::DefaultSearchLogger,
                &mut crate::DefaultSearchLogger,
                TimeBudget::max(),
                None,
                None,
            )
            .unwrap();

            // consume context and located_query_terms to build MatchingWords.
            let matching_words = located_query_terms
                .map(|located_query_terms| MatchingWords::new(ctx, &located_query_terms))
                .unwrap_or_default();

            MatcherBuilder::new(
                matching_words,
                TokenizerBuilder::default().into_tokenizer(),
                MarkerOptions {
                    highlight_pre_tag: "<em>".to_string(),
                    highlight_post_tag: "</em>".to_string(),
                    crop_marker: "…".to_string(),
                },
            )
        }
    }

    pub fn rename_me(
        format_options: Option<FormatOptions>,
        text: &str,
        query: &str,
        expected_text: &str,
    ) {
        let temp_index = TempIndex::new();

        // document will always contain the same exact text normally
        // TODO: Describe this better and ask if this is actually the case
        temp_index
            .add_documents(documents!([
                { "id": 1, "text": text.to_string() },
            ]))
            .unwrap();

        let rtxn = temp_index.read_txn().unwrap();
        let builder = MatcherBuilder::new_test(&rtxn, &temp_index, query);
        let mut matcher = builder.build(text, None);

        assert_eq!(matcher.get_formatted_text(format_options), Some(expected_text.to_string()));
    }

    /// "Dei store fiskane eta dei små — dei liger under som minst förmå."
    ///
    /// (Men are like fish; the great ones devour the small.)
    fn rename_me_with_base_text(
        format_options: Option<FormatOptions>,
        query: &str,
        expected_text: &str,
    ) {
        rename_me(
            format_options,
            "Dei store fiskane eta dei små — dei liger under som minst förmå.",
            query,
            expected_text,
        );
    }

    #[test]
    fn phrase_highlight_bigger_than_crop() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(1) }),
            "\"dei liger\"",
            "…<em>dei</em>…",
        );
    }

    #[test]
    fn phrase_highlight_same_size_as_crop() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(2) }),
            "\"dei liger\"",
            "…<em>dei liger</em>…",
        );
    }

    #[test]
    fn phrase_highlight_crop_middle() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(4) }),
            "\"dei liger\"",
            "…små — <em>dei liger</em> under…",
        );
    }

    #[test]
    fn phrase_highlight_crop_end() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(4) }),
            "\"minst förmå\"",
            "…under som <em>minst förmå</em>.",
        );
    }

    #[test]
    fn phrase_highlight_crop_beginning() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(4) }),
            "\"Dei store\"",
            "<em>Dei store</em> fiskane eta…",
        );
    }

    #[test]
    fn highlight_end() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: None }),
            "minst förmå",
            "Dei store fiskane eta dei små — dei liger under som <em>minst</em> <em>förmå</em>.",
        );
    }

    #[test]
    fn highlight_beginning_and_middle() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: None }),
            "Dei store",
            "<em>Dei</em> <em>store</em> fiskane eta <em>dei</em> små — <em>dei</em> liger under som minst förmå.",
        );
    }

    #[test]
    fn partial_match_middle() {
        // TODO: Is this intentional?
        // Here the only interned word is "forma", hence it cannot find the searched prefix
        // word "fo" inside "forma" within milli::search::new::matches::matching_words::MatchingWords::try_get_word_match
        // `milli::search::new::query_term::QueryTerm::all_computed_derivations` might be at fault here

        // interned words = ["forma"]
        rename_me(
            Some(FormatOptions { highlight: true, crop: None }),
            "altså, förmå, på en måte",
            "fo",
            "altså, <em>förmå</em>, på en måte",
        );

        // interned words = ["fo", "forma"]
        rename_me(
            Some(FormatOptions { highlight: true, crop: None }),
            "altså, fo förmå, på en måte",
            "fo",
            "altså, <em>fo</em> <em>fö</em>rmå, på en måte",
        );
    }

    #[test]
    fn partial_match_end() {
        rename_me(
            Some(FormatOptions { highlight: true, crop: None }),
            "förmå, på en måte",
            "fo",
            "<em>förmå</em>, på en måte",
        );

        rename_me(
            Some(FormatOptions { highlight: true, crop: None }),
            "fo förmå, på en måte",
            "fo",
            "<em>fo</em> <em>fö</em>rmå, på en måte",
        );
    }

    #[test]
    fn partial_match_beginning() {
        rename_me(
            Some(FormatOptions { highlight: true, crop: None }),
            "altså, förmå",
            "fo",
            "altså, <em>förmå</em>",
        );

        rename_me(
            Some(FormatOptions { highlight: true, crop: None }),
            "altså, fo förmå",
            "fo",
            "altså, <em>fo</em> <em>fö</em>rmå",
        );
    }

    // #[test]
    // fn format_identity() {
    //     let temp_index = temp_index_with_documents(None);
    //     let rtxn = temp_index.read_txn().unwrap();
    //     let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");
    //     let format_options = Some(FormatOptions { highlight: false, crop: None });

    //     let test_values = [
    //         // Text without any match.
    //         "A quick brown fox can not jump 32 feet, right? Brr, it is cold!",
    //         // Text containing all matches.
    //         "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.",
    //         // Text containing some matches.
    //         "Natalie risk her future to build a world with the boy she loves."
    //     ];

    //     for text in test_values {
    //         let mut matcher = builder.build(text, None);
    //         // no crop and no highlight should return complete text.
    //         assert_eq!(matcher.get_formatted_text(format_options), None);
    //     }
    // }

    // #[test]
    // fn format_highlight() {
    //     let temp_index = temp_index_with_documents(None);
    //     let rtxn = temp_index.read_txn().unwrap();
    //     let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");
    //     let format_options = Some(FormatOptions { highlight: true, crop: None });

    //     let test_values = [
    //         // empty text.
    //         ["", ""],
    //         // text containing only separators.
    //         [":-)", ":-)"],
    //         // Text without any match.
    //         ["A quick brown fox can not jump 32 feet, right? Brr, it is cold!",
    //          "A quick brown fox can not jump 32 feet, right? Brr, it is cold!"],
    //         // Text containing all matches.
    //         ["Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.",
    //          "Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>."],
    //         // Text containing some matches.
    //         ["Natalie risk her future to build a world with the boy she loves.",
    //          "Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves."],
    //     ];

    //     for [text, expected_text] in test_values {
    //         let mut matcher = builder.build(text, None);
    //         // no crop should return complete text with highlighted matches.
    //         assert_eq!(matcher.get_formatted_text(format_options), Some(expected_text.to_string()));
    //     }
    // }

    // #[test]
    // fn highlight_unicode() {
    //     let temp_index = temp_index_with_documents(None);
    //     let rtxn = temp_index.read_txn().unwrap();
    //     let format_options = Some(FormatOptions { highlight: true, crop: None });

    //     let test_values = [
    //         // Text containing prefix match.
    //         ["world", "Ŵôřlḑôle", "<em>Ŵôřlḑ</em>ôle"],
    //         // Text containing unicode match.
    //         ["world", "Ŵôřlḑ", "<em>Ŵôřlḑ</em>"],
    //         // Text containing unicode match.
    //         ["westfali", "Westfália", "<em>Westfáli</em>a"],
    //     ];

    //     for [query, text, expected_text] in test_values {
    //         let builder = MatcherBuilder::new_test(&rtxn, &temp_index, query);
    //         let mut matcher = builder.build(text, None);
    //         // no crop should return complete text with highlighted matches.
    //         assert_eq!(matcher.get_formatted_text(format_options), Some(expected_text.to_string()));
    //     }
    // }

    // #[test]
    // fn format_crop() {
    //     let temp_index = temp_index_with_documents(None);
    //     let rtxn = temp_index.read_txn().unwrap();
    //     let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");
    //     let format_options = Some(FormatOptions { highlight: false, crop: Some(10) });

    //     let test_values = [
    //         // empty text.
    //         // ["", ""],
    //         // text containing only separators.
    //         // [":-)", ":-)"],
    //         // Text without any match.
    //         ["A quick brown fox can not jump 32 feet, right? Brr, it is cold!",
    //          "A quick brown fox can not jump 32 feet, right…"],
    //         // Text without any match starting by a separator.
    //         ["(A quick brown fox can not jump 32 feet, right? Brr, it is cold!)",
    //          "(A quick brown fox can not jump 32 feet, right…" ],
    //         // Test phrase propagation
    //         ["Natalie risk her future. Split The World is a book written by Emily Henry. I never read it.",
    //          "…Split The World is a book written by Emily Henry…"],
    //         // Text containing some matches.
    //         ["Natalie risk her future to build a world with the boy she loves.",
    //          "…future to build a world with the boy she loves."],
    //         // Text containing all matches.
    //         ["Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.",
    //          "…she loves. Emily Henry: The Love That Split The World."],
    //         // Text containing a match unordered and a match ordered.
    //         ["The world split void void void void void void void void void split the world void void",
    //          "…void void void void void split the world void void"],
    //         // Text containing matches with different density.
    //         ["split void the void void world void void void void void void void void void void split the world void void",
    //          "…void void void void void split the world void void"],
    //         ["split split split split split split void void void void void void void void void void split the world void void",
    //          "…void void void void void split the world void void"]
    //     ];

    //     for [text, expected_text] in test_values {
    //         let mut matcher = builder.build(text, None);
    //         // no crop should return complete text with highlighted matches.
    //         assert_eq!(matcher.get_formatted_text(format_options), Some(expected_text.to_string()));
    //     }
    // }

    // #[test]
    // fn format_highlight_crop() {
    //     let temp_index = temp_index_with_documents(None);
    //     let rtxn = temp_index.read_txn().unwrap();
    //     let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");
    //     let format_options = Some(FormatOptions { highlight: true, crop: Some(10) });

    //     let test_values = [
    //         // empty text.
    //         ["", ""],
    //         // text containing only separators.
    //         [":-)", ":-)"],
    //         // Text without any match.
    //         ["A quick brown fox can not jump 32 feet, right? Brr, it is cold!",
    //          "A quick brown fox can not jump 32 feet, right…"],
    //         // Text containing some matches.
    //         ["Natalie risk her future to build a world with the boy she loves.",
    //          "…future to build a <em>world</em> with <em>the</em> boy she loves."],
    //         // Text containing all matches.
    //         ["Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.",
    //          "…she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>."],
    //         // Text containing a match unordered and a match ordered.
    //         ["The world split void void void void void void void void void split the world void void",
    //          "…void void void void void <em>split</em> <em>the</em> <em>world</em> void void"]
    //     ];

    //     for [text, expected_text] in test_values {
    //         let mut matcher = builder.build(text, None);
    //         // no crop should return complete text with highlighted matches.
    //         assert_eq!(matcher.get_formatted_text(format_options), Some(expected_text.to_string()));
    //     }
    // }

    // #[test]
    // fn format_highlight_crop_phrase_query() {
    //     //! testing: https://github.com/meilisearch/meilisearch/issues/3975
    //     let text = "The groundbreaking invention had the power to split the world between those who embraced progress and those who resisted change!";
    //     let temp_index = temp_index_with_documents(Some(documents!([
    //         { "id": 1, "text": text }
    //     ])));
    //     let rtxn = temp_index.read_txn().unwrap();

    //     let format_options = Some(FormatOptions { highlight: true, crop: Some(10) });

    //     let test_values = [
    //         // should return 10 words with a marker at the start as well the end, and the highlighted matches.
    //         ["\"the world\"",
    //          "…the power to split <em>the world</em> between those who embraced…"],
    //         // should highlight "those" and the phrase "and those".
    //         ["those \"and those\"",
    //          "…world between <em>those</em> who embraced progress <em>and those</em> who resisted…"],
    //         ["\"The groundbreaking invention had the power to split the world\"",
    //          "<em>The groundbreaking invention had the power to split the world</em>…"],
    //         ["\"The groundbreaking invention had the power to split the world between those\"",
    //          "<em>The groundbreaking invention had the power to split the world</em>…"],
    //         ["\"The groundbreaking invention\" \"embraced progress and those who resisted change!\"",
    //          "…between those who <em>embraced progress and those who resisted change</em>!"],
    //         ["\"groundbreaking invention\" \"split the world between\"",
    //          "…<em>groundbreaking invention</em> had the power to <em>split the world between</em>…"],
    //         ["\"groundbreaking invention\" \"had the power to split the world between those\"",
    //          "…<em>invention</em> <em>had the power to split the world between those</em>…"],
    //     ];

    //     for [query, expected_text] in test_values {
    //         let builder = MatcherBuilder::new_test(&rtxn, &temp_index, query);
    //         let mut matcher = builder.build(text, None);

    //         assert_eq!(matcher.get_formatted_text(format_options), Some(expected_text.to_string()));
    //     }
    // }

    // #[test]
    // fn smaller_crop_size() {
    //     //! testing: https://github.com/meilisearch/specifications/pull/120#discussion_r836536295
    //     let temp_index = temp_index_with_documents(None);
    //     let rtxn = temp_index.read_txn().unwrap();
    //     let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "split the world");
    //     let text = "void void split the world void void.";
    //     let mut matcher = builder.build(text, None);

    //     let test_values = [
    //         // set a smaller crop size
    //         // because crop size < query size, partially format matches.
    //         (2, "…split the…"),
    //         // set a smaller crop size
    //         // because crop size < query size, partially format matches.
    //         (1, "…split…"),
    //         // set  crop size to 0
    //         // because crop size is 0, crop is ignored.
    //         (0, "void void split the world void void."),
    //     ];

    //     for (crop_size, expected_text) in test_values {
    //         // set a smaller crop size
    //         let format_options = Some(FormatOptions { highlight: false, crop: Some(crop_size) });
    //         assert_eq!(matcher.get_formatted_text(format_options), Some(expected_text.to_string()));
    //     }
    // }

    // #[test]
    // fn partial_matches() {
    //     let temp_index = temp_index_with_documents(None);
    //     let rtxn = temp_index.read_txn().unwrap();
    //     let builder = MatcherBuilder::new_test(&rtxn, &temp_index, "the \"t he\" door \"do or\"");

    //     let format_options = Some(FormatOptions { highlight: true, crop: None });

    //     let text = "the do or die can't be he do and or isn't he";
    //     let mut matcher = builder.build(text, None);
    //     assert_eq!(
    //         matcher.get_formatted_text(format_options),
    //         Some(
    //             "<em>the</em> <em>do or</em> die can't be he do and or isn'<em>t he</em>"
    //                 .to_string()
    //         )
    //     );
    // }
}
