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
        expected_maybe_text: Option<&str>,
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

        assert_eq!(
            matcher.get_formatted_text(format_options),
            expected_maybe_text.map(|v| v.to_string())
        );
    }

    struct FormatVariations<'a> {
        highlight_with_crop: Option<&'a str>,
        highlight: Option<&'a str>,
        crop: Option<&'a str>,
    }

    impl<'a> FormatVariations<'a> {
        fn get(&self) -> [(Option<FormatOptions>, Option<&'a str>); 5] {
            [
                (None, None),
                (Some(FormatOptions { highlight: true, crop: Some(2) }), self.highlight_with_crop),
                (Some(FormatOptions { highlight: true, crop: None }), self.highlight),
                (Some(FormatOptions { highlight: false, crop: Some(2) }), self.crop),
                (Some(FormatOptions { highlight: false, crop: None }), None),
            ]
        }
    }

    /// "Dei store fiskane eta dei små — dei liger under som minst förmå."
    ///
    /// (Men are like fish; the great ones devour the small.)
    fn rename_me_with_base_text(
        format_options: Option<FormatOptions>,
        query: &str,
        expected_maybe_text: Option<&str>,
    ) {
        rename_me(
            format_options,
            "Dei store fiskane eta dei små — dei liger under som minst förmå.",
            query,
            expected_maybe_text,
        );
    }

    #[test]
    fn empty_query() {
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("Dei store…"),
            highlight: None,
            crop: Some("Dei store…"),
        }
        .get())
        {
            rename_me_with_base_text(format_options, "", expected_maybe_text);
        }
    }

    #[test]
    fn only_separators() {
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some(":-…"),
            highlight: None,
            crop: Some(":-…"),
        }
        .get())
        {
            rename_me(format_options, ":-)", ":-)", expected_maybe_text);
        }
    }

    #[test]
    fn highlight_end() {
        // TODO: Why is "förmå" marked as prefix in located matching words?
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("…<em>minst</em> <em>förmå</em>."),
            highlight: Some("Dei store fiskane eta dei små — dei liger under som <em>minst</em> <em>förmå</em>."),
            crop: Some("…minst förmå."),
        }
        .get()) {
            rename_me_with_base_text(format_options, "minst förmå", expected_maybe_text);
        }
    }

    #[test]
    fn highlight_beginning_and_middle() {
        // TODO: Why is "store" marked as prefix in located matching words?
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("<em>Dei</em> <em>store</em>…"),
            highlight: Some("<em>Dei</em> <em>store</em> fiskane eta <em>dei</em> små — <em>dei</em> liger under som minst förmå."),
            crop: Some("Dei store…"),
        }
        .get()) {
            rename_me_with_base_text(format_options, "Dei store", expected_maybe_text);
        }
    }

    #[test]
    fn partial_match_middle() {
        // TODO: Is this intentional?
        // Here the only interned word is "forma", hence it cannot find the searched prefix
        // word "fo" inside "forma" within milli::search::new::matches::matching_words::MatchingWords::try_get_word_match
        // `milli::search::new::query_term::QueryTerm::all_computed_derivations` might be at fault here

        // interned words = ["forma"]
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("…<em>förmå</em>, på…"),
            highlight: Some("altså, <em>förmå</em>, på en måte"),
            crop: Some("…förmå, på…"),
        }
        .get())
        {
            rename_me(format_options, "altså, förmå, på en måte", "fo", expected_maybe_text);
        }

        // interned words = ["fo", "forma"]
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("…<em>fo</em> <em>fö</em>rmå…"),
            highlight: Some("altså, <em>fo</em> <em>fö</em>rmå, på en måte"),
            crop: Some("…fo förmå…"),
        }
        .get())
        {
            rename_me(format_options, "altså, fo förmå, på en måte", "fo", expected_maybe_text);
        }
    }

    #[test]
    fn partial_match_end() {
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("<em>förmå</em>, på…"),
            highlight: Some("<em>förmå</em>, på en måte"),
            crop: Some("förmå, på…"),
        }
        .get())
        {
            rename_me(format_options, "förmå, på en måte", "fo", expected_maybe_text);
        }

        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("<em>fo</em> <em>fö</em>rmå…"),
            highlight: Some("<em>fo</em> <em>fö</em>rmå, på en måte"),
            crop: Some("fo förmå…"),
        }
        .get())
        {
            rename_me(format_options, "fo förmå, på en måte", "fo", expected_maybe_text);
        }
    }

    #[test]
    fn partial_match_beginning() {
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("altså, <em>förmå</em>"),
            highlight: Some("altså, <em>förmå</em>"),
            crop: Some("altså, förmå"),
        }
        .get())
        {
            rename_me(format_options, "altså, förmå", "fo", expected_maybe_text);
        }

        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("…<em>fo</em> <em>fö</em>rmå"),
            highlight: Some("altså, <em>fo</em> <em>fö</em>rmå"),
            crop: Some("…fo förmå"),
        }
        .get())
        {
            rename_me(format_options, "altså, fo förmå", "fo", expected_maybe_text);
        }
    }

    #[test]
    fn separator_at_end() {
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("…<em>minst</em> förmå. , ;"),
            highlight: Some("; , — dei liger under som <em>minst</em> förmå. , ;"),
            crop: Some("…minst förmå. , ;"),
        }
        .get())
        {
            rename_me(
                format_options,
                "; , — dei liger under som minst förmå. , ;",
                "minst",
                expected_maybe_text,
            );
        }
    }

    #[test]
    fn separator_at_beginning() {
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("; , — <em>dei</em> liger…"),
            highlight: Some("; , — <em>dei</em> liger under som minst förmå. , ;"),
            crop: Some("; , — dei liger…"),
        }
        .get())
        {
            rename_me(
                format_options,
                "; , — dei liger under som minst förmå. , ;",
                "dei",
                expected_maybe_text,
            );
        }
    }

    #[test]
    fn phrase() {
        for (format_options, expected_maybe_text) in (FormatVariations {
            highlight_with_crop: Some("…<em>dei liger</em>…"),
            highlight: Some(
                "Dei store fiskane eta dei små — <em>dei liger</em> under som minst förmå.",
            ),
            crop: Some("…dei liger…"),
        }
        .get())
        {
            rename_me_with_base_text(format_options, "\"dei liger\"", expected_maybe_text);
        }
    }

    #[test]
    fn phrase_highlight_bigger_than_crop() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(1) }),
            "\"dei liger\"",
            Some("…<em>dei</em>…"),
        );
    }

    #[test]
    fn phrase_bigger_than_crop() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: false, crop: Some(1) }),
            "\"dei liger\"",
            Some("…dei…"),
        );
    }

    #[test]
    fn phrase_highlight_crop_middle() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(4) }),
            "\"dei liger\"",
            Some("…små — <em>dei liger</em> under…"),
        );
    }

    #[test]
    fn phrase_crop_middle() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: false, crop: Some(4) }),
            "\"dei liger\"",
            Some("…små — dei liger under…"),
        );
    }

    #[test]
    fn phrase_highlight_crop_end() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(4) }),
            "\"minst förmå\"",
            Some("…under som <em>minst förmå</em>."),
        );
    }

    #[test]
    fn phrase_crop_end() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: false, crop: Some(4) }),
            "\"minst förmå\"",
            Some("…under som minst förmå."),
        );
    }

    #[test]
    fn phrase_highlight_crop_beginning() {
        rename_me_with_base_text(
            Some(FormatOptions { highlight: true, crop: Some(4) }),
            "\"Dei store\"",
            Some("<em>Dei store</em> fiskane eta…"),
        );
    }
}
