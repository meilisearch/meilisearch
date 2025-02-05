mod best_match_interval;
mod r#match;
mod matching_words;
mod simple_token_kind;

use std::borrow::Cow;
use std::cmp::{max, min};

use charabia::{Language, SeparatorKind, Token, Tokenizer};
use either::Either;
pub use matching_words::MatchingWords;
use matching_words::{MatchType, PartialMatch};
use r#match::{Match, MatchPosition};
use serde::{Deserialize, Serialize};
use simple_token_kind::SimpleTokenKind;
use utoipa::ToSchema;

const DEFAULT_CROP_MARKER: &str = "…";
const DEFAULT_HIGHLIGHT_PREFIX: &str = "<em>";
const DEFAULT_HIGHLIGHT_SUFFIX: &str = "</em>";

/// Structure used to build a Matcher allowing to customize formatting tags.
pub struct MatcherBuilder<'m> {
    matching_words: MatchingWords,
    tokenizer: Tokenizer<'m>,
    crop_marker: Option<String>,
    highlight_prefix: Option<String>,
    highlight_suffix: Option<String>,
}

impl<'m> MatcherBuilder<'m> {
    pub fn new(matching_words: MatchingWords, tokenizer: Tokenizer<'m>) -> Self {
        Self {
            matching_words,
            tokenizer,
            crop_marker: None,
            highlight_prefix: None,
            highlight_suffix: None,
        }
    }

    pub fn crop_marker(&mut self, marker: String) -> &Self {
        self.crop_marker = Some(marker);
        self
    }

    pub fn highlight_prefix(&mut self, prefix: String) -> &Self {
        self.highlight_prefix = Some(prefix);
        self
    }

    pub fn highlight_suffix(&mut self, suffix: String) -> &Self {
        self.highlight_suffix = Some(suffix);
        self
    }

    pub fn build<'t, 'lang>(
        &self,
        text: &'t str,
        locales: Option<&'lang [Language]>,
    ) -> Matcher<'t, 'm, '_, 'lang> {
        let crop_marker = match &self.crop_marker {
            Some(marker) => marker.as_str(),
            None => DEFAULT_CROP_MARKER,
        };

        let highlight_prefix = match &self.highlight_prefix {
            Some(marker) => marker.as_str(),
            None => DEFAULT_HIGHLIGHT_PREFIX,
        };
        let highlight_suffix = match &self.highlight_suffix {
            Some(marker) => marker.as_str(),
            None => DEFAULT_HIGHLIGHT_SUFFIX,
        };
        Matcher {
            text,
            matching_words: &self.matching_words,
            tokenizer: &self.tokenizer,
            crop_marker,
            highlight_prefix,
            highlight_suffix,
            matches: None,
            locales,
        }
    }
}

#[derive(Copy, Clone, Default, Debug)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, ToSchema)]
pub struct MatchBounds {
    pub start: usize,
    pub length: usize,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub indices: Option<Vec<usize>>,
}

/// Structure used to analyze a string, compute words that match,
/// and format the source string, returning a highlighted and cropped sub-string.
pub struct Matcher<'t, 'tokenizer, 'b, 'lang> {
    text: &'t str,
    matching_words: &'b MatchingWords,
    tokenizer: &'b Tokenizer<'tokenizer>,
    locales: Option<&'lang [Language]>,
    crop_marker: &'b str,
    highlight_prefix: &'b str,
    highlight_suffix: &'b str,
    matches: Option<(Vec<Token<'t>>, Vec<Match>)>,
}

impl<'t, 'tokenizer> Matcher<'t, 'tokenizer, '_, '_> {
    /// Iterates over tokens and save any of them that matches the query.
    fn compute_matches(&mut self) -> &mut Self {
        /// some words are counted as matches only if they are close together and in the good order,
        /// compute_partial_match peek into next words to validate if the match is complete.
        fn compute_partial_match<'a>(
            mut partial: PartialMatch<'a>,
            first_token_position: usize,
            first_word_position: usize,
            first_word_char_start: &usize,
            words_positions: &mut impl Iterator<Item = (usize, usize, &'a Token<'a>)>,
            matches: &mut Vec<Match>,
        ) -> bool {
            for (token_position, word_position, word) in words_positions {
                partial = match partial.match_token(word) {
                    // token matches the partial match, but the match is not full,
                    // we temporarily save the current token then we try to match the next one.
                    Some(MatchType::Partial(partial)) => partial,
                    // partial match is now full, we keep this matches and we advance positions
                    Some(MatchType::Full { ids, .. }) => {
                        // save the token that closes the partial match as a match.
                        matches.push(Match {
                            char_count: word.char_end - *first_word_char_start,
                            ids: ids.clone().collect(),
                            position: MatchPosition::Phrase {
                                word_positions: [first_word_position, word_position],
                                token_positions: [first_token_position, token_position],
                            },
                        });

                        // the match is complete, we return true.
                        return true;
                    }
                    // no match, continue to next match.
                    None => break,
                };
            }

            // the match is not complete, we return false.
            false
        }

        let tokens: Vec<_> =
            self.tokenizer.tokenize_with_allow_list(self.text, self.locales).collect();
        let mut matches = Vec::new();

        let mut words_positions = tokens
            .iter()
            .scan((0, 0), |(token_position, word_position), token| {
                let current_token_position = *token_position;
                let current_word_position = *word_position;
                *token_position += 1;
                if !token.is_separator() {
                    *word_position += 1;
                }

                Some((current_token_position, current_word_position, token))
            })
            .filter(|(_, _, token)| !token.is_separator());

        while let Some((token_position, word_position, word)) = words_positions.next() {
            for match_type in self.matching_words.match_token(word) {
                match match_type {
                    // we match, we save the current token as a match,
                    // then we continue the rest of the tokens.
                    MatchType::Full { ids, char_count, .. } => {
                        let ids: Vec<_> = ids.clone().collect();
                        matches.push(Match {
                            char_count,
                            ids,
                            position: MatchPosition::Word { word_position, token_position },
                        });
                        break;
                    }
                    // we match partially, iterate over next tokens to check if we can complete the match.
                    MatchType::Partial(partial) => {
                        // if match is completed, we break the matching loop over the current token,
                        // then we continue the rest of the tokens.
                        let mut wp = words_positions.clone();
                        if compute_partial_match(
                            partial,
                            token_position,
                            word_position,
                            &word.char_start,
                            &mut wp,
                            &mut matches,
                        ) {
                            words_positions = wp;
                            break;
                        }
                    }
                }
            }
        }

        self.matches = Some((tokens, matches));
        self
    }

    /// Returns boundaries of the words that match the query.
    pub fn matches(&mut self, array_indices: &[usize]) -> Vec<MatchBounds> {
        match &self.matches {
            None => self.compute_matches().matches(array_indices),
            Some((tokens, matches)) => matches
                .iter()
                .map(|m| MatchBounds {
                    start: tokens[m.get_first_token_pos()].byte_start,
                    // TODO: Why is this in chars, while start is in bytes?
                    length: m.char_count,
                    indices: if array_indices.is_empty() {
                        None
                    } else {
                        Some(array_indices.to_owned())
                    },
                })
                .collect(),
        }
    }

    /// Returns the bounds in byte index of the crop window.
    fn crop_bounds(&self, tokens: &[Token<'_>], matches: &[Match], crop_size: usize) -> [usize; 2] {
        let (
            mut remaining_words,
            is_iterating_forward,
            before_tokens_starting_index,
            after_tokens_starting_index,
        ) = if !matches.is_empty() {
            let [matches_first, matches_last] =
                best_match_interval::find_best_match_interval(matches, crop_size);

            let matches_size =
                matches_last.get_last_word_pos() - matches_first.get_first_word_pos() + 1;

            let is_crop_size_gte_match_size = crop_size >= matches_size;
            let is_iterating_forward = matches_size == 0 || is_crop_size_gte_match_size;

            let remaining_words = if is_crop_size_gte_match_size {
                crop_size - matches_size
            } else {
                // in case matches size is greater than crop size, which implies there's only one match,
                // we count words backwards, because we have to remove words, as they're extra words outside of
                // crop window
                matches_size - crop_size
            };

            let after_tokens_starting_index = if matches_size == 0 {
                0
            } else {
                let last_match_last_token_position_plus_one = matches_last.get_last_token_pos() + 1;
                if last_match_last_token_position_plus_one < tokens.len() {
                    last_match_last_token_position_plus_one
                } else {
                    // we have matched the end of possible tokens, there's nothing to advance
                    tokens.len()
                }
            };

            (
                remaining_words,
                is_iterating_forward,
                if is_iterating_forward { matches_first.get_first_token_pos() } else { 0 },
                after_tokens_starting_index,
            )
        } else {
            (crop_size, true, 0, 0)
        };

        // create the initial state of the crop window: 2 iterators starting from the matches positions,
        // a reverse iterator starting from the first match token position and going towards the beginning of the text,
        let mut before_tokens = tokens[..before_tokens_starting_index].iter().rev().peekable();
        // an iterator ...
        let mut after_tokens = if is_iterating_forward {
            // ... starting from the last match token position and going towards the end of the text.
            Either::Left(tokens[after_tokens_starting_index..].iter().peekable())
        } else {
            // ... starting from the last match token position and going towards the start of the text.
            Either::Right(tokens[..=after_tokens_starting_index].iter().rev().peekable())
        };

        // grows the crop window peeking in both directions
        // until the window contains the good number of words:
        while remaining_words > 0 {
            let before_token_kind = before_tokens.peek().map(SimpleTokenKind::new);
            let after_token_kind =
                after_tokens.as_mut().either(|v| v.peek(), |v| v.peek()).map(SimpleTokenKind::new);

            match (before_token_kind, after_token_kind) {
                // we can expand both sides.
                (Some(before_token_kind), Some(after_token_kind)) => {
                    match (before_token_kind, after_token_kind) {
                        // if they are both separators and are the same kind then advance both,
                        // or expand in the soft separator separator side.
                        (
                            SimpleTokenKind::Separator(before_token_separator_kind),
                            SimpleTokenKind::Separator(after_token_separator_kind),
                        ) => {
                            if before_token_separator_kind == after_token_separator_kind {
                                before_tokens.next();

                                // this avoid having an ending separator before crop marker.
                                if remaining_words > 1 {
                                    after_tokens.next();
                                }
                            } else if matches!(before_token_separator_kind, SeparatorKind::Hard) {
                                after_tokens.next();
                            } else {
                                before_tokens.next();
                            }
                        }
                        // if one of the tokens is a word, we expend in the side of the word.
                        // left is a word, advance left.
                        (SimpleTokenKind::NotSeparator, SimpleTokenKind::Separator(_)) => {
                            before_tokens.next();
                            remaining_words -= 1;
                        }
                        // right is a word, advance right.
                        (SimpleTokenKind::Separator(_), SimpleTokenKind::NotSeparator) => {
                            after_tokens.next();
                            remaining_words -= 1;
                        }
                        // both are words, advance left then right if remaining_word > 0.
                        (SimpleTokenKind::NotSeparator, SimpleTokenKind::NotSeparator) => {
                            before_tokens.next();
                            remaining_words -= 1;

                            if remaining_words > 0 {
                                after_tokens.next();
                                remaining_words -= 1;
                            }
                        }
                    }
                }
                // the end of the text is reached, advance left.
                (Some(before_token_kind), None) => {
                    before_tokens.next();
                    if matches!(before_token_kind, SimpleTokenKind::NotSeparator) {
                        remaining_words -= 1;
                    }
                }
                // the start of the text is reached, advance right.
                (None, Some(after_token_kind)) => {
                    after_tokens.next();
                    if matches!(after_token_kind, SimpleTokenKind::NotSeparator) {
                        remaining_words -= 1;
                    }
                }
                // no more token to add.
                (None, None) => break,
            }
        }

        // finally, keep the byte index of each bound of the crop window.
        let crop_byte_start = before_tokens.next().map_or(0, |t| t.byte_end);
        let crop_byte_end = after_tokens.next().map_or(self.text.len(), |t| t.byte_start);

        [crop_byte_start, crop_byte_end]
    }

    // Returns the formatted version of the original text.
    pub fn format(&mut self, format_options: FormatOptions) -> Cow<'t, str> {
        if !format_options.highlight && format_options.crop.is_none() {
            // compute matches is not needed if no highlight nor crop is requested.
            Cow::Borrowed(self.text)
        } else {
            match &self.matches {
                Some((tokens, matches)) => {
                    // If the text has to be cropped, crop around the best interval.
                    let [crop_byte_start, crop_byte_end] = match format_options.crop {
                        Some(crop_size) if crop_size > 0 => {
                            self.crop_bounds(tokens, matches, crop_size)
                        }
                        _ => [0, self.text.len()],
                    };

                    let mut formatted = Vec::new();

                    // push crop marker if it's not the start of the text.
                    if crop_byte_start > 0 && !self.crop_marker.is_empty() {
                        formatted.push(self.crop_marker);
                    }

                    let mut byte_index = crop_byte_start;

                    if format_options.highlight {
                        // insert highlight markers around matches.
                        for m in matches {
                            let [m_byte_start, m_byte_end] = match m.position {
                                MatchPosition::Word { token_position, .. } => {
                                    let token = &tokens[token_position];
                                    [&token.byte_start, &token.byte_end]
                                }
                                MatchPosition::Phrase { token_positions: [ftp, ltp], .. } => {
                                    [&tokens[ftp].byte_start, &tokens[ltp].byte_end]
                                }
                            };

                            // skip matches out of the crop window
                            if *m_byte_end < crop_byte_start || *m_byte_start > crop_byte_end {
                                continue;
                            }

                            // adjust start and end to the crop window size
                            let [m_byte_start, m_byte_end] = [
                                max(m_byte_start, &crop_byte_start),
                                min(m_byte_end, &crop_byte_end),
                            ];

                            // push text that is positioned before our matches
                            if byte_index < *m_byte_start {
                                formatted.push(&self.text[byte_index..*m_byte_start]);
                            }

                            formatted.push(self.highlight_prefix);

                            // TODO: This is additional work done, charabia::token::Token byte_len
                            // should already get us the original byte length, however, that doesn't work as
                            // it's supposed to, investigate why
                            let highlight_byte_index = self.text[*m_byte_start..]
                                .char_indices()
                                .nth(m.char_count)
                                .map_or(*m_byte_end, |(i, _)| min(i + *m_byte_start, *m_byte_end));
                            formatted.push(&self.text[*m_byte_start..highlight_byte_index]);

                            formatted.push(self.highlight_suffix);

                            // if it's a prefix highlight, we put the end of the word after the highlight marker.
                            if highlight_byte_index < *m_byte_end {
                                formatted.push(&self.text[highlight_byte_index..*m_byte_end]);
                            }

                            byte_index = *m_byte_end;
                        }
                    }

                    // push the rest of the text between last match and the end of crop.
                    if byte_index < crop_byte_end {
                        formatted.push(&self.text[byte_index..crop_byte_end]);
                    }

                    // push crop marker if it's not the end of the text.
                    if crop_byte_end < self.text.len() && !self.crop_marker.is_empty() {
                        formatted.push(self.crop_marker);
                    }

                    if formatted.len() == 1 {
                        // avoid concatenating if there is already 1 slice.
                        Cow::Borrowed(&self.text[crop_byte_start..crop_byte_end])
                    } else {
                        Cow::Owned(formatted.concat())
                    }
                }
                None => self.compute_matches().format(format_options),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use charabia::TokenizerBuilder;
    use matching_words::tests::temp_index_with_documents;

    use super::*;
    use crate::index::tests::TempIndex;
    use crate::{execute_search, filtered_universe, SearchContext, TimeBudget};

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
                crate::search::new::GeoSortStrategy::default(),
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
}
