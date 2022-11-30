use std::borrow::Cow;

use charabia::{SeparatorKind, Token, Tokenizer};
use matching_words::{MatchType, PartialMatch, PrimitiveWordId};
pub use matching_words::{MatchingWord, MatchingWords};
use serde::Serialize;

pub mod matching_words;

const DEFAULT_CROP_MARKER: &str = "…";
const DEFAULT_HIGHLIGHT_PREFIX: &str = "<em>";
const DEFAULT_HIGHLIGHT_SUFFIX: &str = "</em>";

/// Structure used to build a Matcher allowing to customize formating tags.
pub struct MatcherBuilder<'a, A> {
    matching_words: MatchingWords,
    tokenizer: Tokenizer<'a, A>,
    crop_marker: Option<String>,
    highlight_prefix: Option<String>,
    highlight_suffix: Option<String>,
}

impl<'a, A> MatcherBuilder<'a, A> {
    pub fn new(matching_words: MatchingWords, tokenizer: Tokenizer<'a, A>) -> Self {
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

    pub fn build<'t, 'm>(&'m self, text: &'t str) -> Matcher<'t, 'm, A> {
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
}

#[derive(Clone, Debug)]
pub struct Match {
    match_len: usize,
    // ids of the query words that matches.
    ids: Vec<PrimitiveWordId>,
    // position of the word in the whole text.
    word_position: usize,
    // position of the token in the whole text.
    token_position: usize,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct MatchBounds {
    pub start: usize,
    pub length: usize,
}

/// Structure used to analize a string, compute words that match,
/// and format the source string, returning a highlighted and cropped sub-string.
pub struct Matcher<'t, 'm, A> {
    text: &'t str,
    matching_words: &'m MatchingWords,
    tokenizer: &'m Tokenizer<'m, A>,
    crop_marker: &'m str,
    highlight_prefix: &'m str,
    highlight_suffix: &'m str,
    matches: Option<(Vec<Token<'t>>, Vec<Match>)>,
}

impl<'t, A: AsRef<[u8]>> Matcher<'t, '_, A> {
    /// Iterates over tokens and save any of them that matches the query.
    fn compute_matches(&mut self) -> &mut Self {
        /// some words are counted as matches only if they are close together and in the good order,
        /// compute_partial_match peek into next words to validate if the match is complete.
        fn compute_partial_match<'a>(
            mut partial: PartialMatch,
            token_position: usize,
            word_position: usize,
            words_positions: &mut impl Iterator<Item = (usize, usize, &'a Token<'a>)>,
            matches: &mut Vec<Match>,
        ) -> bool {
            let mut potential_matches = vec![(token_position, word_position, partial.char_len())];

            for (token_position, word_position, word) in words_positions {
                partial = match partial.match_token(word) {
                    // token matches the partial match, but the match is not full,
                    // we temporarly save the current token then we try to match the next one.
                    Some(MatchType::Partial(partial)) => {
                        potential_matches.push((token_position, word_position, partial.char_len()));
                        partial
                    }
                    // partial match is now full, we keep this matches and we advance positions
                    Some(MatchType::Full { char_len, ids }) => {
                        // save previously matched tokens as matches.
                        let iter = potential_matches.into_iter().map(
                            |(token_position, word_position, match_len)| Match {
                                match_len,
                                ids: ids.to_vec(),
                                word_position,
                                token_position,
                            },
                        );
                        matches.extend(iter);

                        // save the token that closes the partial match as a match.
                        matches.push(Match {
                            match_len: char_len,
                            ids: ids.to_vec(),
                            word_position,
                            token_position,
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

        let tokens: Vec<_> = self.tokenizer.tokenize(self.text).collect();
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
                    MatchType::Full { char_len, ids } => {
                        matches.push(Match {
                            match_len: char_len,
                            ids: ids.to_vec(),
                            word_position,
                            token_position,
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
    pub fn matches(&mut self) -> Vec<MatchBounds> {
        match &self.matches {
            None => self.compute_matches().matches(),
            Some((tokens, matches)) => matches
                .iter()
                .map(|m| MatchBounds {
                    start: tokens[m.token_position].byte_start,
                    length: m.match_len,
                })
                .collect(),
        }
    }

    /// Returns the bounds in byte index of the crop window.
    fn crop_bounds(&self, tokens: &[Token], matches: &[Match], crop_size: usize) -> (usize, usize) {
        // if there is no match, we start from the beginning of the string by default.
        let first_match_word_position = matches.first().map(|m| m.word_position).unwrap_or(0);
        let first_match_token_position = matches.first().map(|m| m.token_position).unwrap_or(0);
        let last_match_word_position = matches.last().map(|m| m.word_position).unwrap_or(0);
        let last_match_token_position = matches.last().map(|m| m.token_position).unwrap_or(0);

        // matches needs to be counted in the crop len.
        let mut remaining_words = crop_size + first_match_word_position - last_match_word_position;

        // create the initial state of the crop window: 2 iterators starting from the matches positions,
        // a reverse iterator starting from the first match token position and going towards the beginning of the text,
        let mut before_tokens = tokens[..first_match_token_position].iter().rev().peekable();
        // an iterator starting from the last match token position and going towards the end of the text.
        let mut after_tokens = tokens[last_match_token_position..].iter().peekable();

        // grows the crop window peeking in both directions
        // until the window contains the good number of words:
        while remaining_words > 0 {
            let before_token = before_tokens.peek().map(|t| t.separator_kind());
            let after_token = after_tokens.peek().map(|t| t.separator_kind());

            match (before_token, after_token) {
                // we can expand both sides.
                (Some(before_token), Some(after_token)) => {
                    match (before_token, after_token) {
                        // if they are both separators and are the same kind then advance both,
                        // or expand in the soft separator separator side.
                        (Some(before_token_kind), Some(after_token_kind)) => {
                            if before_token_kind == after_token_kind {
                                before_tokens.next();

                                // this avoid having an ending separator before crop marker.
                                if remaining_words > 1 {
                                    after_tokens.next();
                                }
                            } else if before_token_kind == SeparatorKind::Hard {
                                after_tokens.next();
                            } else {
                                before_tokens.next();
                            }
                        }
                        // if one of the tokens is a word, we expend in the side of the word.
                        // left is a word, advance left.
                        (None, Some(_)) => {
                            before_tokens.next();
                            remaining_words -= 1;
                        }
                        // right is a word, advance right.
                        (Some(_), None) => {
                            after_tokens.next();
                            remaining_words -= 1;
                        }
                        // both are words, advance left then right if remaining_word > 0.
                        (None, None) => {
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
                (Some(before_token), None) => {
                    before_tokens.next();
                    if before_token.is_none() {
                        remaining_words -= 1;
                    }
                }
                // the start of the text is reached, advance right.
                (None, Some(after_token)) => {
                    after_tokens.next();
                    if after_token.is_none() {
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

        (crop_byte_start, crop_byte_end)
    }

    /// Compute the score of a match interval:
    /// 1) count unique matches
    /// 2) calculate distance between matches
    /// 3) count ordered matches
    fn match_interval_score(&self, matches: &[Match]) -> (i16, i16, i16) {
        let mut ids: Vec<PrimitiveWordId> = Vec::with_capacity(matches.len());
        let mut order_score = 0;
        let mut distance_score = 0;

        let mut iter = matches.iter().peekable();
        while let Some(m) = iter.next() {
            if let Some(next_match) = iter.peek() {
                // if matches are ordered
                if next_match.ids.iter().min() > m.ids.iter().min() {
                    order_score += 1;
                }

                // compute distance between matches
                distance_score -= (next_match.word_position - m.word_position).min(7) as i16;
            }

            ids.extend(m.ids.iter());
        }

        ids.sort_unstable();
        ids.dedup();
        let uniq_score = ids.len() as i16;

        // rank by unique match count, then by distance between matches, then by ordered match count.
        (uniq_score, distance_score, order_score)
    }

    /// Returns the matches interval where the score computed by match_interval_score is the best.
    fn find_best_match_interval<'a>(&self, matches: &'a [Match], crop_size: usize) -> &'a [Match] {
        // we compute the matches interval if we have at least 2 matches.
        if matches.len() > 1 {
            // positions of the first and the last match of the best matches interval in `matches`.
            let mut best_interval = (0, 0);
            let mut best_interval_score = self.match_interval_score(&matches[0..=0]);
            // current interval positions.
            let mut interval_first = 0;
            let mut interval_last = 0;
            for (index, next_match) in matches.iter().enumerate().skip(1) {
                // if next match would make interval gross more than crop_size,
                // we compare the current interval with the best one,
                // then we increase `interval_first` until next match can be added.
                if next_match.word_position - matches[interval_first].word_position >= crop_size {
                    let interval_score =
                        self.match_interval_score(&matches[interval_first..=interval_last]);

                    // keep interval if it's the best
                    if interval_score > best_interval_score {
                        best_interval = (interval_first, interval_last);
                        best_interval_score = interval_score;
                    }

                    // advance start of the interval while interval is longer than crop_size.
                    while next_match.word_position - matches[interval_first].word_position
                        >= crop_size
                    {
                        interval_first += 1;
                    }
                }
                interval_last = index;
            }

            // compute the last interval score and compare it to the best one.
            let interval_score =
                self.match_interval_score(&matches[interval_first..=interval_last]);
            if interval_score > best_interval_score {
                best_interval = (interval_first, interval_last);
            }

            &matches[best_interval.0..=best_interval.1]
        } else {
            matches
        }
    }

    // Returns the formatted version of the original text.
    pub fn format(&mut self, format_options: FormatOptions) -> Cow<'t, str> {
        if !format_options.highlight && format_options.crop.is_none() {
            // compute matches is not needed if no highlight nor crop is requested.
            Cow::Borrowed(self.text)
        } else {
            match &self.matches {
                Some((tokens, matches)) => {
                    // If the text has to be cropped,
                    // compute the best interval to crop around.
                    let matches = match format_options.crop {
                        Some(crop_size) if crop_size > 0 => {
                            self.find_best_match_interval(matches, crop_size)
                        }
                        _ => matches,
                    };

                    // If the text has to be cropped,
                    // crop around the best interval.
                    let (byte_start, byte_end) = match format_options.crop {
                        Some(crop_size) if crop_size > 0 => {
                            self.crop_bounds(tokens, matches, crop_size)
                        }
                        _ => (0, self.text.len()),
                    };

                    let mut formatted = Vec::new();

                    // push crop marker if it's not the start of the text.
                    if byte_start > 0 && !self.crop_marker.is_empty() {
                        formatted.push(self.crop_marker);
                    }

                    let mut byte_index = byte_start;

                    if format_options.highlight {
                        // insert highlight markers around matches.
                        for m in matches {
                            let token = &tokens[m.token_position];

                            if byte_index < token.byte_start {
                                formatted.push(&self.text[byte_index..token.byte_start]);
                            }

                            let highlight_byte_index = self.text[token.byte_start..]
                                .char_indices()
                                .enumerate()
                                .find(|(i, _)| *i == m.match_len)
                                .map_or(token.byte_end, |(_, (i, _))| i + token.byte_start);
                            formatted.push(self.highlight_prefix);
                            formatted.push(&self.text[token.byte_start..highlight_byte_index]);
                            formatted.push(self.highlight_suffix);
                            // if it's a prefix highlight, we put the end of the word after the highlight marker.
                            if highlight_byte_index < token.byte_end {
                                formatted.push(&self.text[highlight_byte_index..token.byte_end]);
                            }

                            byte_index = token.byte_end;
                        }
                    }

                    // push the rest of the text between last match and the end of crop.
                    if byte_index < byte_end {
                        formatted.push(&self.text[byte_index..byte_end]);
                    }

                    // push crop marker if it's not the end of the text.
                    if byte_end < self.text.len() && !self.crop_marker.is_empty() {
                        formatted.push(self.crop_marker);
                    }

                    if formatted.len() == 1 {
                        // avoid concatenating if there is already 1 slice.
                        Cow::Borrowed(&self.text[byte_start..byte_end])
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
    use std::rc::Rc;

    use charabia::TokenizerBuilder;

    use super::*;
    use crate::search::matches::matching_words::MatchingWord;

    fn matching_words() -> MatchingWords {
        let all = vec![
            Rc::new(MatchingWord::new("split".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("the".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("world".to_string(), 1, true).unwrap()),
        ];
        let matching_words = vec![
            (vec![all[0].clone()], vec![0]),
            (vec![all[1].clone()], vec![1]),
            (vec![all[2].clone()], vec![2]),
        ];

        MatchingWords::new(matching_words)
    }

    impl MatcherBuilder<'_, Vec<u8>> {
        pub fn from_matching_words(matching_words: MatchingWords) -> Self {
            Self::new(matching_words, TokenizerBuilder::default().build())
        }
    }

    #[test]
    fn format_identity() {
        let matching_words = matching_words();

        let builder = MatcherBuilder::from_matching_words(matching_words);

        let format_options = FormatOptions { highlight: false, crop: None };

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let mut matcher = builder.build(text);
        // no crop and no highlight should return complete text.
        assert_eq!(&matcher.format(format_options), &text);

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let mut matcher = builder.build(text);
        // no crop and no highlight should return complete text.
        assert_eq!(&matcher.format(format_options), &text);

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let mut matcher = builder.build(text);
        // no crop and no highlight should return complete text.
        assert_eq!(&matcher.format(format_options), &text);
    }

    #[test]
    fn format_highlight() {
        let matching_words = matching_words();

        let builder = MatcherBuilder::from_matching_words(matching_words);

        let format_options = FormatOptions { highlight: true, crop: None };

        // empty text.
        let text = "";
        let mut matcher = builder.build(text);
        assert_eq!(&matcher.format(format_options), "");

        // text containing only separators.
        let text = ":-)";
        let mut matcher = builder.build(text);
        assert_eq!(&matcher.format(format_options), ":-)");

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let mut matcher = builder.build(text);
        // no crop should return complete text, because there is no matches.
        assert_eq!(&matcher.format(format_options), &text);

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let mut matcher = builder.build(text);
        // no crop should return complete text with highlighted matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>."
        );

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let mut matcher = builder.build(text);
        // no crop should return complete text with highlighted matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves."
        );
    }

    #[test]
    fn highlight_unicode() {
        let all = vec![
            Rc::new(MatchingWord::new("wessfali".to_string(), 1, true).unwrap()),
            Rc::new(MatchingWord::new("world".to_string(), 1, true).unwrap()),
        ];
        let matching_words = vec![(vec![all[0].clone()], vec![0]), (vec![all[1].clone()], vec![1])];

        let matching_words = MatchingWords::new(matching_words);

        let builder = MatcherBuilder::from_matching_words(matching_words);

        let format_options = FormatOptions { highlight: true, crop: None };

        // Text containing prefix match.
        let text = "Ŵôřlḑôle";
        let mut matcher = builder.build(text);
        // no crop should return complete text with highlighted matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"<em>Ŵôřlḑ</em>ôle"
        );

        // Text containing unicode match.
        let text = "Ŵôřlḑ";
        let mut matcher = builder.build(text);
        // no crop should return complete text with highlighted matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"<em>Ŵôřlḑ</em>"
        );

        // Text containing unicode match.
        let text = "Westfália";
        let mut matcher = builder.build(text);
        // no crop should return complete text with highlighted matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"<em>Westfáli</em>a"
        );
    }

    #[test]
    fn format_crop() {
        let matching_words = matching_words();

        let builder = MatcherBuilder::from_matching_words(matching_words);

        let format_options = FormatOptions { highlight: false, crop: Some(10) };

        // empty text.
        let text = "";
        let mut matcher = builder.build(text);
        insta::assert_snapshot!(
            matcher.format(format_options),
            @""
        );

        // text containing only separators.
        let text = ":-)";
        let mut matcher = builder.build(text);
        insta::assert_snapshot!(
            matcher.format(format_options),
            @":-)"
        );

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let mut matcher = builder.build(text);
        // no highlight should return 10 first words with a marker at the end.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"A quick brown fox can not jump 32 feet, right…"
        );

        // Text without any match starting by a separator.
        let text = "(A quick brown fox can not jump 32 feet, right? Brr, it is cold!)";
        let mut matcher = builder.build(text);
        // no highlight should return 10 first words with a marker at the end.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"(A quick brown fox can not jump 32 feet, right…"
        );

        // Test phrase propagation
        let text = "Natalie risk her future. Split The World is a book written by Emily Henry. I never read it.";
        let mut matcher = builder.build(text);
        // should crop the phrase instead of croping around the match.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"… Split The World is a book written by Emily Henry…"
        );

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let mut matcher = builder.build(text);
        // no highlight should return 10 last words with a marker at the start.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…future to build a world with the boy she loves…"
        );

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let mut matcher = builder.build(text);
        // no highlight should return 10 last words with a marker at the start.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…she loves. Emily Henry: The Love That Split The World."
        );

        // Text containing a match unordered and a match ordered.
        let text = "The world split void void void void void void void void void split the world void void";
        let mut matcher = builder.build(text);
        // crop should return 10 last words with a marker at the start.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…void void void void void split the world void void"
        );

        // Text containing matches with diferent density.
        let text = "split void the void void world void void void void void void void void void void split the world void void";
        let mut matcher = builder.build(text);
        // crop should return 10 last words with a marker at the start.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…void void void void void split the world void void"
        );

        // Text containing matches with same word.
        let text = "split split split split split split void void void void void void void void void void split the world void void";
        let mut matcher = builder.build(text);
        // crop should return 10 last words with a marker at the start.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…void void void void void split the world void void"
        );
    }

    #[test]
    fn format_highlight_crop() {
        let matching_words = matching_words();

        let builder = MatcherBuilder::from_matching_words(matching_words);

        let format_options = FormatOptions { highlight: true, crop: Some(10) };

        // empty text.
        let text = "";
        let mut matcher = builder.build(text);
        insta::assert_snapshot!(
            matcher.format(format_options),
            @""
        );

        // text containing only separators.
        let text = ":-)";
        let mut matcher = builder.build(text);
        insta::assert_snapshot!(
            matcher.format(format_options),
            @":-)"
        );

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let mut matcher = builder.build(text);
        // both should return 10 first words with a marker at the end.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"A quick brown fox can not jump 32 feet, right…"
        );

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let mut matcher = builder.build(text);
        // both should return 10 last words with a marker at the start and highlighted matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…future to build a <em>world</em> with <em>the</em> boy she loves…"
        );

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let mut matcher = builder.build(text);
        // both should return 10 last words with a marker at the start and highlighted matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>."
        );

        // Text containing a match unordered and a match ordered.
        let text = "The world split void void void void void void void void void split the world void void";
        let mut matcher = builder.build(text);
        // crop should return 10 last words with a marker at the start.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…void void void void void <em>split</em> <em>the</em> <em>world</em> void void"
        );
    }

    #[test]
    fn smaller_crop_size() {
        //! testing: https://github.com/meilisearch/specifications/pull/120#discussion_r836536295
        let matching_words = matching_words();

        let builder = MatcherBuilder::from_matching_words(matching_words);

        let text = "void void split the world void void.";

        // set a smaller crop size
        let format_options = FormatOptions { highlight: false, crop: Some(2) };
        let mut matcher = builder.build(text);
        // because crop size < query size, partially format matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…split the…"
        );

        // set a smaller crop size
        let format_options = FormatOptions { highlight: false, crop: Some(1) };
        let mut matcher = builder.build(text);
        // because crop size < query size, partially format matches.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"…split…"
        );

        // set  crop size to 0
        let format_options = FormatOptions { highlight: false, crop: Some(0) };
        let mut matcher = builder.build(text);
        // because crop size is 0, crop is ignored.
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"void void split the world void void."
        );
    }

    #[test]
    fn partial_matches() {
        let all = vec![
            Rc::new(MatchingWord::new("the".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("t".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("he".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("door".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("do".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("or".to_string(), 0, false).unwrap()),
        ];
        let matching_words = vec![
            (vec![all[0].clone()], vec![0]),
            (vec![all[1].clone(), all[2].clone()], vec![0]),
            (vec![all[3].clone()], vec![1]),
            (vec![all[4].clone(), all[5].clone()], vec![1]),
            (vec![all[4].clone()], vec![2]),
        ];

        let matching_words = MatchingWords::new(matching_words);

        let mut builder = MatcherBuilder::from_matching_words(matching_words);
        builder.highlight_prefix("_".to_string());
        builder.highlight_suffix("_".to_string());

        let format_options = FormatOptions { highlight: true, crop: None };

        let text = "the do or die can't be he do and or isn't he";
        let mut matcher = builder.build(text);
        insta::assert_snapshot!(
            matcher.format(format_options),
            @"_the_ _do_ _or_ die can't be he _do_ and or isn'_t_ _he_"
        );
    }
}
