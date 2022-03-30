use std::borrow::Cow;

pub use matching_words::MatchingWords;
use meilisearch_tokenizer::token::SeparatorKind;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig, Token};

use crate::search::query_tree::Operation;

mod matching_words;

const DEFAULT_CROP_SIZE: usize = 10;
const DEFAULT_CROP_MARKER: &'static str = "…";
const DEFAULT_HIGHLIGHT_PREFIX: &'static str = "<em>";
const DEFAULT_HIGHLIGHT_SUFFIX: &'static str = "</em>";

pub struct MatcherBuilder {
    matching_words: MatchingWords,
    crop_size: usize,
    crop_marker: Option<String>,
    highlight_prefix: Option<String>,
    highlight_suffix: Option<String>,
}

impl MatcherBuilder {
    pub fn from_query_tree(query_tree: &Operation) -> Self {
        let matching_words = MatchingWords::from_query_tree(query_tree);

        Self {
            matching_words,
            crop_size: DEFAULT_CROP_SIZE,
            crop_marker: None,
            highlight_prefix: None,
            highlight_suffix: None,
        }
    }

    pub fn from_matching_words(matching_words: MatchingWords) -> Self {
        Self {
            matching_words,
            crop_size: DEFAULT_CROP_SIZE,
            crop_marker: None,
            highlight_prefix: None,
            highlight_suffix: None,
        }
    }

    pub fn crop_size(&mut self, word_count: usize) -> &Self {
        self.crop_size = word_count;
        self
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

    pub fn build<'t, 'm>(&'m self, tokens: &'t [Token], text: &'t str) -> Matcher<'t, 'm> {
        let crop_marker = match &self.crop_marker {
            Some(marker) => marker.as_str(),
            None => &DEFAULT_CROP_MARKER,
        };

        let highlight_prefix = match &self.highlight_prefix {
            Some(marker) => marker.as_str(),
            None => &DEFAULT_HIGHLIGHT_PREFIX,
        };
        let highlight_suffix = match &self.highlight_suffix {
            Some(marker) => marker.as_str(),
            None => &DEFAULT_HIGHLIGHT_SUFFIX,
        };
        Matcher {
            text,
            tokens,
            matching_words: &self.matching_words,
            crop_size: self.crop_size,
            crop_marker,
            highlight_prefix,
            highlight_suffix,
            matches: None,
        }
    }
}

// impl Default for MatcherBuilder {
//     fn default() -> Self {
//         Self {
//             crop_size: DEFAULT_CROP_SIZE,
//             crop_marker: None,
//             highlight_prefix: None,
//             highlight_suffix: None,
//         }
//     }
// }

#[derive(Clone, Debug)]
pub struct Match {
    match_len: usize,
    // id of the query word that matches.
    id: usize,
    // position of the word in the whole text.
    word_position: usize,
    // position of the token in the whole text.
    token_position: usize,
}

#[derive(Clone, Debug)]
pub struct MatchBounds {
    start: usize,
    length: usize,
}

pub struct Matcher<'t, 'm> {
    text: &'t str,
    tokens: &'t [Token<'t>],
    matching_words: &'m MatchingWords,
    crop_size: usize,
    crop_marker: &'m str,
    highlight_prefix: &'m str,
    highlight_suffix: &'m str,
    matches: Option<Vec<Match>>,
}

impl<'t> Matcher<'t, '_> {
    fn compute_matches(&mut self) -> &mut Self {
        let mut matches = Vec::new();
        let mut word_position = 0;
        let mut token_position = 0;
        for token in self.tokens {
            if token.is_separator().is_none() {
                if let Some((match_len, id)) = self.matching_words.matching_bytes_with_id(&token) {
                    matches.push(Match { match_len, id, word_position, token_position });
                }
                word_position += 1;
            }
            token_position += 1;
        }

        self.matches = Some(matches);
        self
    }

    pub fn matches(&mut self) -> Vec<MatchBounds> {
        match &self.matches {
            None => self.compute_matches().matches(),
            Some(matches) => matches
                .iter()
                .map(|m| MatchBounds {
                    start: self.tokens[m.token_position].byte_start,
                    length: m.match_len,
                })
                .collect(),
        }
    }

    fn token_crop_bounds(&self, matches: &[Match]) -> (usize, usize) {
        let first_match_word_position = matches.first().map(|m| m.word_position).unwrap_or(0);
        let first_match_token_position = matches.first().map(|m| m.token_position).unwrap_or(0);
        let last_match_word_position = matches.last().map(|m| m.word_position).unwrap_or(0);
        let last_match_token_position = matches.last().map(|m| m.token_position).unwrap_or(0);

        // TODO: buggy if no match and first token is a sepparator
        let mut remaining_words =
            self.crop_size + first_match_word_position - last_match_word_position;
        // if first token is a word, then remove 1 to remaining_words.
        if let Some(None) = self.tokens.get(first_match_token_position).map(|t| t.is_separator()) {
            remaining_words -= 1;
        }
        let mut first_token_position = first_match_token_position;
        let mut last_token_position = last_match_token_position;

        while remaining_words > 0 {
            match (
                first_token_position.checked_sub(1).and_then(|i| self.tokens.get(i)),
                last_token_position.checked_add(1).and_then(|i| self.tokens.get(i)),
            ) {
                (Some(ft), Some(lt)) => {
                    match (ft.is_separator(), lt.is_separator()) {
                        // if they are both separators and are the same kind then advance both
                        (Some(f_kind), Some(s_kind)) => {
                            if f_kind == s_kind {
                                first_token_position -= 1;
                                last_token_position += 1;
                            } else if f_kind == SeparatorKind::Hard {
                                last_token_position += 1;
                            } else {
                                first_token_position -= 1;
                            }
                        }
                        // left is a word, advance left
                        (None, Some(_)) => {
                            first_token_position -= 1;
                            remaining_words -= 1;
                        }
                        // right is a word, advance right
                        (Some(_), None) => {
                            last_token_position += 1;
                            remaining_words -= 1;
                        }
                        // both are words, advance left then right if remaining_word > 0
                        (None, None) => {
                            first_token_position -= 1;
                            remaining_words -= 1;

                            if remaining_words > 0 {
                                last_token_position += 1;
                                remaining_words -= 1;
                            }
                        }
                    }
                }
                // the end of the text is reached, advance left.
                (Some(ft), None) => {
                    first_token_position -= 1;
                    if ft.is_separator().is_none() {
                        remaining_words -= 1;
                    }
                }
                // the start of the text is reached, advance right.
                (None, Some(lt)) => {
                    last_token_position += 1;
                    if lt.is_separator().is_none() {
                        remaining_words -= 1;
                    }
                }
                // no more token to add.
                (None, None) => break,
            }
        }

        // if tokens after the end of the window are separators,
        // then add them to the window in order to keep context in cropped text.
        while let Some(_separator_kind) = last_token_position
            .checked_add(1)
            .and_then(|i| self.tokens.get(i))
            .and_then(|t| t.is_separator())
        {
            last_token_position += 1;
        }

        (first_token_position, last_token_position)
    }

    fn match_interval_score(&self, matches: &[Match]) -> (i16, i16, i16) {
        let mut ids = Vec::with_capacity(matches.len());
        let mut order_score = 0;
        let mut distance_score = 0;

        let mut iter = matches.iter().peekable();
        while let Some(m) = iter.next() {
            if let Some(next_match) = iter.peek() {
                // if matches are ordered
                if next_match.id > m.id {
                    order_score += 1;
                }

                // compute distance between matches
                distance_score -= (next_match.word_position - m.word_position).min(7) as i16;
            }

            ids.push(m.id);
        }

        ids.sort_unstable();
        ids.dedup();
        let uniq_score = ids.len() as i16;

        // rank by unique match count, then by distance between matches, then by ordered match count.
        (uniq_score, distance_score, order_score)
    }

    fn find_best_match_interval<'a>(&self, matches: &'a [Match]) -> &'a [Match] {
        if matches.len() > 1 {
            let mut best_interval = (0, 0);
            let mut best_interval_score = self.match_interval_score(&matches[0..=0]);
            let mut interval_first = 0;
            let mut interval_last = 0;
            for (index, next_match) in matches.iter().enumerate().skip(1) {
                // if next match would make interval gross more than crop_size
                if next_match.word_position - matches[interval_first].word_position
                    >= self.crop_size
                {
                    let interval_score =
                        self.match_interval_score(&matches[interval_first..=interval_last]);

                    // keep interval if it's the best
                    if interval_score > best_interval_score {
                        best_interval = (interval_first, interval_last);
                        best_interval_score = interval_score;
                    }

                    // advance start of the interval while interval is longer than crop_size
                    while next_match.word_position - matches[interval_first].word_position
                        >= self.crop_size
                    {
                        interval_first += 1;
                    }
                }
                interval_last = index;
            }

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

    fn crop_bounds(&self, matches: &[Match]) -> (usize, usize) {
        let match_interval = self.find_best_match_interval(matches);

        let (first_token_position, last_token_position) = self.token_crop_bounds(match_interval);

        let byte_start = self.tokens.get(first_token_position).map_or(0, |t| t.byte_start);
        let byte_end = self.tokens.get(last_token_position).map_or(byte_start, |t| t.byte_end);
        (byte_start, byte_end)
    }

    pub fn format(&mut self, highlight: bool, crop: bool) -> Cow<'t, str> {
        // If 0 it will be considered null and thus not crop the field
        // https://github.com/meilisearch/specifications/pull/120#discussion_r836536295
        let crop = crop && self.crop_size > 0;
        if !highlight && !crop {
            // compute matches is not needed if no highlight or crop is requested.
            Cow::Borrowed(self.text)
        } else {
            match &self.matches {
                Some(matches) => {
                    let (byte_start, byte_end) =
                        if crop { self.crop_bounds(matches) } else { (0, self.text.len()) };

                    let mut formatted = Vec::new();

                    // push crop marker if it's not the start of the text.
                    if byte_start > 0 && !self.crop_marker.is_empty() {
                        formatted.push(self.crop_marker);
                    }

                    let mut byte_index = byte_start;

                    if highlight {
                        // insert highlight markers around matches.
                        let tokens = self.tokens;
                        for m in matches
                            .iter()
                            .skip_while(|m| tokens[m.token_position].byte_start < byte_start)
                            .take_while(|m| tokens[m.token_position].byte_start < byte_end)
                        {
                            let token = &tokens[m.token_position];

                            if byte_index < token.byte_start {
                                formatted.push(&self.text[byte_index..token.byte_start]);
                            }

                            formatted.push(self.highlight_prefix);
                            formatted.push(&self.text[token.byte_start..token.byte_end]);
                            formatted.push(self.highlight_suffix);

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
                None => self.compute_matches().format(highlight, crop),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::query_tree::{Query, QueryKind};

    fn query_tree() -> Operation {
        Operation::Or(
            false,
            vec![Operation::And(vec![
                Operation::Query(Query {
                    prefix: true,
                    kind: QueryKind::exact("split".to_string()),
                }),
                Operation::Query(Query {
                    prefix: false,
                    kind: QueryKind::exact("the".to_string()),
                }),
                Operation::Query(Query {
                    prefix: true,
                    kind: QueryKind::tolerant(1, "world".to_string()),
                }),
            ])],
        )
    }

    #[test]
    fn format_identity() {
        let query_tree = query_tree();

        let builder = MatcherBuilder::from_query_tree(&query_tree);
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());

        let highlight = false;
        let crop = false;

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no crop and no highlight should return complete text.
        assert_eq!(&matcher.format(highlight, crop), &text);

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no crop and no highlight should return complete text.
        assert_eq!(&matcher.format(highlight, crop), &text);

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no crop and no highlight should return complete text.
        assert_eq!(&matcher.format(highlight, crop), &text);
    }

    #[test]
    fn format_highlight() {
        let query_tree = query_tree();

        let builder = MatcherBuilder::from_query_tree(&query_tree);
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());

        let highlight = true;
        let crop = false;

        // empty text.
        let text = "";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        assert_eq!(&matcher.format(highlight, crop), "");

        // text containing only separators.
        let text = ":-)";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        assert_eq!(&matcher.format(highlight, crop), ":-)");

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no crop should return complete text, because there is no matches.
        assert_eq!(&matcher.format(highlight, crop), &text);

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no crop should return complete text with highlighted matches.
        assert_eq!(&matcher.format(highlight, crop), "Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>.");

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no crop should return complete text with highlighted matches.
        assert_eq!(
            &matcher.format(highlight, crop),
            "Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves."
        );
    }

    #[test]
    fn format_crop() {
        let query_tree = query_tree();

        let builder = MatcherBuilder::from_query_tree(&query_tree);
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());

        let highlight = false;
        let crop = true;

        // empty text.
        let text = "";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        assert_eq!(&matcher.format(highlight, crop), "");

        // text containing only separators.
        let text = ":-)";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        assert_eq!(&matcher.format(highlight, crop), ":-)");

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no highlight should return 10 first words with a marker at the end.
        assert_eq!(
            &matcher.format(highlight, crop),
            "A quick brown fox can not jump 32 feet, right? …"
        );

        // Text without any match starting by a separator.
        let text = "(A quick brown fox can not jump 32 feet, right? Brr, it is cold!)";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no highlight should return 10 first words with a marker at the end.
        assert_eq!(
            &matcher.format(highlight, crop),
            "(A quick brown fox can not jump 32 feet, right? …"
        );

        // Test phrase propagation
        let text = "Natalie risk her future. Split The World is a book written by Emily Henry. I never read it.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // should crop the phrase instead of croping around the match.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…Split The World is a book written by Emily Henry. …"
        );

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no highlight should return 10 last words with a marker at the start.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…future to build a world with the boy she loves."
        );

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no highlight should return 10 last words with a marker at the start.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…she loves. Emily Henry: The Love That Split The World."
        );

        // Text containing a match unordered and a match ordered.
        let text = "The world split void void void void void void void void void split the world void void";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // crop should return 10 last words with a marker at the start.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…void void void void void split the world void void"
        );

        // Text containing matches with diferent density.
        let text = "split void the void void world void void void void void void void void void void split the world void void";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // crop should return 10 last words with a marker at the start.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…void void void void void split the world void void"
        );

        // Text containing matches with same word.
        let text = "split split split split split split void void void void void void void void void void split the world void void";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // crop should return 10 last words with a marker at the start.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…void void void void void split the world void void"
        );
    }

    #[test]
    fn format_highlight_crop() {
        let query_tree = query_tree();

        let builder = MatcherBuilder::from_query_tree(&query_tree);
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());

        let highlight = true;
        let crop = true;

        // empty text.
        let text = "";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        assert_eq!(&matcher.format(highlight, crop), "");

        // text containing only separators.
        let text = ":-)";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        assert_eq!(&matcher.format(highlight, crop), ":-)");

        // Text without any match.
        let text = "A quick brown fox can not jump 32 feet, right? Brr, it is cold!";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // both should return 10 first words with a marker at the end.
        assert_eq!(
            &matcher.format(highlight, crop),
            "A quick brown fox can not jump 32 feet, right? …"
        );

        // Text containing some matches.
        let text = "Natalie risk her future to build a world with the boy she loves.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // both should return 10 last words with a marker at the start and highlighted matches.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…future to build a <em>world</em> with <em>the</em> boy she loves."
        );

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // both should return 10 last words with a marker at the start and highlighted matches.
        assert_eq!(&matcher.format(highlight, crop), "…she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>.");

        // Text containing a match unordered and a match ordered.
        let text = "The world split void void void void void void void void void split the world void void";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // crop should return 10 last words with a marker at the start.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…void void void void void <em>split</em> <em>the</em> <em>world</em> void void"
        );
    }

    #[test]
    fn smaller_crop_size() {
        //! testing: https://github.com/meilisearch/specifications/pull/120#discussion_r836536295
        let query_tree = query_tree();

        let mut builder = MatcherBuilder::from_query_tree(&query_tree);
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());

        let highlight = false;
        let crop = true;

        let text = "void void split the world void void.";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();

        // set a smaller crop size
        builder.crop_size(2);
        let mut matcher = builder.build(&tokens[..], text);
        // because crop size < query size, partially format matches.
        assert_eq!(&matcher.format(highlight, crop), "…split the …");

        // set a smaller crop size
        builder.crop_size(1);
        let mut matcher = builder.build(&tokens[..], text);
        // because crop size < query size, partially format matches.
        assert_eq!(&matcher.format(highlight, crop), "…split …");

        // set a smaller crop size
        builder.crop_size(0);
        let mut matcher = builder.build(&tokens[..], text);
        // because crop size is 0, crop is ignored.
        assert_eq!(&matcher.format(highlight, crop), "void void split the world void void.");
    }
}
