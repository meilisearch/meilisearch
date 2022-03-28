use std::borrow::Cow;

use matching_words::MatchingWords;
use meilisearch_tokenizer::token::SeparatorKind;
use meilisearch_tokenizer::{Analyzer, AnalyzerConfig, Token};

use crate::search::query_tree::Operation;

pub mod matching_words;

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

#[derive(Clone)]
pub struct Match {
    match_len: usize,
    // id of the query word that matches.
    id: usize,
    // position of the word in the whole text.
    word_position: usize,
    // position of the token in the whole text.
    token_position: usize,
}

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

    fn crop_around(&self, matches: &[Match]) -> (usize, usize) {
        let first_match_word_position = matches.first().map(|m| m.word_position).unwrap_or(0);
        let first_match_token_position = matches.first().map(|m| m.token_position).unwrap_or(0);
        let last_match_word_position = matches.last().map(|m| m.word_position).unwrap_or(0);
        let last_match_token_position = matches.last().map(|m| m.token_position).unwrap_or(0);

        // TODO: buggy if no match and fisrt token is a sepparator
        let mut remaining_words =
            self.crop_size + first_match_word_position - last_match_word_position - 1;
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
                (Some(ft), None) => {
                    first_token_position -= 1;
                    if ft.is_separator().is_none() {
                        remaining_words -= 1;
                    }
                }
                (None, Some(lt)) => {
                    last_token_position += 1;
                    if lt.is_separator().is_none() {
                        remaining_words -= 1;
                    }
                }
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

        (self.tokens[first_token_position].byte_start, self.tokens[last_token_position].byte_end)
    }

    fn crop_bounds(&self, matches: &[Match]) -> (usize, usize) {
        match matches {
            // at least 2 matches
            [first, last, ..] => self.crop_around(&[first.clone()][..]),
            // less than 2 matches
            _ => self.crop_around(matches),
        }
    }

    pub fn format(&mut self, highlight: bool, crop: bool) -> Cow<'t, str> {
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
    }

    #[test]
    fn format_highlight_crop() {
        let query_tree = query_tree();

        let builder = MatcherBuilder::from_query_tree(&query_tree);
        let analyzer = Analyzer::new(AnalyzerConfig::<Vec<u8>>::default());

        let highlight = true;
        let crop = true;

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
}
