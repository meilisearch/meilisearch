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

pub struct Match<'t> {
    token: &'t Token<'t>,
    match_len: usize,
    // id of the query word that matches.
    id: usize,
    // position of the word in the whole text.
    position: usize,
}

pub struct MatchBounds {
    start: usize,
    length: usize,
}

impl<'t> From<&Match<'t>> for MatchBounds {
    fn from(m: &Match) -> Self {
        MatchBounds { start: m.token.byte_start, length: m.match_len }
    }
}

pub struct Matcher<'t, 'm> {
    text: &'t str,
    tokens: &'t [Token<'t>],
    matching_words: &'m MatchingWords,
    crop_size: usize,
    crop_marker: &'m str,
    highlight_prefix: &'m str,
    highlight_suffix: &'m str,
    matches: Option<Vec<Match<'t>>>,
}

impl<'t> Matcher<'t, '_> {
    fn compute_matches(&mut self) -> &mut Self {
        let mut matches = Vec::new();
        let mut position = 0;
        for token in self.tokens {
            match token.is_separator() {
                Some(SeparatorKind::Hard) => position += 7,
                None => {
                    if let Some((match_len, id)) =
                        self.matching_words.matching_bytes_with_id(&token)
                    {
                        matches.push(Match { token, match_len, id, position });
                    }
                    position += 1;
                }
                _otherwise => {}
            }
        }

        self.matches = Some(matches);
        self
    }

    pub fn matches(&mut self) -> Vec<MatchBounds> {
        match &self.matches {
            None => self.compute_matches().matches(),
            Some(matches) => matches.iter().map(MatchBounds::from).collect(),
        }
    }

    fn crop_bounds(&self, matches: &[Match<'t>]) -> (usize, usize) {
        let byte_end = self
            .tokens
            .iter()
            .filter(|t| t.is_separator().is_none())
            .enumerate()
            .take_while(|(i, _)| *i < self.crop_size)
            .last()
            .map_or(self.text.len(), |(_, t)| t.byte_end);

        (0, byte_end)
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
                        for m in matches
                            .iter()
                            .skip_while(|m| m.token.byte_start < byte_start)
                            .take_while(|m| m.token.byte_start < byte_end)
                        {
                            if byte_index < m.token.byte_start {
                                formatted.push(&self.text[byte_index..m.token.byte_start]);
                            }

                            formatted.push(self.highlight_prefix);
                            formatted.push(&self.text[m.token.byte_start..m.token.byte_end]);
                            formatted.push(self.highlight_suffix);

                            byte_index = m.token.byte_end;
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
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World";
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
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no crop should return complete text with highlighted matches.
        assert_eq!(&matcher.format(highlight, crop), "Natalie risk her future to build a <em>world</em> with <em>the</em> boy she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>");

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
            "A quick brown fox can not jump 32 feet, right…"
        );

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // no highlight should return 10 last words with a marker at the start.
        assert_eq!(
            &matcher.format(highlight, crop),
            "…she loves. Emily Henry: The Love That Split The World"
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
            "A quick brown fox can not jump 32 feet, right…"
        );

        // Text containing all matches.
        let text = "Natalie risk her future to build a world with the boy she loves. Emily Henry: The Love That Split The World";
        let analyzed = analyzer.analyze(&text);
        let tokens: Vec<_> = analyzed.tokens().collect();
        let mut matcher = builder.build(&tokens[..], text);
        // both should return 10 last words with a marker at the start and highlighted matches.
        assert_eq!(&matcher.format(highlight, crop), "…she loves. Emily Henry: <em>The</em> Love That <em>Split</em> <em>The</em> <em>World</em>");

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
