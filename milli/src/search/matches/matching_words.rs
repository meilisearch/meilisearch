use std::cmp::{min, Reverse};
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Index, IndexMut};

use levenshtein_automata::{Distance, DFA};
use meilisearch_tokenizer::Token;

use crate::search::build_dfa;

type IsPrefix = bool;

/// Structure created from a query tree
/// referencing words that match the given query tree.
#[derive(Default)]
pub struct MatchingWords {
    inner: Vec<(Vec<MatchingWord>, Vec<PrimitiveWordId>)>,
}

impl MatchingWords {
    pub fn new(mut matching_words: Vec<(Vec<MatchingWord>, Vec<PrimitiveWordId>)>) -> Self {
        // Sort word by len in DESC order prioritizing the longuest matches,
        // in order to highlight the longuest part of the matched word.
        matching_words.sort_unstable_by_key(|(mw, _)| Reverse((mw.len(), mw[0].word.len())));

        Self { inner: matching_words }
    }

    pub fn match_token<'a, 'b>(&'a self, token: &'b Token<'b>) -> MatchesIter<'a, 'b> {
        MatchesIter { inner: Box::new(self.inner.iter()), token }
    }
}

pub struct MatchesIter<'a, 'b> {
    inner: Box<dyn Iterator<Item = &'a (Vec<MatchingWord>, Vec<PrimitiveWordId>)> + 'a>,
    token: &'b Token<'b>,
}

impl<'a> Iterator for MatchesIter<'a, '_> {
    type Item = MatchType<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((matching_words, ids)) => match matching_words[0].match_token(&self.token) {
                Some(char_len) => {
                    if matching_words.len() > 1 {
                        Some(MatchType::Partial(PartialMatch {
                            matching_words: &matching_words[1..],
                            ids,
                            char_len,
                        }))
                    } else {
                        Some(MatchType::Full { char_len, ids })
                    }
                }
                None => self.next(),
            },
            None => None,
        }
    }
}

pub type PrimitiveWordId = u8;
pub struct MatchingWord {
    pub dfa: DFA,
    pub word: String,
    pub typo: u8,
    pub prefix: IsPrefix,
}

impl fmt::Debug for MatchingWord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MatchingWord")
            .field("word", &self.word)
            .field("typo", &self.typo)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl PartialEq for MatchingWord {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.typo == other.typo && self.word == other.word
    }
}

impl MatchingWord {
    pub fn new(word: String, typo: u8, prefix: IsPrefix) -> Self {
        let dfa = build_dfa(&word, typo, prefix);

        Self { dfa, word, typo, prefix }
    }

    pub fn match_token(&self, token: &Token) -> Option<usize> {
        match self.dfa.eval(token.text()) {
            Distance::Exact(t) if t <= self.typo => {
                if self.prefix {
                    let len = bytes_to_highlight(token.text(), &self.word);
                    Some(token.num_chars_from_bytes(len))
                } else {
                    Some(token.num_chars_from_bytes(token.text().len()))
                }
            }
            _otherwise => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum MatchType<'a> {
    Full { char_len: usize, ids: &'a [PrimitiveWordId] },
    Partial(PartialMatch<'a>),
}

#[derive(Debug, PartialEq)]
pub struct PartialMatch<'a> {
    matching_words: &'a [MatchingWord],
    ids: &'a [PrimitiveWordId],
    char_len: usize,
}

impl<'a> PartialMatch<'a> {
    pub fn match_token(self, token: &Token) -> Option<MatchType<'a>> {
        self.matching_words[0].match_token(token).map(|char_len| {
            if self.matching_words.len() > 1 {
                MatchType::Partial(PartialMatch {
                    matching_words: &self.matching_words[1..],
                    ids: self.ids,
                    char_len,
                })
            } else {
                MatchType::Full { char_len, ids: self.ids }
            }
        })
    }

    pub fn char_len(&self) -> usize {
        self.char_len
    }
}

// A simple wrapper around vec so we can get contiguous but index it like it's 2D array.
struct N2Array<T> {
    y_size: usize,
    buf: Vec<T>,
}

impl<T: Clone> N2Array<T> {
    fn new(x: usize, y: usize, value: T) -> N2Array<T> {
        N2Array { y_size: y, buf: vec![value; x * y] }
    }
}

impl<T> Index<(usize, usize)> for N2Array<T> {
    type Output = T;

    #[inline]
    fn index(&self, (x, y): (usize, usize)) -> &T {
        &self.buf[(x * self.y_size) + y]
    }
}

impl<T> IndexMut<(usize, usize)> for N2Array<T> {
    #[inline]
    fn index_mut(&mut self, (x, y): (usize, usize)) -> &mut T {
        &mut self.buf[(x * self.y_size) + y]
    }
}

/// Returns the number of **bytes** we want to highlight in the `source` word.
/// Basically we want to highlight as much characters as possible in the source until it has too much
/// typos (= 2)
/// The algorithm is a modified
/// [Damerau-Levenshtein](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance)
fn bytes_to_highlight(source: &str, target: &str) -> usize {
    let n = source.chars().count();
    let m = target.chars().count();

    if n == 0 {
        return 0;
    }
    // since we allow two typos we can send two characters even if it's completely wrong
    if m < 3 {
        return source.chars().take(m).map(|c| c.len_utf8()).sum();
    }
    if n == m && source == target {
        return source.len();
    }

    let inf = n + m;
    let mut matrix = N2Array::new(n + 2, m + 2, 0);

    matrix[(0, 0)] = inf;
    for i in 0..=n {
        matrix[(i + 1, 0)] = inf;
        matrix[(i + 1, 1)] = i;
    }
    for j in 0..=m {
        matrix[(0, j + 1)] = inf;
        matrix[(1, j + 1)] = j;
    }

    let mut last_row = BTreeMap::new();

    for (row, char_s) in source.chars().enumerate() {
        let mut last_match_col = 0;
        let row = row + 1;

        for (col, char_t) in target.chars().enumerate() {
            let col = col + 1;
            let last_match_row = *last_row.get(&char_t).unwrap_or(&0);
            let cost = if char_s == char_t { 0 } else { 1 };

            let dist_add = matrix[(row, col + 1)] + 1;
            let dist_del = matrix[(row + 1, col)] + 1;
            let dist_sub = matrix[(row, col)] + cost;
            let dist_trans = matrix[(last_match_row, last_match_col)]
                + (row - last_match_row - 1)
                + 1
                + (col - last_match_col - 1);
            let dist = min(min(dist_add, dist_del), min(dist_sub, dist_trans));
            matrix[(row + 1, col + 1)] = dist;

            if cost == 0 {
                last_match_col = col;
            }
        }

        last_row.insert(char_s, row);
    }

    let mut minimum = (u32::max_value(), 0);
    for x in 0..=m {
        let dist = matrix[(n + 1, x + 1)] as u32;
        if dist < minimum.0 {
            minimum = (dist, x);
        }
    }

    // everything was done characters wise and now we want to returns a number of bytes
    source.chars().take(minimum.1).map(|c| c.len_utf8()).sum()
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::str::from_utf8;

    use meilisearch_tokenizer::TokenKind;

    use super::*;
    use crate::MatchingWords;

    #[test]
    fn test_bytes_to_highlight() {
        struct TestBytesToHighlight {
            query: &'static str,
            text: &'static str,
            length: usize,
        }
        let tests = [
            TestBytesToHighlight { query: "bip", text: "bip", length: "bip".len() },
            TestBytesToHighlight { query: "bip", text: "boup", length: "bip".len() },
            TestBytesToHighlight {
                query: "Levenshtein",
                text: "Levenshtein",
                length: "Levenshtein".len(),
            },
            // we get to the end of our word with only one typo
            TestBytesToHighlight {
                query: "Levenste",
                text: "Levenshtein",
                length: "Levenste".len(),
            },
            // we get our third and last authorized typo right on the last character
            TestBytesToHighlight {
                query: "Levenstein",
                text: "Levenshte",
                length: "Levenste".len(),
            },
            // we get to the end of our word with only two typos at the beginning
            TestBytesToHighlight {
                query: "Bavenshtein",
                text: "Levenshtein",
                length: "Bavenshtein".len(),
            },
            TestBytesToHighlight {
                query: "Альфа", text: "Альфой", length: "Альф".len()
            },
            TestBytesToHighlight {
                query: "Go💼", text: "Go💼od luck.", length: "Go💼".len()
            },
            TestBytesToHighlight {
                query: "Go💼od", text: "Go💼od luck.", length: "Go💼od".len()
            },
            TestBytesToHighlight {
                query: "chäräcters",
                text: "chäräcters",
                length: "chäräcters".len(),
            },
            TestBytesToHighlight { query: "ch", text: "chäräcters", length: "ch".len() },
            TestBytesToHighlight { query: "chär", text: "chäräcters", length: "chär".len() },
        ];

        for test in &tests {
            let length = bytes_to_highlight(test.text, test.query);
            assert_eq!(length, test.length, r#"lenght between: "{}" "{}""#, test.query, test.text);
            assert!(
                from_utf8(&test.query.as_bytes()[..length]).is_ok(),
                r#"converting {}[..{}] to an utf8 str failed"#,
                test.query,
                length
            );
        }
    }

    #[test]
    fn matching_words() {
        let matching_words = vec![
            (vec![MatchingWord::new("split".to_string(), 1, true)], vec![0]),
            (vec![MatchingWord::new("this".to_string(), 0, false)], vec![1]),
            (vec![MatchingWord::new("world".to_string(), 1, true)], vec![2]),
        ];

        let matching_words = MatchingWords::new(matching_words);

        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    word: Cow::Borrowed("word"),
                    byte_start: 0,
                    char_index: 0,
                    byte_end: "word".len(),
                    char_map: None,
                })
                .next(),
            Some(MatchType::Full { char_len: 3, ids: &[2] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    word: Cow::Borrowed("nyc"),
                    byte_start: 0,
                    char_index: 0,
                    byte_end: "nyc".len(),
                    char_map: None,
                })
                .next(),
            None
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    word: Cow::Borrowed("world"),
                    byte_start: 0,
                    char_index: 0,
                    byte_end: "world".len(),
                    char_map: None,
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &[2] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    word: Cow::Borrowed("splitted"),
                    byte_start: 0,
                    char_index: 0,
                    byte_end: "splitted".len(),
                    char_map: None,
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &[0] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    word: Cow::Borrowed("thisnew"),
                    byte_start: 0,
                    char_index: 0,
                    byte_end: "thisnew".len(),
                    char_map: None,
                })
                .next(),
            None
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    word: Cow::Borrowed("borld"),
                    byte_start: 0,
                    char_index: 0,
                    byte_end: "borld".len(),
                    char_map: None,
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &[2] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    word: Cow::Borrowed("wordsplit"),
                    byte_start: 0,
                    char_index: 0,
                    byte_end: "wordsplit".len(),
                    char_map: None,
                })
                .next(),
            Some(MatchType::Full { char_len: 4, ids: &[2] })
        );
    }
}
