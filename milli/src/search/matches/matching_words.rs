use std::cmp::{min, Reverse};
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Index, IndexMut};
use std::rc::Rc;

use charabia::Token;
use levenshtein_automata::{Distance, DFA};

use crate::search::build_dfa;
use crate::MAX_WORD_LENGTH;

type IsPrefix = bool;

/// Structure created from a query tree
/// referencing words that match the given query tree.
#[derive(Default)]
pub struct MatchingWords {
    inner: Vec<(Vec<Rc<MatchingWord>>, Vec<PrimitiveWordId>)>,
}

impl fmt::Debug for MatchingWords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[")?;
        for (matching_words, primitive_word_id) in self.inner.iter() {
            writeln!(f, "({matching_words:?}, {primitive_word_id:?})")?;
        }
        writeln!(f, "]")?;
        Ok(())
    }
}

impl MatchingWords {
    pub fn new(mut matching_words: Vec<(Vec<Rc<MatchingWord>>, Vec<PrimitiveWordId>)>) -> Self {
        // Sort word by len in DESC order prioritizing the longuest matches,
        // in order to highlight the longuest part of the matched word.
        matching_words.sort_unstable_by_key(|(mw, _)| Reverse((mw.len(), mw[0].word.len())));

        Self { inner: matching_words }
    }

    /// Returns an iterator over terms that match or partially match the given token.
    pub fn match_token<'a, 'b>(&'a self, token: &'b Token<'b>) -> MatchesIter<'a, 'b> {
        MatchesIter { inner: Box::new(self.inner.iter()), token }
    }
}

/// Iterator over terms that match the given token,
/// This allow to lazily evaluate matches.
pub struct MatchesIter<'a, 'b> {
    #[allow(clippy::type_complexity)]
    inner: Box<dyn Iterator<Item = &'a (Vec<Rc<MatchingWord>>, Vec<PrimitiveWordId>)> + 'a>,
    token: &'b Token<'b>,
}

impl<'a> Iterator for MatchesIter<'a, '_> {
    type Item = MatchType<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((matching_words, ids)) => match matching_words[0].match_token(self.token) {
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

/// Id of a matching term corespounding to a word written by the end user.
pub type PrimitiveWordId = u8;

/// Structure used to match a specific term.
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
    pub fn new(word: String, typo: u8, prefix: IsPrefix) -> Option<Self> {
        if word.len() > MAX_WORD_LENGTH {
            return None;
        }
        let dfa = build_dfa(&word, typo, prefix);

        Some(Self { dfa, word, typo, prefix })
    }

    /// Returns the lenght in chars of the match in case of the token matches the term.
    pub fn match_token(&self, token: &Token) -> Option<usize> {
        match self.dfa.eval(token.lemma()) {
            Distance::Exact(t) if t <= self.typo => {
                if self.prefix {
                    let len = bytes_to_highlight(token.lemma(), &self.word);
                    Some(token.original_lengths(len).0)
                } else {
                    Some(token.original_lengths(token.lemma().len()).0)
                }
            }
            _otherwise => None,
        }
    }
}

/// A given token can partially match a query word for several reasons:
/// - split words
/// - multi-word synonyms
/// In these cases we need to match consecutively several tokens to consider that the match is full.
#[derive(Debug, PartialEq)]
pub enum MatchType<'a> {
    Full { char_len: usize, ids: &'a [PrimitiveWordId] },
    Partial(PartialMatch<'a>),
}

/// Structure helper to match several tokens in a row in order to complete a partial match.
#[derive(Debug, PartialEq)]
pub struct PartialMatch<'a> {
    matching_words: &'a [Rc<MatchingWord>],
    ids: &'a [PrimitiveWordId],
    char_len: usize,
}

impl<'a> PartialMatch<'a> {
    /// Returns:
    /// - None if the given token breaks the partial match
    /// - Partial if the given token matches the partial match but doesn't complete it
    /// - Full if the given token completes the partial match
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
            let cost = usize::from(char_s != char_t);

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

    use charabia::TokenKind;

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
        let all = vec![
            Rc::new(MatchingWord::new("split".to_string(), 1, true).unwrap()),
            Rc::new(MatchingWord::new("this".to_string(), 0, false).unwrap()),
            Rc::new(MatchingWord::new("world".to_string(), 1, true).unwrap()),
        ];
        let matching_words = vec![
            (vec![all[0].clone()], vec![0]),
            (vec![all[1].clone()], vec![1]),
            (vec![all[2].clone()], vec![2]),
        ];

        let matching_words = MatchingWords::new(matching_words);

        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("word"),
                    char_end: "word".chars().count(),
                    byte_end: "word".len(),
                    ..Default::default()
                })
                .next(),
            Some(MatchType::Full { char_len: 3, ids: &[2] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("nyc"),
                    char_end: "nyc".chars().count(),
                    byte_end: "nyc".len(),
                    ..Default::default()
                })
                .next(),
            None
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("world"),
                    char_end: "world".chars().count(),
                    byte_end: "world".len(),
                    ..Default::default()
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &[2] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("splitted"),
                    char_end: "splitted".chars().count(),
                    byte_end: "splitted".len(),
                    ..Default::default()
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &[0] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("thisnew"),
                    char_end: "thisnew".chars().count(),
                    byte_end: "thisnew".len(),
                    ..Default::default()
                })
                .next(),
            None
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("borld"),
                    char_end: "borld".chars().count(),
                    byte_end: "borld".len(),
                    ..Default::default()
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &[2] })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("wordsplit"),
                    char_end: "wordsplit".chars().count(),
                    byte_end: "wordsplit".len(),
                    ..Default::default()
                })
                .next(),
            Some(MatchType::Full { char_len: 4, ids: &[2] })
        );
    }
}
