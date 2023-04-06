use std::cmp::Reverse;
use std::ops::RangeInclusive;

use charabia::Token;

use super::super::interner::Interned;
use super::super::query_term::{
    Lazy, LocatedQueryTerm, OneTypoTerm, QueryTerm, TwoTypoTerm, ZeroTypoTerm,
};
use super::super::{DedupInterner, Phrase};
use crate::SearchContext;

pub struct LocatedMatchingPhrase {
    pub value: Interned<Phrase>,
    pub positions: RangeInclusive<WordId>,
}

pub struct LocatedMatchingWords {
    pub value: Vec<Interned<String>>,
    pub positions: RangeInclusive<WordId>,
    pub is_prefix: bool,
}

/// Structure created from a query tree
/// referencing words that match the given query tree.
pub struct MatchingWords<'ctx> {
    word_interner: &'ctx DedupInterner<String>,
    phrase_interner: &'ctx DedupInterner<Phrase>,
    phrases: Vec<LocatedMatchingPhrase>,
    words: Vec<LocatedMatchingWords>,
}

/// Extract and centralize the different phrases and words to match stored in a QueryTerm.
fn extract_matching_terms(term: &QueryTerm) -> (Vec<Interned<Phrase>>, Vec<Interned<String>>) {
    let mut matching_words = Vec::new();
    let mut matching_phrases = Vec::new();

    // the structure is exhaustively extracted to ensure that no field is missing.
    let QueryTerm {
        original: _,
        is_multiple_words: _,
        max_nbr_typos: _,
        is_prefix: _,
        zero_typo,
        one_typo,
        two_typo,
    } = term;

    // the structure is exhaustively extracted to ensure that no field is missing.
    let ZeroTypoTerm { phrase, zero_typo, prefix_of: _, synonyms, use_prefix_db: _ } = zero_typo;

    // zero typo
    if let Some(phrase) = phrase {
        matching_phrases.push(*phrase);
    }
    if let Some(zero_typo) = zero_typo {
        matching_words.push(*zero_typo);
    }
    for synonym in synonyms {
        matching_phrases.push(*synonym);
    }

    // one typo
    // the structure is exhaustively extracted to ensure that no field is missing.
    if let Lazy::Init(OneTypoTerm { split_words, one_typo }) = one_typo {
        if let Some(split_words) = split_words {
            matching_phrases.push(*split_words);
        }
        for one_typo in one_typo {
            matching_words.push(*one_typo);
        }
    }

    // two typos
    // the structure is exhaustively extracted to ensure that no field is missing.
    if let Lazy::Init(TwoTypoTerm { two_typos }) = two_typo {
        for two_typos in two_typos {
            matching_words.push(*two_typos);
        }
    }

    (matching_phrases, matching_words)
}

impl<'ctx> MatchingWords<'ctx> {
    pub fn new(ctx: &'ctx SearchContext, located_terms: Vec<LocatedQueryTerm>) -> Self {
        let mut phrases = Vec::new();
        let mut words = Vec::new();

        // Extract and centralize the different phrases and words to match stored in a QueryTerm using extract_matching_terms
        // and wrap them in dedicated structures.
        for located_term in located_terms {
            let term = ctx.term_interner.get(located_term.value);
            let (matching_phrases, matching_words) = extract_matching_terms(term);

            for matching_phrase in matching_phrases {
                phrases.push(LocatedMatchingPhrase {
                    value: matching_phrase,
                    positions: located_term.positions.clone(),
                });
            }
            words.push(LocatedMatchingWords {
                value: matching_words,
                positions: located_term.positions.clone(),
                is_prefix: term.is_prefix,
            });
        }

        // Sort word to put prefixes at the bottom prioritizing the exact matches.
        words.sort_unstable_by_key(|lmw| (lmw.is_prefix, Reverse(lmw.positions.len())));

        Self {
            phrases,
            words,
            word_interner: &ctx.word_interner,
            phrase_interner: &ctx.phrase_interner,
        }
    }

    /// Returns an iterator over terms that match or partially match the given token.
    pub fn match_token<'b>(&'ctx self, token: &'b Token<'b>) -> MatchesIter<'ctx, 'b> {
        MatchesIter { matching_words: self, phrases: Box::new(self.phrases.iter()), token }
    }

    /// Try to match the token with one of the located_words.
    fn match_unique_words(&'ctx self, token: &Token) -> Option<MatchType<'ctx>> {
        for located_words in &self.words {
            for word in &located_words.value {
                let word = self.word_interner.get(*word);
                // if the word is a prefix we match using starts_with.
                if located_words.is_prefix && token.lemma().starts_with(word) {
                    let char_len = token.original_lengths(word.len()).0;
                    let ids = &located_words.positions;
                    return Some(MatchType::Full { char_len, ids });
                // else we exact match the token.
                } else if token.lemma() == word {
                    let char_len = token.char_end - token.char_start;
                    let ids = &located_words.positions;
                    return Some(MatchType::Full { char_len, ids });
                }
            }
        }

        None
    }
}

/// Iterator over terms that match the given token,
/// This allow to lazily evaluate matches.
pub struct MatchesIter<'a, 'b> {
    matching_words: &'a MatchingWords<'a>,
    phrases: Box<dyn Iterator<Item = &'a LocatedMatchingPhrase> + 'a>,
    token: &'b Token<'b>,
}

impl<'a> Iterator for MatchesIter<'a, '_> {
    type Item = MatchType<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.phrases.next() {
            // Try to match all the phrases first.
            Some(located_phrase) => {
                let phrase = self.matching_words.phrase_interner.get(located_phrase.value);

                // create a PartialMatch struct to make it compute the first match
                // instead of duplicating the code.
                let ids = &located_phrase.positions;
                // collect the references of words from the interner.
                let words = phrase
                    .words
                    .iter()
                    .map(|word| {
                        word.map(|word| self.matching_words.word_interner.get(word).as_str())
                    })
                    .collect();
                let partial = PartialMatch { matching_words: words, ids, char_len: 0 };

                partial.match_token(self.token).or_else(|| self.next())
            }
            // If no phrases matches, try to match uiques words.
            None => self.matching_words.match_unique_words(self.token),
        }
    }
}

/// Id of a matching term corespounding to a word written by the end user.
pub type WordId = u16;

/// A given token can partially match a query word for several reasons:
/// - split words
/// - multi-word synonyms
/// In these cases we need to match consecutively several tokens to consider that the match is full.
#[derive(Debug, PartialEq)]
pub enum MatchType<'a> {
    Full { char_len: usize, ids: &'a RangeInclusive<WordId> },
    Partial(PartialMatch<'a>),
}

/// Structure helper to match several tokens in a row in order to complete a partial match.
#[derive(Debug, PartialEq)]
pub struct PartialMatch<'a> {
    matching_words: Vec<Option<&'a str>>,
    ids: &'a RangeInclusive<WordId>,
    char_len: usize,
}

impl<'a> PartialMatch<'a> {
    /// Returns:
    /// - None if the given token breaks the partial match
    /// - Partial if the given token matches the partial match but doesn't complete it
    /// - Full if the given token completes the partial match
    pub fn match_token(self, token: &Token) -> Option<MatchType<'a>> {
        let Self { mut matching_words, ids, .. } = self;

        let is_matching = match matching_words.first()? {
            Some(word) => &token.lemma() == word,
            // a None value in the phrase corresponds to a stop word,
            // the walue is considered a match if the current token is categorized as a stop word.
            None => token.is_stopword(),
        };

        let char_len = token.char_end - token.char_start;
        // if there are remaining words to match in the phrase and the current token is matching,
        // return a new Partial match allowing the highlighter to continue.
        if is_matching && matching_words.len() > 1 {
            matching_words.remove(0);
            Some(MatchType::Partial(PartialMatch { matching_words, ids, char_len }))
        // if there is no remaining word to match in the phrase and the current token is matching,
        // return a Full match.
        } else if is_matching {
            Some(MatchType::Full { char_len, ids })
        // if the current token doesn't match, return None to break the match sequence.
        } else {
            None
        }
    }

    pub fn char_len(&self) -> usize {
        self.char_len
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::borrow::Cow;

    use charabia::{TokenKind, TokenizerBuilder};

    use super::super::super::located_query_terms_from_string;
    use super::*;
    use crate::index::tests::TempIndex;

    pub(crate) fn temp_index_with_documents() -> TempIndex {
        let temp_index = TempIndex::new();
        temp_index
            .add_documents(documents!([
                { "id": 1, "name": "split this world westfali westfalia the" },
            ]))
            .unwrap();
        temp_index
    }

    #[test]
    fn matching_words() {
        let temp_index = temp_index_with_documents();
        let rtxn = temp_index.read_txn().unwrap();
        let mut ctx = SearchContext::new(&temp_index, &rtxn);
        let tokenizer = TokenizerBuilder::new().build();
        let tokens = tokenizer.tokenize("split this world");
        let query_terms = located_query_terms_from_string(&mut ctx, tokens, None).unwrap();
        let matching_words = MatchingWords::new(&ctx, query_terms);

        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("split"),
                    char_end: "split".chars().count(),
                    byte_end: "split".len(),
                    ..Default::default()
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &(0..=0) })
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
            Some(MatchType::Full { char_len: 5, ids: &(2..=2) })
        );
        assert_eq!(
            matching_words
                .match_token(&Token {
                    kind: TokenKind::Word,
                    lemma: Cow::Borrowed("worlded"),
                    char_end: "worlded".chars().count(),
                    byte_end: "worlded".len(),
                    ..Default::default()
                })
                .next(),
            Some(MatchType::Full { char_len: 5, ids: &(2..=2) })
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
    }
}
