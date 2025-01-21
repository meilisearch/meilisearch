use std::cmp::Reverse;
use std::fmt;
use std::ops::RangeInclusive;

use charabia::Token;

use super::super::interner::Interned;
use super::super::query_term::LocatedQueryTerm;
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
    pub original_char_count: usize,
}

/// Structure created from a query tree
/// referencing words that match the given query tree.
#[derive(Default)]
pub struct MatchingWords {
    word_interner: DedupInterner<String>,
    phrase_interner: DedupInterner<Phrase>,
    phrases: Vec<LocatedMatchingPhrase>,
    words: Vec<LocatedMatchingWords>,
}

impl MatchingWords {
    pub fn new(ctx: SearchContext<'_>, located_terms: Vec<LocatedQueryTerm>) -> Self {
        let mut phrases = Vec::new();
        let mut words = Vec::new();

        // Extract and centralize the different phrases and words to match stored in a QueryTerm
        // and wrap them in dedicated structures.
        for located_term in located_terms {
            let term = ctx.term_interner.get(located_term.value);
            let (matching_words, matching_phrases) = term.all_computed_derivations();

            for matching_phrase in matching_phrases {
                phrases.push(LocatedMatchingPhrase {
                    value: matching_phrase,
                    positions: located_term.positions.clone(),
                });
            }

            words.push(LocatedMatchingWords {
                value: matching_words,
                positions: located_term.positions.clone(),
                is_prefix: term.is_prefix(),
                original_char_count: term.original_word(&ctx).chars().count(),
            });
        }

        // Sort word to put prefixes at the bottom prioritizing the exact matches.
        words.sort_unstable_by_key(|lmw| (lmw.is_prefix, Reverse(lmw.positions.len())));

        Self {
            phrases,
            words,
            word_interner: ctx.word_interner,
            phrase_interner: ctx.phrase_interner,
        }
    }

    /// Returns an iterator over terms that match or partially match the given token.
    pub fn match_token<'a, 'b>(&'a self, token: &'b Token<'b>) -> MatchesIter<'a, 'b> {
        MatchesIter { matching_words: self, phrases: Box::new(self.phrases.iter()), token }
    }

    /// Try to match the token with one of the located_words.
    fn match_unique_words<'a>(&'a self, token: &Token<'_>) -> Option<MatchType<'a>> {
        for located_words in &self.words {
            for word in &located_words.value {
                let word = self.word_interner.get(*word);
                // if the word is a prefix we match using starts_with.
                if located_words.is_prefix && token.lemma().starts_with(word) {
                    let Some((char_index, c)) =
                        word.char_indices().take(located_words.original_char_count).last()
                    else {
                        continue;
                    };
                    let prefix_length = char_index + c.len_utf8();
                    let (char_count, byte_len) = token.original_lengths(prefix_length);
                    let ids = &located_words.positions;
                    return Some(MatchType::Full { ids, char_count, byte_len });
                // else we exact match the token.
                } else if token.lemma() == word {
                    let ids = &located_words.positions;
                    return Some(MatchType::Full {
                        char_count: token.char_end - token.char_start,
                        byte_len: token.byte_end - token.byte_start,
                        ids,
                    });
                }
            }
        }

        None
    }
}

/// Iterator over terms that match the given token,
/// This allow to lazily evaluate matches.
pub struct MatchesIter<'a, 'b> {
    matching_words: &'a MatchingWords,
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
                let partial = PartialMatch { matching_words: words, ids };

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
///   In these cases we need to match consecutively several tokens to consider that the match is full.
#[derive(Debug, PartialEq)]
pub enum MatchType<'a> {
    Full { char_count: usize, byte_len: usize, ids: &'a RangeInclusive<WordId> },
    Partial(PartialMatch<'a>),
}

/// Structure helper to match several tokens in a row in order to complete a partial match.
#[derive(Debug, PartialEq)]
pub struct PartialMatch<'a> {
    matching_words: Vec<Option<&'a str>>,
    ids: &'a RangeInclusive<WordId>,
}

impl<'a> PartialMatch<'a> {
    /// Returns:
    /// - None if the given token breaks the partial match
    /// - Partial if the given token matches the partial match but doesn't complete it
    /// - Full if the given token completes the partial match
    pub fn match_token(self, token: &Token<'_>) -> Option<MatchType<'a>> {
        let Self { mut matching_words, ids, .. } = self;

        let is_matching = match matching_words.first()? {
            Some(word) => &token.lemma() == word,
            // a None value in the phrase corresponds to a stop word,
            // the walue is considered a match if the current token is categorized as a stop word.
            None => token.is_stopword(),
        };

        // if there are remaining words to match in the phrase and the current token is matching,
        // return a new Partial match allowing the highlighter to continue.
        if is_matching && matching_words.len() > 1 {
            matching_words.remove(0);
            Some(MatchType::Partial(Self { matching_words, ids }))
        // if there is no remaining word to match in the phrase and the current token is matching,
        // return a Full match.
        } else if is_matching {
            Some(MatchType::Full {
                char_count: token.char_end - token.char_start,
                byte_len: token.byte_end - token.byte_start,
                ids,
            })
        // if the current token doesn't match, return None to break the match sequence.
        } else {
            None
        }
    }
}

impl fmt::Debug for MatchingWords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let MatchingWords { word_interner, phrase_interner, phrases, words } = self;

        let phrases: Vec<_> = phrases
            .iter()
            .map(|p| {
                (
                    phrase_interner
                        .get(p.value)
                        .words
                        .iter()
                        .map(|w| w.map_or("STOP_WORD", |w| word_interner.get(w)))
                        .collect::<Vec<_>>()
                        .join(" "),
                    p.positions.clone(),
                )
            })
            .collect();

        let words: Vec<_> = words
            .iter()
            .flat_map(|w| {
                w.value
                    .iter()
                    .map(|s| (word_interner.get(*s), w.positions.clone(), w.is_prefix))
                    .collect::<Vec<_>>()
            })
            .collect();

        f.debug_struct("MatchingWords").field("phrases", &phrases).field("words", &words).finish()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::borrow::Cow;

    use charabia::{TokenKind, TokenizerBuilder};

    use super::super::super::located_query_terms_from_tokens;
    use super::*;
    use crate::index::tests::TempIndex;
    use crate::search::new::query_term::ExtractedTokens;

    pub(crate) fn temp_index_with_documents() -> TempIndex {
        let temp_index = TempIndex::new();
        temp_index
            .add_documents(documents!([
                { "id": 1, "name": "split this world westfali westfalia the Ŵôřlḑôle" },
                { "id": 2, "name": "Westfália" },
                { "id": 3, "name": "Ŵôřlḑôle" },
            ]))
            .unwrap();
        temp_index
    }

    #[test]
    fn matching_words() {
        let temp_index = temp_index_with_documents();
        let rtxn = temp_index.read_txn().unwrap();
        let mut ctx = SearchContext::new(&temp_index, &rtxn).unwrap();
        let mut builder = TokenizerBuilder::default();
        let tokenizer = builder.build();
        let tokens = tokenizer.tokenize("split this world");
        let ExtractedTokens { query_terms, .. } =
            located_query_terms_from_tokens(&mut ctx, tokens, None).unwrap();
        let matching_words = MatchingWords::new(ctx, query_terms);

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
            Some(MatchType::Full { char_count: 5, byte_len: 5, ids: &(0..=0) })
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
            Some(MatchType::Full { char_count: 5, byte_len: 5, ids: &(2..=2) })
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
            Some(MatchType::Full { char_count: 5, byte_len: 5, ids: &(2..=2) })
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
