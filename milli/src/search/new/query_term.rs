use std::mem;
use std::ops::RangeInclusive;

use charabia::normalizer::NormalizedTokenIter;
use charabia::{SeparatorKind, TokenKind};
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Streamer};
use heed::types::DecodeIgnore;
use heed::RoTxn;
use itertools::Itertools;

use super::interner::{Interned, Interner};
use super::SearchContext;
use crate::search::fst_utils::{Complement, Intersection, StartsWith, Union};
use crate::search::{build_dfa, get_first};
use crate::{CboRoaringBitmapLenCodec, Index, Result};

/// A phrase in the user's search query, consisting of several words
/// that must appear side-by-side in the search results.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct Phrase {
    pub words: Vec<Option<Interned<String>>>,
}
impl Phrase {
    pub fn description(&self, interner: &Interner<String>) -> String {
        self.words.iter().flatten().map(|w| interner.get(*w)).join(" ")
    }
}

/// A structure storing all the different ways to match
/// a term in the user's search query.
#[derive(Clone)]
pub struct WordDerivations {
    /// The original word
    pub original: Interned<String>,
    // TODO: original should only be used for debugging purposes?
    // TODO: pub zero_typo: Option<Interned<String>>,
    // TODO: pub prefix_of: Box<[Interned<String>]>,
    /// All the synonyms of the original word
    pub synonyms: Box<[Interned<Phrase>]>,

    /// The original word split into multiple consecutive words
    pub split_words: Option<Interned<Phrase>>,

    /// The original words and words which are prefixed by it
    pub zero_typo: Box<[Interned<String>]>,

    /// Words that are 1 typo away from the original word
    pub one_typo: Box<[Interned<String>]>,

    /// Words that are 2 typos away from the original word
    pub two_typos: Box<[Interned<String>]>,

    /// True if the prefix databases must be used to retrieve
    /// the words which are prefixed by the original word.
    pub use_prefix_db: bool,
}
impl WordDerivations {
    /// Return an iterator over all the single words derived from the original word.
    ///
    /// This excludes synonyms, split words, and words stored in the prefix databases.
    pub fn all_derivations_except_prefix_db(
        &'_ self,
    ) -> impl Iterator<Item = Interned<String>> + Clone + '_ {
        self.zero_typo.iter().chain(self.one_typo.iter()).chain(self.two_typos.iter()).copied()
    }
    fn is_empty(&self) -> bool {
        self.zero_typo.is_empty()
            && self.one_typo.is_empty()
            && self.two_typos.is_empty()
            && self.synonyms.is_empty()
            && self.split_words.is_none()
            && !self.use_prefix_db
    }
}

/// Compute the word derivations for the given word
pub fn word_derivations(
    ctx: &mut SearchContext,
    word: &str,
    max_typo: u8,
    is_prefix: bool,
) -> Result<WordDerivations> {
    let fst = ctx.index.words_fst(ctx.txn)?;
    let word_interned = ctx.word_interner.insert(word.to_owned());

    let use_prefix_db = is_prefix
        && ctx
            .index
            .word_prefix_docids
            .remap_data_type::<DecodeIgnore>()
            .get(ctx.txn, word)?
            .is_some();

    let mut zero_typo = vec![];
    let mut one_typo = vec![];
    let mut two_typos = vec![];

    if max_typo == 0 {
        if is_prefix && !use_prefix_db {
            let prefix = Str::new(word).starts_with();
            let mut stream = fst.search(prefix).into_stream();

            while let Some(word) = stream.next() {
                let word = std::str::from_utf8(word)?.to_owned();
                let word_interned = ctx.word_interner.insert(word);
                zero_typo.push(word_interned);
            }
        } else if fst.contains(word) {
            zero_typo.push(word_interned);
        }
    } else if max_typo == 1 {
        let dfa = build_dfa(word, 1, is_prefix);
        let starts = StartsWith(Str::new(get_first(word)));
        let mut stream = fst.search_with_state(Intersection(starts, &dfa)).into_stream();

        while let Some((word, state)) = stream.next() {
            let word = std::str::from_utf8(word)?;
            let word_interned = ctx.word_interner.insert(word.to_owned());
            let d = dfa.distance(state.1);
            match d.to_u8() {
                0 => {
                    zero_typo.push(word_interned);
                }
                1 => {
                    one_typo.push(word_interned);
                }
                _ => panic!(),
            }
        }
    } else {
        let starts = StartsWith(Str::new(get_first(word)));
        let first = Intersection(build_dfa(word, 1, is_prefix), Complement(&starts));
        let second_dfa = build_dfa(word, 2, is_prefix);
        let second = Intersection(&second_dfa, &starts);
        let automaton = Union(first, &second);

        let mut stream = fst.search_with_state(automaton).into_stream();

        while let Some((found_word, state)) = stream.next() {
            let found_word = std::str::from_utf8(found_word)?;
            let found_word_interned = ctx.word_interner.insert(found_word.to_owned());
            // in the case the typo is on the first letter, we know the number of typo
            // is two
            if get_first(found_word) != get_first(word) {
                two_typos.push(found_word_interned);
            } else {
                // Else, we know that it is the second dfa that matched and compute the
                // correct distance
                let d = second_dfa.distance((state.1).0);
                match d.to_u8() {
                    0 => {
                        zero_typo.push(found_word_interned);
                    }
                    1 => {
                        one_typo.push(found_word_interned);
                    }
                    2 => {
                        two_typos.push(found_word_interned);
                    }
                    _ => panic!(),
                }
            }
        }
    }
    let split_words = split_best_frequency(ctx.index, ctx.txn, word)?.map(|(l, r)| {
        ctx.phrase_interner.insert(Phrase {
            words: vec![Some(ctx.word_interner.insert(l)), Some(ctx.word_interner.insert(r))],
        })
    });

    let synonyms = ctx.index.synonyms(ctx.txn)?;

    let synonyms = synonyms
        .get(&vec![word.to_owned()])
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|words| {
            let words = words.into_iter().map(|w| Some(ctx.word_interner.insert(w))).collect();
            ctx.phrase_interner.insert(Phrase { words })
        })
        .collect();

    Ok(WordDerivations {
        original: ctx.word_interner.insert(word.to_owned()),
        synonyms,
        split_words,
        zero_typo: zero_typo.into_boxed_slice(),
        one_typo: one_typo.into_boxed_slice(),
        two_typos: two_typos.into_boxed_slice(),
        use_prefix_db,
    })
}

/// Split the original word into the two words that appear the
/// most next to each other in the index.
///
/// Return `None` if the original word cannot be split.
fn split_best_frequency(
    index: &Index,
    txn: &RoTxn,
    original: &str,
) -> Result<Option<(String, String)>> {
    let chars = original.char_indices().skip(1);
    let mut best = None;

    for (i, _) in chars {
        let (left, right) = original.split_at(i);

        let key = (1, left, right);
        let frequency = index
            .word_pair_proximity_docids
            .remap_data_type::<CboRoaringBitmapLenCodec>()
            .get(txn, &key)?
            .unwrap_or(0);

        if frequency != 0 && best.map_or(true, |(old, _, _)| frequency > old) {
            best = Some((frequency, left, right));
        }
    }

    Ok(best.map(|(_, left, right)| (left.to_owned(), right.to_owned())))
}

#[derive(Clone)]
pub enum QueryTerm {
    Phrase { phrase: Interned<Phrase> },
    Word { derivations: WordDerivations },
}

impl QueryTerm {
    /// Return the original word from the given query term
    pub fn original_single_word<'interner>(
        &self,
        word_interner: &'interner Interner<String>,
    ) -> Option<&'interner str> {
        match self {
            QueryTerm::Phrase { phrase: _ } => None,
            QueryTerm::Word { derivations } => {
                if derivations.is_empty() {
                    None
                } else {
                    Some(word_interner.get(derivations.original))
                }
            }
        }
    }
}

/// A query term term coupled with its position in the user's search query.
#[derive(Clone)]
pub struct LocatedQueryTerm {
    pub value: QueryTerm,
    pub positions: RangeInclusive<i8>,
}

impl LocatedQueryTerm {
    /// Return `true` iff the word derivations within the query term are empty
    pub fn is_empty(&self) -> bool {
        match &self.value {
            // TODO: phrases should be greedily computed, so that they can be excluded from
            // the query graph right from the start?
            QueryTerm::Phrase { phrase: _ } => false,
            QueryTerm::Word { derivations, .. } => derivations.is_empty(),
        }
    }
}

/// Convert the tokenised search query into a list of located query terms.
pub fn located_query_terms_from_string<'search>(
    ctx: &mut SearchContext<'search>,
    query: NormalizedTokenIter<Vec<u8>>,
    words_limit: Option<usize>,
) -> Result<Vec<LocatedQueryTerm>> {
    let authorize_typos = ctx.index.authorize_typos(ctx.txn)?;
    let min_len_one_typo = ctx.index.min_word_len_one_typo(ctx.txn)?;
    let min_len_two_typos = ctx.index.min_word_len_two_typos(ctx.txn)?;

    // TODO: should `exact_words` also disable prefix search, ngrams, split words, or synonyms?
    let exact_words = ctx.index.exact_words(ctx.txn)?;

    let nbr_typos = |word: &str| {
        if !authorize_typos
            || word.len() < min_len_one_typo as usize
            || exact_words.as_ref().map_or(false, |fst| fst.contains(word))
        {
            0
        } else if word.len() < min_len_two_typos as usize {
            1
        } else {
            2
        }
    };

    let mut located_terms = Vec::new();

    let mut phrase = Vec::new();
    let mut quoted = false;

    let parts_limit = words_limit.unwrap_or(usize::MAX);

    let mut position = -1i8;
    let mut phrase_start = -1i8;
    let mut phrase_end = -1i8;

    let mut peekable = query.peekable();
    while let Some(token) = peekable.next() {
        // early return if word limit is exceeded
        if located_terms.len() >= parts_limit {
            return Ok(located_terms);
        }

        match token.kind {
            TokenKind::Word | TokenKind::StopWord => {
                position += 1;
                // 1. if the word is quoted we push it in a phrase-buffer waiting for the ending quote,
                // 2. if the word is not the last token of the query and is not a stop_word we push it as a non-prefix word,
                // 3. if the word is the last token of the query we push it as a prefix word.
                if quoted {
                    phrase_end = position;
                    if phrase.is_empty() {
                        phrase_start = position;
                    }
                    if let TokenKind::StopWord = token.kind {
                        phrase.push(None);
                    } else {
                        let word = ctx.word_interner.insert(token.lemma().to_string());
                        // TODO: in a phrase, check that every word exists
                        // otherwise return WordDerivations::Empty
                        phrase.push(Some(word));
                    }
                } else if peekable.peek().is_some() {
                    match token.kind {
                        TokenKind::Word => {
                            let word = token.lemma();
                            let derivations = word_derivations(ctx, word, nbr_typos(word), false)?;
                            let located_term = LocatedQueryTerm {
                                value: QueryTerm::Word { derivations },
                                positions: position..=position,
                            };
                            located_terms.push(located_term);
                        }
                        TokenKind::StopWord | TokenKind::Separator(_) | TokenKind::Unknown => {}
                    }
                } else {
                    let word = token.lemma();
                    let derivations = word_derivations(ctx, word, nbr_typos(word), true)?;
                    let located_term = LocatedQueryTerm {
                        value: QueryTerm::Word { derivations },
                        positions: position..=position,
                    };
                    located_terms.push(located_term);
                }
            }
            TokenKind::Separator(separator_kind) => {
                match separator_kind {
                    SeparatorKind::Hard => {
                        position += 1;
                    }
                    SeparatorKind::Soft => {
                        position += 0;
                    }
                }
                let quote_count = token.lemma().chars().filter(|&s| s == '"').count();
                // swap quoted state if we encounter a double quote
                if quote_count % 2 != 0 {
                    quoted = !quoted;
                }
                // if there is a quote or a hard separator we close the phrase.
                if !phrase.is_empty() && (quote_count > 0 || separator_kind == SeparatorKind::Hard)
                {
                    let located_query_term = LocatedQueryTerm {
                        value: QueryTerm::Phrase {
                            phrase: ctx
                                .phrase_interner
                                .insert(Phrase { words: mem::take(&mut phrase) }),
                        },
                        positions: phrase_start..=phrase_end,
                    };
                    located_terms.push(located_query_term);
                }
            }
            _ => (),
        }
    }

    // If a quote is never closed, we consider all of the end of the query as a phrase.
    if !phrase.is_empty() {
        let located_query_term = LocatedQueryTerm {
            value: QueryTerm::Phrase {
                phrase: ctx.phrase_interner.insert(Phrase { words: mem::take(&mut phrase) }),
            },
            positions: phrase_start..=phrase_end,
        };
        located_terms.push(located_query_term);
    }

    Ok(located_terms)
}

// TODO: return a word derivations instead?
pub fn ngram2(
    ctx: &mut SearchContext,
    x: &LocatedQueryTerm,
    y: &LocatedQueryTerm,
) -> Option<(Interned<String>, RangeInclusive<i8>)> {
    if *x.positions.end() != y.positions.start() - 1 {
        return None;
    }
    match (
        &x.value.original_single_word(&ctx.word_interner),
        &y.value.original_single_word(&ctx.word_interner),
    ) {
        (Some(w1), Some(w2)) => {
            let term = (
                ctx.word_interner.insert(format!("{w1}{w2}")),
                *x.positions.start()..=*y.positions.end(),
            );
            Some(term)
        }
        _ => None,
    }
}

// TODO: return a word derivations instead?
pub fn ngram3(
    ctx: &mut SearchContext,
    x: &LocatedQueryTerm,
    y: &LocatedQueryTerm,
    z: &LocatedQueryTerm,
) -> Option<(Interned<String>, RangeInclusive<i8>)> {
    if *x.positions.end() != y.positions.start() - 1
        || *y.positions.end() != z.positions.start() - 1
    {
        return None;
    }
    match (
        &x.value.original_single_word(&ctx.word_interner),
        &y.value.original_single_word(&ctx.word_interner),
        &z.value.original_single_word(&ctx.word_interner),
    ) {
        (Some(w1), Some(w2), Some(w3)) => {
            let term = (
                ctx.word_interner.insert(format!("{w1}{w2}{w3}")),
                *x.positions.start()..=*z.positions.end(),
            );
            Some(term)
        }
        _ => None,
    }
}
