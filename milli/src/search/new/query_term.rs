// TODO: put primitive query part in here

use std::borrow::Cow;
use std::mem;
use std::ops::RangeInclusive;

use charabia::normalizer::NormalizedTokenIter;
use charabia::{SeparatorKind, TokenKind};
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Streamer};
use heed::types::DecodeIgnore;
use heed::RoTxn;
use itertools::Itertools;

use crate::search::fst_utils::{Complement, Intersection, StartsWith, Union};
use crate::search::{build_dfa, get_first};
use crate::{CboRoaringBitmapLenCodec, Index, Result};

use super::interner::{Interned, Interner};
use super::SearchContext;

#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct Phrase {
    pub words: Vec<Option<Interned<String>>>,
}
impl Phrase {
    pub fn description(&self, interner: &Interner<String>) -> String {
        self.words.iter().flatten().map(|w| interner.get(*w)).join(" ")
    }
}

#[derive(Clone)]
pub struct WordDerivations {
    pub original: Interned<String>,
    // TODO: pub prefix_of: Vec<String>,
    pub synonyms: Box<[Interned<Phrase>]>,
    pub split_words: Option<Interned<Phrase>>,
    pub zero_typo: Box<[Interned<String>]>,
    pub one_typo: Box<[Interned<String>]>,
    pub two_typos: Box<[Interned<String>]>,
    pub use_prefix_db: bool,
}
impl WordDerivations {
    pub fn all_derivations_except_prefix_db(
        &'_ self,
    ) -> impl Iterator<Item = Interned<String>> + Clone + '_ {
        self.zero_typo.iter().chain(self.one_typo.iter()).chain(self.two_typos.iter()).copied()
    }
    fn is_empty(&self) -> bool {
        self.zero_typo.is_empty()
            && self.one_typo.is_empty()
            && self.two_typos.is_empty()
            && !self.use_prefix_db
    }
}

pub fn word_derivations(
    ctx: &mut SearchContext,
    word: &str,
    max_typo: u8,
    is_prefix: bool,
    fst: &fst::Set<Cow<[u8]>>,
) -> Result<WordDerivations> {
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
    // TODO: should there be SplitWord, NGram2, and NGram3 variants?
    // NGram2 can have 1 typo and synonyms
    // NGram3 cannot have typos but can have synonyms
    // SplitWords are a phrase
    // Can NGrams be prefixes?
    Phrase { phrase: Interned<Phrase> },
    Word { derivations: WordDerivations },
}

impl QueryTerm {
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

#[derive(Clone)]
pub struct LocatedQueryTerm {
    pub value: QueryTerm,
    pub positions: RangeInclusive<i8>,
}

impl LocatedQueryTerm {
    pub fn is_empty(&self) -> bool {
        match &self.value {
            QueryTerm::Phrase { phrase: _ } => false,
            QueryTerm::Word { derivations, .. } => derivations.is_empty(),
        }
    }
}

pub fn located_query_terms_from_string<'search>(
    ctx: &mut SearchContext<'search>,
    query: NormalizedTokenIter<Vec<u8>>,
    words_limit: Option<usize>,
) -> Result<Vec<LocatedQueryTerm>> {
    let authorize_typos = ctx.index.authorize_typos(ctx.txn)?;
    let min_len_one_typo = ctx.index.min_word_len_one_typo(ctx.txn)?;
    let min_len_two_typos = ctx.index.min_word_len_two_typos(ctx.txn)?;

    let exact_words = ctx.index.exact_words(ctx.txn)?;
    let fst = ctx.index.words_fst(ctx.txn)?;

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

    let mut primitive_query = Vec::new();
    let mut phrase = Vec::new();

    let mut quoted = false;

    let parts_limit = words_limit.unwrap_or(usize::MAX);

    let mut position = -1i8;
    let mut phrase_start = -1i8;
    let mut phrase_end = -1i8;

    let mut peekable = query.peekable();
    while let Some(token) = peekable.next() {
        // early return if word limit is exceeded
        if primitive_query.len() >= parts_limit {
            return Ok(primitive_query);
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
                            let derivations =
                                word_derivations(ctx, word, nbr_typos(word), false, &fst)?;
                            let located_term = LocatedQueryTerm {
                                value: QueryTerm::Word { derivations },
                                positions: position..=position,
                            };
                            primitive_query.push(located_term);
                        }
                        TokenKind::StopWord | TokenKind::Separator(_) | TokenKind::Unknown => {}
                    }
                } else {
                    let word = token.lemma();
                    let derivations = word_derivations(ctx, word, nbr_typos(word), true, &fst)?;
                    let located_term = LocatedQueryTerm {
                        value: QueryTerm::Word { derivations },
                        positions: position..=position,
                    };
                    primitive_query.push(located_term);
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
                    primitive_query.push(located_query_term);
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
        primitive_query.push(located_query_term);
    }

    Ok(primitive_query)
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
