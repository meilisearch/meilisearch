use std::collections::HashSet;
use std::mem;
use std::ops::RangeInclusive;

use charabia::normalizer::NormalizedTokenIter;
use charabia::{SeparatorKind, TokenKind};
use fst::automaton::Str;
use fst::{Automaton, IntoStreamer, Streamer};
use heed::types::DecodeIgnore;
use heed::RoTxn;
use itertools::Itertools;

use super::interner::{DedupInterner, Interned};
use super::SearchContext;
use crate::search::fst_utils::{Complement, Intersection, StartsWith, Union};
use crate::search::{build_dfa, get_first};
use crate::{CboRoaringBitmapLenCodec, Index, Result, MAX_WORD_LENGTH};

/// A phrase in the user's search query, consisting of several words
/// that must appear side-by-side in the search results.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct Phrase {
    pub words: Vec<Option<Interned<String>>>,
}
impl Phrase {
    pub fn description(&self, interner: &DedupInterner<String>) -> String {
        self.words.iter().flatten().map(|w| interner.get(*w)).join(" ")
    }
}

/// A structure storing all the different ways to match
/// a term in the user's search query.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryTerm {
    /// The original terms, for debugging purposes
    pub original: Interned<String>,
    /// Whether the term is an ngram
    pub is_ngram: bool,
    /// Whether the term can be only the prefix of a word
    pub is_prefix: bool,
    /// The original phrase, if any
    pub phrase: Option<Interned<Phrase>>,
    /// A single word equivalent to the original term, with zero typos
    pub zero_typo: Option<Interned<String>>,
    /// All the words that contain the original word as prefix
    pub prefix_of: Box<[Interned<String>]>,
    /// All the synonyms of the original word or phrase
    pub synonyms: Box<[Interned<Phrase>]>,

    /// The original word split into multiple consecutive words
    pub split_words: Option<Interned<Phrase>>,

    /// Words that are 1 typo away from the original word
    pub one_typo: Box<[Interned<String>]>,

    /// Words that are 2 typos away from the original word
    pub two_typos: Box<[Interned<String>]>,

    /// A prefix in the prefix databases matching the original word
    pub use_prefix_db: Option<Interned<String>>,
}
impl QueryTerm {
    pub fn removing_forbidden_terms(
        &self,
        allowed_words: &HashSet<Interned<String>>,
        allowed_phrases: &HashSet<Interned<Phrase>>,
    ) -> Option<Self> {
        let QueryTerm {
            original,
            is_ngram,
            is_prefix,
            phrase,
            zero_typo,
            prefix_of,
            synonyms,
            split_words,
            one_typo,
            two_typos,
            use_prefix_db,
        } = self;

        let mut changed = false;

        let mut new_zero_typo = None;
        if let Some(w) = zero_typo {
            if allowed_words.contains(w) {
                new_zero_typo = Some(*w);
            } else {
                changed = true;
            }
        }
        // TODO: this is incorrect, prefix DB stuff should be treated separately
        let mut new_use_prefix_db = None;
        if let Some(w) = use_prefix_db {
            if allowed_words.contains(w) {
                new_use_prefix_db = Some(*w);
            } else {
                changed = true;
            }
        }
        let mut new_prefix_of = vec![];
        for w in prefix_of.iter() {
            if allowed_words.contains(w) {
                new_prefix_of.push(*w);
            } else {
                changed = true;
            }
        }
        let mut new_one_typo = vec![];
        for w in one_typo.iter() {
            if allowed_words.contains(w) {
                new_one_typo.push(*w);
            } else {
                changed = true;
            }
        }
        let mut new_two_typos = vec![];
        for w in two_typos.iter() {
            if allowed_words.contains(w) {
                new_two_typos.push(*w);
            } else {
                changed = true;
            }
        }
        // TODO: this is incorrect, prefix DB stuff should be treated separately
        let mut new_phrase = None;
        if let Some(w) = phrase {
            if !allowed_phrases.contains(w) {
                new_phrase = Some(*w);
            } else {
                changed = true;
            }
        }
        let mut new_split_words = None;
        if let Some(w) = split_words {
            if allowed_phrases.contains(w) {
                new_split_words = Some(*w);
            } else {
                changed = true;
            }
        }
        let mut new_synonyms = vec![];
        for w in synonyms.iter() {
            if allowed_phrases.contains(w) {
                new_synonyms.push(*w);
            } else {
                changed = true;
            }
        }
        if changed {
            Some(QueryTerm {
                original: *original,
                is_ngram: *is_ngram,
                is_prefix: *is_prefix,
                phrase: new_phrase,
                zero_typo: new_zero_typo,
                prefix_of: new_prefix_of.into_boxed_slice(),
                synonyms: new_synonyms.into_boxed_slice(),
                split_words: new_split_words,
                one_typo: new_one_typo.into_boxed_slice(),
                two_typos: new_two_typos.into_boxed_slice(),
                use_prefix_db: new_use_prefix_db,
            })
        } else {
            None
        }
    }
    pub fn phrase(
        word_interner: &mut DedupInterner<String>,
        phrase_interner: &mut DedupInterner<Phrase>,
        phrase: Phrase,
    ) -> Self {
        Self {
            original: word_interner.insert(phrase.description(word_interner)),
            phrase: Some(phrase_interner.insert(phrase)),
            is_prefix: false,
            zero_typo: None,
            prefix_of: Box::new([]),
            synonyms: Box::new([]),
            split_words: None,
            one_typo: Box::new([]),
            two_typos: Box::new([]),
            use_prefix_db: None,
            is_ngram: false,
        }
    }
    pub fn empty(word_interner: &mut DedupInterner<String>, original: &str) -> Self {
        Self {
            original: word_interner.insert(original.to_owned()),
            phrase: None,
            is_prefix: false,
            zero_typo: None,
            prefix_of: Box::new([]),
            synonyms: Box::new([]),
            split_words: None,
            one_typo: Box::new([]),
            two_typos: Box::new([]),
            use_prefix_db: None,
            is_ngram: false,
        }
    }
    /// Return an iterator over all the single words derived from the original word.
    ///
    /// This excludes synonyms, split words, and words stored in the prefix databases.
    pub fn all_single_words_except_prefix_db(
        &'_ self,
    ) -> impl Iterator<Item = Interned<String>> + Clone + '_ {
        self.zero_typo
            .iter()
            .chain(self.prefix_of.iter())
            .chain(self.one_typo.iter())
            .chain(self.two_typos.iter())
            .copied()
    }
    /// Return an iterator over all the single words derived from the original word.
    ///
    /// This excludes synonyms, split words, and words stored in the prefix databases.
    pub fn all_phrases(&'_ self) -> impl Iterator<Item = Interned<Phrase>> + Clone + '_ {
        self.split_words.iter().chain(self.synonyms.iter()).copied()
    }
    pub fn is_empty(&self) -> bool {
        self.zero_typo.is_none()
            && self.one_typo.is_empty()
            && self.two_typos.is_empty()
            && self.prefix_of.is_empty()
            && self.synonyms.is_empty()
            && self.split_words.is_none()
            && self.use_prefix_db.is_none()
    }
}

/// Compute the query term for the given word
pub fn query_term_from_word(
    ctx: &mut SearchContext,
    word: &str,
    max_typo: u8,
    is_prefix: bool,
) -> Result<QueryTerm> {
    if word.len() > MAX_WORD_LENGTH {
        return Ok(QueryTerm::empty(&mut ctx.word_interner, word));
    }

    let fst = ctx.index.words_fst(ctx.txn)?;
    let word_interned = ctx.word_interner.insert(word.to_owned());

    let use_prefix_db = is_prefix
        && ctx
            .index
            .word_prefix_docids
            .remap_data_type::<DecodeIgnore>()
            .get(ctx.txn, word)?
            .is_some();
    let use_prefix_db = if use_prefix_db { Some(word_interned) } else { None };

    let mut zero_typo = None;
    let mut prefix_of = vec![];
    let mut one_typo = vec![];
    let mut two_typos = vec![];

    if fst.contains(word) {
        zero_typo = Some(word_interned);
    }

    if max_typo == 0 {
        if is_prefix && use_prefix_db.is_none() {
            let prefix = Str::new(word).starts_with();
            let mut stream = fst.search(prefix).into_stream();

            while let Some(derived_word) = stream.next() {
                let derived_word = std::str::from_utf8(derived_word)?.to_owned();
                let derived_word_interned = ctx.word_interner.insert(derived_word);
                if derived_word_interned != word_interned {
                    prefix_of.push(derived_word_interned);
                }
            }
        }
    } else if max_typo == 1 {
        let dfa = build_dfa(word, 1, is_prefix);
        let starts = StartsWith(Str::new(get_first(word)));
        let mut stream = fst.search_with_state(Intersection(starts, &dfa)).into_stream();
        // TODO: There may be wayyy too many matches (e.g. in the thousands), how to reduce them?

        while let Some((derived_word, state)) = stream.next() {
            let derived_word = std::str::from_utf8(derived_word)?;

            let d = dfa.distance(state.1);
            let derived_word_interned = ctx.word_interner.insert(derived_word.to_owned());
            match d.to_u8() {
                0 => {
                    if derived_word_interned != word_interned {
                        prefix_of.push(derived_word_interned);
                    }
                }
                1 => {
                    one_typo.push(derived_word_interned);
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
        // TODO: There may be wayyy too many matches (e.g. in the thousands), how to reduce them?

        while let Some((derived_word, state)) = stream.next() {
            let derived_word = std::str::from_utf8(derived_word)?;
            let derived_word_interned = ctx.word_interner.insert(derived_word.to_owned());
            // in the case the typo is on the first letter, we know the number of typo
            // is two
            if get_first(derived_word) != get_first(word) {
                two_typos.push(derived_word_interned);
            } else {
                // Else, we know that it is the second dfa that matched and compute the
                // correct distance
                let d = second_dfa.distance((state.1).0);
                match d.to_u8() {
                    0 => {
                        if derived_word_interned != word_interned {
                            prefix_of.push(derived_word_interned);
                        }
                    }
                    1 => {
                        one_typo.push(derived_word_interned);
                    }
                    2 => {
                        two_typos.push(derived_word_interned);
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

    Ok(QueryTerm {
        original: word_interned,
        phrase: None,
        is_prefix,
        zero_typo,
        prefix_of: prefix_of.into_boxed_slice(),
        synonyms,
        split_words,
        one_typo: one_typo.into_boxed_slice(),
        two_typos: two_typos.into_boxed_slice(),
        use_prefix_db,
        is_ngram: false,
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

impl QueryTerm {
    /// Return the original word from the given query term
    pub fn original_single_word(&self) -> Option<Interned<String>> {
        if self.phrase.is_some() || self.is_ngram {
            None
        } else {
            Some(self.original)
        }
    }
}

/// A query term term coupled with its position in the user's search query.
#[derive(Clone)]
pub struct LocatedQueryTerm {
    pub value: Interned<QueryTerm>,
    // TODO: consider changing to u8, or even a u16
    pub positions: RangeInclusive<i8>,
}

impl LocatedQueryTerm {
    /// Return `true` iff the term is empty
    pub fn is_empty(&self, interner: &DedupInterner<QueryTerm>) -> bool {
        interner.get(self.value).is_empty()
    }
}

/// Convert the tokenised search query into a list of located query terms.
// TODO: checking if the positions are correct for phrases, separators, ngrams
// hard-limit the number of tokens that are considered
pub fn located_query_terms_from_string(
    ctx: &mut SearchContext,
    query: NormalizedTokenIter<&[u8]>,
    words_limit: Option<usize>,
) -> Result<Vec<LocatedQueryTerm>> {
    let nbr_typos = number_of_typos_allowed(ctx)?;

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
                        // otherwise return an empty term
                        phrase.push(Some(word));
                    }
                } else if peekable.peek().is_some() {
                    match token.kind {
                        TokenKind::Word => {
                            let word = token.lemma();
                            let term = query_term_from_word(ctx, word, nbr_typos(word), false)?;
                            let located_term = LocatedQueryTerm {
                                value: ctx.term_interner.insert(term),
                                positions: position..=position,
                            };
                            located_terms.push(located_term);
                        }
                        TokenKind::StopWord | TokenKind::Separator(_) | TokenKind::Unknown => {}
                    }
                } else {
                    let word = token.lemma();
                    // eagerly compute all derivations
                    let term = query_term_from_word(ctx, word, nbr_typos(word), true)?;
                    let located_term = LocatedQueryTerm {
                        value: ctx.term_interner.insert(term),
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
                // TODO: limit phrase size?
                if !phrase.is_empty() && (quote_count > 0 || separator_kind == SeparatorKind::Hard)
                {
                    let located_query_term = LocatedQueryTerm {
                        value: ctx.term_interner.insert(QueryTerm::phrase(
                            &mut ctx.word_interner,
                            &mut ctx.phrase_interner,
                            Phrase { words: mem::take(&mut phrase) },
                        )),
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
            value: ctx.term_interner.insert(QueryTerm::phrase(
                &mut ctx.word_interner,
                &mut ctx.phrase_interner,
                Phrase { words: mem::take(&mut phrase) },
            )),
            positions: phrase_start..=phrase_end,
        };
        located_terms.push(located_query_term);
    }

    Ok(located_terms)
}

pub fn number_of_typos_allowed<'ctx>(
    ctx: &SearchContext<'ctx>,
) -> Result<impl Fn(&str) -> u8 + 'ctx> {
    let authorize_typos = ctx.index.authorize_typos(ctx.txn)?;
    let min_len_one_typo = ctx.index.min_word_len_one_typo(ctx.txn)?;
    let min_len_two_typos = ctx.index.min_word_len_two_typos(ctx.txn)?;

    // TODO: should `exact_words` also disable prefix search, ngrams, split words, or synonyms?
    let exact_words = ctx.index.exact_words(ctx.txn)?;

    Ok(Box::new(move |word: &str| {
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
    }))
}

pub fn make_ngram(
    ctx: &mut SearchContext,
    terms: &[LocatedQueryTerm],
    number_of_typos_allowed: &impl Fn(&str) -> u8,
) -> Result<Option<LocatedQueryTerm>> {
    assert!(!terms.is_empty());
    for ts in terms.windows(2) {
        let [t1, t2] = ts else { panic!() };
        if *t1.positions.end() != t2.positions.start() - 1 {
            return Ok(None);
        }
    }
    let mut words_interned = vec![];
    for term in terms {
        if let Some(original_term_word) = ctx.term_interner.get(term.value).original_single_word() {
            words_interned.push(original_term_word);
        } else {
            return Ok(None);
        }
    }
    let words =
        words_interned.iter().map(|&i| ctx.word_interner.get(i).to_owned()).collect::<Vec<_>>();

    let start = *terms.first().as_ref().unwrap().positions.start();
    let end = *terms.last().as_ref().unwrap().positions.end();
    let is_prefix = ctx.term_interner.get(terms.last().as_ref().unwrap().value).is_prefix;
    let ngram_str = words.join("");
    if ngram_str.len() > MAX_WORD_LENGTH {
        return Ok(None);
    }

    let mut term = query_term_from_word(
        ctx,
        &ngram_str,
        number_of_typos_allowed(ngram_str.as_str()).saturating_sub(terms.len() as u8),
        is_prefix,
    )?;
    term.original = ctx.word_interner.insert(words.join(" "));
    // Now add the synonyms
    let index_synonyms = ctx.index.synonyms(ctx.txn)?;
    let mut term_synonyms = term.synonyms.to_vec();
    term_synonyms.extend(index_synonyms.get(&words).cloned().unwrap_or_default().into_iter().map(
        |words| {
            let words = words.into_iter().map(|w| Some(ctx.word_interner.insert(w))).collect();
            ctx.phrase_interner.insert(Phrase { words })
        },
    ));
    term.synonyms = term_synonyms.into_boxed_slice();
    if let Some(split_words) = term.split_words {
        let split_words = ctx.phrase_interner.get(split_words);
        if split_words.words == words_interned.iter().map(|&i| Some(i)).collect::<Vec<_>>() {
            term.split_words = None;
        }
    }
    if term.is_empty() {
        return Ok(None);
    }
    term.is_ngram = true;
    let term = LocatedQueryTerm { value: ctx.term_interner.insert(term), positions: start..=end };

    Ok(Some(term))
}
