use std::borrow::Cow;
use std::collections::BTreeSet;
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

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Lazy<T> {
    Uninit,
    Init(T),
}
impl<T> Lazy<T> {
    pub fn is_init(&self) -> bool {
        match self {
            Lazy::Uninit => false,
            Lazy::Init(_) => true,
        }
    }
    pub fn is_uninit(&self) -> bool {
        match self {
            Lazy::Uninit => true,
            Lazy::Init(_) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NTypoTermSubset {
    All,
    Subset {
        words: BTreeSet<Interned<String>>,
        phrases: BTreeSet<Interned<Phrase>>,
        // TODO: prefixes: BTreeSet<Interned<String>>,
    },
    Nothing,
}

impl NTypoTermSubset {
    pub fn contains_word(&self, word: Interned<String>) -> bool {
        match self {
            NTypoTermSubset::All => true,
            NTypoTermSubset::Subset { words, phrases: _ } => words.contains(&word),
            NTypoTermSubset::Nothing => false,
        }
    }
    pub fn contains_phrase(&self, phrase: Interned<Phrase>) -> bool {
        match self {
            NTypoTermSubset::All => true,
            NTypoTermSubset::Subset { words: _, phrases } => phrases.contains(&phrase),
            NTypoTermSubset::Nothing => false,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            NTypoTermSubset::All => false,
            NTypoTermSubset::Subset { words, phrases } => words.is_empty() && phrases.is_empty(),
            NTypoTermSubset::Nothing => true,
        }
    }
    pub fn union(&mut self, other: &Self) {
        match self {
            Self::All => {}
            Self::Subset { words, phrases } => match other {
                Self::All => {
                    *self = Self::All;
                }
                Self::Subset { words: w2, phrases: p2 } => {
                    words.extend(w2);
                    phrases.extend(p2);
                }
                Self::Nothing => {}
            },
            Self::Nothing => {
                *self = other.clone();
            }
        }
    }
    pub fn intersect(&mut self, other: &Self) {
        match self {
            Self::All => *self = other.clone(),
            Self::Subset { words, phrases } => match other {
                Self::All => {}
                Self::Subset { words: w2, phrases: p2 } => {
                    let mut ws = BTreeSet::new();
                    for w in words.intersection(w2) {
                        ws.insert(*w);
                    }
                    let mut ps = BTreeSet::new();
                    for p in phrases.intersection(p2) {
                        ps.insert(*p);
                    }
                    *words = ws;
                    *phrases = ps;
                }
                Self::Nothing => *self = Self::Nothing,
            },
            Self::Nothing => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryTermSubset {
    pub original: Interned<QueryTerm>,
    pub zero_typo_subset: NTypoTermSubset,
    pub one_typo_subset: NTypoTermSubset,
    pub two_typo_subset: NTypoTermSubset,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LocatedQueryTermSubset {
    pub term_subset: QueryTermSubset,
    pub positions: RangeInclusive<i8>,
    pub term_ids: RangeInclusive<u8>,
}

impl QueryTermSubset {
    pub fn empty(for_term: Interned<QueryTerm>) -> Self {
        Self {
            original: for_term,
            zero_typo_subset: NTypoTermSubset::Nothing,
            one_typo_subset: NTypoTermSubset::Nothing,
            two_typo_subset: NTypoTermSubset::Nothing,
        }
    }
    pub fn full(for_term: Interned<QueryTerm>) -> Self {
        Self {
            original: for_term,
            zero_typo_subset: NTypoTermSubset::All,
            one_typo_subset: NTypoTermSubset::All,
            two_typo_subset: NTypoTermSubset::All,
        }
    }

    pub fn union(&mut self, other: &Self) {
        assert!(self.original == other.original);
        self.zero_typo_subset.union(&other.zero_typo_subset);
        self.one_typo_subset.union(&other.one_typo_subset);
        self.two_typo_subset.union(&other.two_typo_subset);
    }
    pub fn intersect(&mut self, other: &Self) {
        assert!(self.original == other.original);
        self.zero_typo_subset.intersect(&other.zero_typo_subset);
        self.one_typo_subset.intersect(&other.one_typo_subset);
        self.two_typo_subset.intersect(&other.two_typo_subset);
    }

    pub fn use_prefix_db(&self, ctx: &SearchContext) -> Option<Interned<String>> {
        let original = ctx.term_interner.get(self.original);
        let Some(use_prefix_db) = original.zero_typo.use_prefix_db else {
            return None
        };
        match &self.zero_typo_subset {
            NTypoTermSubset::All => Some(use_prefix_db),
            NTypoTermSubset::Subset { words, phrases: _ } => {
                // TODO: use a subset of prefix words instead
                if words.contains(&use_prefix_db) {
                    Some(use_prefix_db)
                } else {
                    None
                }
            }
            NTypoTermSubset::Nothing => None,
        }
    }
    pub fn all_single_words_except_prefix_db(
        &self,
        ctx: &mut SearchContext,
    ) -> Result<BTreeSet<Interned<String>>> {
        let original = ctx.term_interner.get_mut(self.original);
        let mut result = BTreeSet::default();
        // TODO: a compute_partially funtion
        if !self.one_typo_subset.is_empty() || !self.two_typo_subset.is_empty() {
            original.compute_fully_if_needed(
                ctx.index,
                ctx.txn,
                &mut ctx.word_interner,
                &mut ctx.phrase_interner,
            )?;
        }

        if !self.zero_typo_subset.is_empty() {
            let ZeroTypoTerm { phrase: _, zero_typo, prefix_of, synonyms: _, use_prefix_db: _ } =
                &original.zero_typo;
            result.extend(zero_typo.iter().copied());
            result.extend(prefix_of.iter().copied());
        };

        match &self.one_typo_subset {
            NTypoTermSubset::All => {
                let Lazy::Init(OneTypoTerm { split_words: _, one_typo }) = &original.one_typo else {
                    panic!()
                };
                result.extend(one_typo.iter().copied())
            }
            NTypoTermSubset::Subset { words, phrases: _ } => {
                let Lazy::Init(OneTypoTerm { split_words: _, one_typo }) = &original.one_typo else {
                    panic!()
                };
                result.extend(one_typo.intersection(words));
            }
            NTypoTermSubset::Nothing => {}
        };

        match &self.two_typo_subset {
            NTypoTermSubset::All => {
                let Lazy::Init(TwoTypoTerm { two_typos }) = &original.two_typo else {
                    panic!()
                };
                result.extend(two_typos.iter().copied());
            }
            NTypoTermSubset::Subset { words, phrases: _ } => {
                let Lazy::Init(TwoTypoTerm { two_typos }) = &original.two_typo else {
                    panic!()
                };
                result.extend(two_typos.intersection(words));
            }
            NTypoTermSubset::Nothing => {}
        };

        Ok(result)
    }
    pub fn all_phrases(&self, ctx: &mut SearchContext) -> Result<BTreeSet<Interned<Phrase>>> {
        let original = ctx.term_interner.get_mut(self.original);
        let mut result = BTreeSet::default();

        if !self.one_typo_subset.is_empty() {
            // TODO: compute less than fully if possible
            original.compute_fully_if_needed(
                ctx.index,
                ctx.txn,
                &mut ctx.word_interner,
                &mut ctx.phrase_interner,
            )?;
        }

        let ZeroTypoTerm { phrase, zero_typo: _, prefix_of: _, synonyms, use_prefix_db: _ } =
            &original.zero_typo;
        result.extend(phrase.iter().copied());
        result.extend(synonyms.iter().copied());

        if !self.one_typo_subset.is_empty() {
            let Lazy::Init(OneTypoTerm { split_words, one_typo: _ }) = &original.one_typo else {
                panic!();
            };
            result.extend(split_words.iter().copied());
        }

        Ok(result)
    }
}

impl QueryTerm {
    pub fn compute_fully_if_needed(
        &mut self,
        index: &Index,
        txn: &RoTxn,
        word_interner: &mut DedupInterner<String>,
        phrase_interner: &mut DedupInterner<Phrase>,
    ) -> Result<()> {
        if self.max_nbr_typos == 0 {
            self.one_typo = Lazy::Init(OneTypoTerm::default());
            self.two_typo = Lazy::Init(TwoTypoTerm::default());
        } else if self.max_nbr_typos == 1 && self.one_typo.is_uninit() {
            assert!(self.two_typo.is_uninit());
            self.initialize_one_typo_subterm(index, txn, word_interner, phrase_interner)?;
            assert!(self.one_typo.is_init());
            self.two_typo = Lazy::Init(TwoTypoTerm::default());
        } else if self.max_nbr_typos > 1 && self.two_typo.is_uninit() {
            assert!(self.two_typo.is_uninit());
            self.initialize_one_and_two_typo_subterm(index, txn, word_interner, phrase_interner)?;
            assert!(self.one_typo.is_init() && self.two_typo.is_init());
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryTerm {
    pub original: Interned<String>,
    pub is_multiple_words: bool,
    pub max_nbr_typos: u8,
    pub is_prefix: bool,
    pub zero_typo: ZeroTypoTerm,
    // May not be computed yet
    pub one_typo: Lazy<OneTypoTerm>,
    // May not be computed yet
    pub two_typo: Lazy<TwoTypoTerm>,
}

// SubTerms will be in a dedup interner
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct ZeroTypoTerm {
    /// The original phrase, if any
    pub phrase: Option<Interned<Phrase>>,
    /// A single word equivalent to the original term, with zero typos
    pub zero_typo: Option<Interned<String>>,
    /// All the words that contain the original word as prefix
    pub prefix_of: BTreeSet<Interned<String>>,
    /// All the synonyms of the original word or phrase
    pub synonyms: BTreeSet<Interned<Phrase>>,
    /// A prefix in the prefix databases matching the original word
    pub use_prefix_db: Option<Interned<String>>,
}
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct OneTypoTerm {
    /// The original word split into multiple consecutive words
    pub split_words: Option<Interned<Phrase>>,
    /// Words that are 1 typo away from the original word
    pub one_typo: BTreeSet<Interned<String>>,
}
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct TwoTypoTerm {
    /// Words that are 2 typos away from the original word
    pub two_typos: BTreeSet<Interned<String>>,
}

impl ZeroTypoTerm {
    fn is_empty(&self) -> bool {
        let ZeroTypoTerm { phrase, zero_typo, prefix_of, synonyms, use_prefix_db } = self;
        phrase.is_none()
            && zero_typo.is_none()
            && prefix_of.is_empty()
            && synonyms.is_empty()
            && use_prefix_db.is_none()
    }
}
impl OneTypoTerm {
    fn is_empty(&self) -> bool {
        let OneTypoTerm { split_words, one_typo } = self;
        one_typo.is_empty() && split_words.is_none()
    }
}
impl TwoTypoTerm {
    fn is_empty(&self) -> bool {
        let TwoTypoTerm { two_typos } = self;
        two_typos.is_empty()
    }
}

impl QueryTerm {
    pub fn phrase(
        word_interner: &mut DedupInterner<String>,
        phrase_interner: &mut DedupInterner<Phrase>,
        phrase: Phrase,
    ) -> Self {
        Self {
            original: word_interner.insert(phrase.description(word_interner)),
            is_multiple_words: false,
            max_nbr_typos: 0,
            is_prefix: false,
            zero_typo: ZeroTypoTerm {
                phrase: Some(phrase_interner.insert(phrase)),
                zero_typo: None,
                prefix_of: BTreeSet::default(),
                synonyms: BTreeSet::default(),
                use_prefix_db: None,
            },
            one_typo: Lazy::Uninit,
            two_typo: Lazy::Uninit,
        }
    }
    pub fn empty(word_interner: &mut DedupInterner<String>, original: &str) -> Self {
        Self {
            original: word_interner.insert(original.to_owned()),
            is_multiple_words: false,
            is_prefix: false,
            max_nbr_typos: 0,
            zero_typo: <_>::default(),
            one_typo: Lazy::Init(<_>::default()),
            two_typo: Lazy::Init(<_>::default()),
        }
    }

    pub fn is_empty(&self) -> bool {
        let Lazy::Init(one_typo) = &self.one_typo else {
            return false;
        };
        let Lazy::Init(two_typo) = &self.two_typo else {
            return false;
        };

        self.zero_typo.is_empty() && one_typo.is_empty() && two_typo.is_empty()
    }
}

pub enum ZeroOrOneTypo {
    Zero,
    One,
}

fn find_zero_typo_prefix_derivations(
    word_interned: Interned<String>,
    fst: fst::Set<Cow<[u8]>>,
    word_interner: &mut DedupInterner<String>,
    mut visit: impl FnMut(Interned<String>) -> Result<()>,
) -> Result<()> {
    let word = word_interner.get(word_interned).to_owned();
    let word = word.as_str();
    let prefix = Str::new(word).starts_with();
    let mut stream = fst.search(prefix).into_stream();

    while let Some(derived_word) = stream.next() {
        let derived_word = std::str::from_utf8(derived_word)?.to_owned();
        let derived_word_interned = word_interner.insert(derived_word);
        if derived_word_interned != word_interned {
            visit(derived_word_interned)?;
        }
    }
    Ok(())
}

fn find_zero_one_typo_derivations(
    word_interned: Interned<String>,
    is_prefix: bool,
    fst: fst::Set<Cow<[u8]>>,
    word_interner: &mut DedupInterner<String>,
    mut visit: impl FnMut(Interned<String>, ZeroOrOneTypo) -> Result<()>,
) -> Result<()> {
    let word = word_interner.get(word_interned).to_owned();
    let word = word.as_str();

    let dfa = build_dfa(word, 1, is_prefix);
    let starts = StartsWith(Str::new(get_first(word)));
    let mut stream = fst.search_with_state(Intersection(starts, &dfa)).into_stream();
    // TODO: There may be wayyy too many matches (e.g. in the thousands), how to reduce them?

    while let Some((derived_word, state)) = stream.next() {
        let derived_word = std::str::from_utf8(derived_word)?;
        let derived_word = word_interner.insert(derived_word.to_owned());
        let d = dfa.distance(state.1);
        match d.to_u8() {
            0 => {
                if derived_word != word_interned {
                    visit(derived_word, ZeroOrOneTypo::Zero)?;
                }
            }
            1 => {
                visit(derived_word, ZeroOrOneTypo::One)?;
            }
            _ => panic!(),
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumberOfTypos {
    Zero,
    One,
    Two,
}
fn find_zero_one_two_typo_derivations(
    word_interned: Interned<String>,
    is_prefix: bool,
    fst: fst::Set<Cow<[u8]>>,
    word_interner: &mut DedupInterner<String>,
    mut visit: impl FnMut(Interned<String>, NumberOfTypos) -> Result<()>,
) -> Result<()> {
    let word = word_interner.get(word_interned).to_owned();
    let word = word.as_str();

    let starts = StartsWith(Str::new(get_first(word)));
    let first = Intersection(build_dfa(word, 1, is_prefix), Complement(&starts));
    let second_dfa = build_dfa(word, 2, is_prefix);
    let second = Intersection(&second_dfa, &starts);
    let automaton = Union(first, &second);

    let mut stream = fst.search_with_state(automaton).into_stream();
    // TODO: There may be wayyy too many matches (e.g. in the thousands), how to reduce them?

    while let Some((derived_word, state)) = stream.next() {
        let derived_word = std::str::from_utf8(derived_word)?;
        let derived_word_interned = word_interner.insert(derived_word.to_owned());
        // in the case the typo is on the first letter, we know the number of typo
        // is two
        if get_first(derived_word) != get_first(word) {
            visit(derived_word_interned, NumberOfTypos::Two)?;
        } else {
            // Else, we know that it is the second dfa that matched and compute the
            // correct distance
            let d = second_dfa.distance((state.1).0);
            match d.to_u8() {
                0 => {
                    if derived_word_interned != word_interned {
                        visit(derived_word_interned, NumberOfTypos::Zero)?;
                    }
                }
                1 => {
                    visit(derived_word_interned, NumberOfTypos::One)?;
                }
                2 => {
                    visit(derived_word_interned, NumberOfTypos::Two)?;
                }
                _ => panic!(),
            }
        }
    }
    Ok(())
}

fn partially_initialized_term_from_word(
    ctx: &mut SearchContext,
    word: &str,
    max_typo: u8,
    is_prefix: bool,
) -> Result<QueryTerm> {
    let word_interned = ctx.word_interner.insert(word.to_owned());

    if word.len() > MAX_WORD_LENGTH {
        return Ok(QueryTerm::empty(&mut ctx.word_interner, word));
    }

    let fst = ctx.index.words_fst(ctx.txn)?;

    let use_prefix_db = is_prefix
        && ctx
            .index
            .word_prefix_docids
            .remap_data_type::<DecodeIgnore>()
            .get(ctx.txn, word)?
            .is_some();
    let use_prefix_db = if use_prefix_db { Some(word_interned) } else { None };

    let mut zero_typo = None;
    let mut prefix_of = BTreeSet::new();

    if fst.contains(word) {
        zero_typo = Some(word_interned);
    }

    if is_prefix && use_prefix_db.is_none() {
        find_zero_typo_prefix_derivations(
            word_interned,
            fst,
            &mut ctx.word_interner,
            |derived_word| {
                prefix_of.insert(derived_word);
                Ok(())
            },
        )?;
    }
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
    let zero_typo = ZeroTypoTerm { phrase: None, zero_typo, prefix_of, synonyms, use_prefix_db };

    Ok(QueryTerm {
        original: word_interned,
        is_multiple_words: false,
        max_nbr_typos: max_typo,
        is_prefix,
        zero_typo,
        one_typo: Lazy::Uninit,
        two_typo: Lazy::Uninit,
    })
}

fn find_split_words(
    index: &Index,
    txn: &RoTxn,
    word_interner: &mut DedupInterner<String>,
    phrase_interner: &mut DedupInterner<Phrase>,
    word: &str,
) -> Result<Option<Interned<Phrase>>> {
    let split_words = split_best_frequency(index, txn, word)?.map(|(l, r)| {
        phrase_interner.insert(Phrase {
            words: vec![Some(word_interner.insert(l)), Some(word_interner.insert(r))],
        })
    });
    Ok(split_words)
}

impl QueryTerm {
    fn initialize_one_typo_subterm(
        &mut self,
        index: &Index,
        txn: &RoTxn,
        word_interner: &mut DedupInterner<String>,
        phrase_interner: &mut DedupInterner<Phrase>,
    ) -> Result<()> {
        let QueryTerm { original, is_prefix, one_typo, .. } = self;
        let original_str = word_interner.get(*original).to_owned();
        if one_typo.is_init() {
            return Ok(());
        }
        let mut one_typo_words = BTreeSet::new();

        find_zero_one_typo_derivations(
            *original,
            *is_prefix,
            index.words_fst(txn)?,
            word_interner,
            |derived_word, nbr_typos| {
                match nbr_typos {
                    ZeroOrOneTypo::Zero => {}
                    ZeroOrOneTypo::One => {
                        one_typo_words.insert(derived_word);
                    }
                }
                Ok(())
            },
        )?;
        let split_words =
            find_split_words(index, txn, word_interner, phrase_interner, original_str.as_str())?;
        let one_typo = OneTypoTerm { split_words, one_typo: one_typo_words };

        self.one_typo = Lazy::Init(one_typo);

        Ok(())
    }
    fn initialize_one_and_two_typo_subterm(
        &mut self,
        index: &Index,
        txn: &RoTxn,
        word_interner: &mut DedupInterner<String>,
        phrase_interner: &mut DedupInterner<Phrase>,
    ) -> Result<()> {
        let QueryTerm { original, is_prefix, two_typo, .. } = self;
        let original_str = word_interner.get(*original).to_owned();
        if two_typo.is_init() {
            return Ok(());
        }
        let mut one_typo_words = BTreeSet::new();
        let mut two_typo_words = BTreeSet::new();

        find_zero_one_two_typo_derivations(
            *original,
            *is_prefix,
            index.words_fst(txn)?,
            word_interner,
            |derived_word, nbr_typos| {
                match nbr_typos {
                    NumberOfTypos::Zero => {}
                    NumberOfTypos::One => {
                        one_typo_words.insert(derived_word);
                    }
                    NumberOfTypos::Two => {
                        two_typo_words.insert(derived_word);
                    }
                }
                Ok(())
            },
        )?;
        let split_words =
            find_split_words(index, txn, word_interner, phrase_interner, original_str.as_str())?;
        let one_typo = OneTypoTerm { one_typo: one_typo_words, split_words };

        let two_typo = TwoTypoTerm { two_typos: two_typo_words };

        self.one_typo = Lazy::Init(one_typo);
        self.two_typo = Lazy::Init(two_typo);

        Ok(())
    }
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
        if self.is_multiple_words {
            None
        } else {
            Some(self.original)
        }
    }
}

/// A query term term coupled with its position in the user's search query.
#[derive(Clone)]
pub struct LocatedQueryTerm {
    // should the query term subset really be interned?
    // possibly, yes
    pub value: Interned<QueryTerm>,
    pub positions: RangeInclusive<i8>,
}

/// Convert the tokenised search query into a list of located query terms.
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
                            let term = partially_initialized_term_from_word(
                                ctx,
                                word,
                                nbr_typos(word),
                                false,
                            )?;
                            let located_term = LocatedQueryTerm {
                                value: ctx.term_interner.push(term),
                                positions: position..=position,
                            };
                            located_terms.push(located_term);
                        }
                        TokenKind::StopWord | TokenKind::Separator(_) | TokenKind::Unknown => {}
                    }
                } else {
                    let word = token.lemma();
                    let term =
                        partially_initialized_term_from_word(ctx, word, nbr_typos(word), true)?;
                    let located_term = LocatedQueryTerm {
                        value: ctx.term_interner.push(term),
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
                        value: ctx.term_interner.push(QueryTerm::phrase(
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
            value: ctx.term_interner.push(QueryTerm::phrase(
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

    let max_nbr_typos =
        number_of_typos_allowed(ngram_str.as_str()).saturating_sub(terms.len() as u8 - 1);

    let mut term = partially_initialized_term_from_word(ctx, &ngram_str, max_nbr_typos, is_prefix)?;

    // let (_, mut zero_typo, mut one_typo, two_typo) =
    //     all_subterms_from_word(ctx, &ngram_str, max_nbr_typos, is_prefix)?;
    let original = ctx.word_interner.insert(words.join(" "));

    // Now add the synonyms
    let index_synonyms = ctx.index.synonyms(ctx.txn)?;

    term.zero_typo.synonyms.extend(
        index_synonyms.get(&words).cloned().unwrap_or_default().into_iter().map(|words| {
            let words = words.into_iter().map(|w| Some(ctx.word_interner.insert(w))).collect();
            ctx.phrase_interner.insert(Phrase { words })
        }),
    );

    let term = QueryTerm {
        original,
        is_multiple_words: true,
        is_prefix,
        max_nbr_typos,
        zero_typo: term.zero_typo,
        one_typo: Lazy::Uninit,
        two_typo: Lazy::Uninit,
    };

    let term = LocatedQueryTerm { value: ctx.term_interner.push(term), positions: start..=end };

    Ok(Some(term))
}
