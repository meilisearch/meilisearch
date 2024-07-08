mod compute_derivations;
mod ntypo_subset;
mod parse_query;
mod phrase;

use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::ops::RangeInclusive;

use either::Either;
pub use ntypo_subset::NTypoTermSubset;
pub use parse_query::{
    located_query_terms_from_tokens, make_ngram, number_of_typos_allowed, ExtractedTokens,
};
pub use phrase::Phrase;

use super::interner::{DedupInterner, Interned};
use super::{limits, SearchContext, Word};
use crate::Result;

/// A set of word derivations attached to a location in the search query.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct LocatedQueryTermSubset {
    pub term_subset: QueryTermSubset,
    pub positions: RangeInclusive<u16>,
    pub term_ids: RangeInclusive<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryTermSubset {
    original: Interned<QueryTerm>,
    zero_typo_subset: NTypoTermSubset,
    one_typo_subset: NTypoTermSubset,
    two_typo_subset: NTypoTermSubset,
    /// `true` if the term cannot be deleted through the term matching strategy
    ///
    /// Note that there are other reasons for which a term cannot be deleted, such as
    /// being a phrase. In that case, this field could be set to `false`, but it
    /// still wouldn't be deleteable by the term matching strategy.
    mandatory: bool,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryTerm {
    original: Interned<String>,
    ngram_words: Option<Vec<Interned<String>>>,
    max_levenshtein_distance: u8,
    is_prefix: bool,
    zero_typo: ZeroTypoTerm,
    // May not be computed yet
    one_typo: Lazy<OneTypoTerm>,
    // May not be computed yet
    two_typo: Lazy<TwoTypoTerm>,
}

// SubTerms will be in a dedup interner
#[derive(Default, Clone, PartialEq, Eq, Hash)]
struct ZeroTypoTerm {
    /// The original phrase, if any
    phrase: Option<Interned<Phrase>>,
    /// A single word equivalent to the original term, with zero typos
    exact: Option<Interned<String>>,
    /// All the words that contain the original word as prefix
    prefix_of: BTreeSet<Interned<String>>,
    /// All the synonyms of the original word or phrase
    synonyms: BTreeSet<Interned<Phrase>>,
    /// A prefix in the prefix databases matching the original word
    use_prefix_db: Option<Interned<String>>,
}
#[derive(Default, Clone, PartialEq, Eq, Hash)]
struct OneTypoTerm {
    /// The original word split into multiple consecutive words
    split_words: Option<Interned<Phrase>>,
    /// Words that are 1 typo away from the original word
    one_typo: BTreeSet<Interned<String>>,
}
#[derive(Default, Clone, PartialEq, Eq, Hash)]
struct TwoTypoTerm {
    /// Words that are 2 typos away from the original word
    two_typos: BTreeSet<Interned<String>>,
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

#[derive(Clone, Copy)]
pub enum ExactTerm {
    Phrase(Interned<Phrase>),
    Word(Interned<String>),
}

impl ExactTerm {
    pub fn interned_words<'ctx>(
        &self,
        ctx: &'ctx SearchContext<'ctx>,
    ) -> impl Iterator<Item = Option<Interned<String>>> + 'ctx {
        match *self {
            ExactTerm::Phrase(phrase) => {
                let phrase = ctx.phrase_interner.get(phrase);
                Either::Left(phrase.words.iter().copied())
            }
            ExactTerm::Word(word) => Either::Right(std::iter::once(Some(word))),
        }
    }
}

impl QueryTermSubset {
    pub fn is_mandatory(&self) -> bool {
        self.mandatory
    }
    pub fn make_mandatory(&mut self) {
        self.mandatory = true;
    }
    pub fn exact_term(&self, ctx: &SearchContext<'_>) -> Option<ExactTerm> {
        let full_query_term = ctx.term_interner.get(self.original);
        if full_query_term.ngram_words.is_some() {
            return None;
        }
        if let Some(phrase) = full_query_term.zero_typo.phrase {
            self.zero_typo_subset.contains_phrase(phrase).then_some(ExactTerm::Phrase(phrase))
        } else if let Some(word) = full_query_term.zero_typo.exact {
            self.zero_typo_subset.contains_word(word).then_some(ExactTerm::Word(word))
        } else {
            None
        }
    }

    pub fn empty(for_term: Interned<QueryTerm>) -> Self {
        Self {
            original: for_term,
            zero_typo_subset: NTypoTermSubset::Nothing,
            one_typo_subset: NTypoTermSubset::Nothing,
            two_typo_subset: NTypoTermSubset::Nothing,
            mandatory: false,
        }
    }
    pub fn full(for_term: Interned<QueryTerm>) -> Self {
        Self {
            original: for_term,
            zero_typo_subset: NTypoTermSubset::All,
            one_typo_subset: NTypoTermSubset::All,
            two_typo_subset: NTypoTermSubset::All,
            mandatory: false,
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

    pub fn use_prefix_db(&self, ctx: &SearchContext<'_>) -> Option<Word> {
        let original = ctx.term_interner.get(self.original);
        let use_prefix_db = original.zero_typo.use_prefix_db?;
        let word = match &self.zero_typo_subset {
            NTypoTermSubset::All => Some(use_prefix_db),
            NTypoTermSubset::Subset { words, phrases: _ } => {
                if words.contains(&use_prefix_db) {
                    Some(use_prefix_db)
                } else {
                    None
                }
            }
            NTypoTermSubset::Nothing => None,
        };
        word.map(|word| {
            if original.ngram_words.is_some() {
                Word::Derived(word)
            } else {
                Word::Original(word)
            }
        })
    }
    pub fn all_single_words_except_prefix_db(
        &self,
        ctx: &mut SearchContext<'_>,
    ) -> Result<BTreeSet<Word>> {
        let mut result = BTreeSet::default();
        if !self.one_typo_subset.is_empty() || !self.two_typo_subset.is_empty() {
            self.original.compute_fully_if_needed(ctx)?;
        }

        let original = ctx.term_interner.get_mut(self.original);
        match &self.zero_typo_subset {
            NTypoTermSubset::All => {
                let ZeroTypoTerm {
                    phrase: _,
                    exact: zero_typo,
                    prefix_of,
                    synonyms: _,
                    use_prefix_db: _,
                } = &original.zero_typo;
                result.extend(zero_typo.iter().copied().map(|w| {
                    if original.ngram_words.is_some() {
                        Word::Derived(w)
                    } else {
                        Word::Original(w)
                    }
                }));
                result.extend(prefix_of.iter().copied().map(|w| {
                    if original.ngram_words.is_some() {
                        Word::Derived(w)
                    } else {
                        Word::Original(w)
                    }
                }));
            }
            NTypoTermSubset::Subset { words, phrases: _ } => {
                let ZeroTypoTerm {
                    phrase: _,
                    exact: zero_typo,
                    prefix_of,
                    synonyms: _,
                    use_prefix_db: _,
                } = &original.zero_typo;
                if let Some(zero_typo) = zero_typo {
                    if words.contains(zero_typo) {
                        if original.ngram_words.is_some() {
                            result.insert(Word::Derived(*zero_typo));
                        } else {
                            result.insert(Word::Original(*zero_typo));
                        }
                    }
                }
                result.extend(prefix_of.intersection(words).copied().map(|w| {
                    if original.ngram_words.is_some() {
                        Word::Derived(w)
                    } else {
                        Word::Original(w)
                    }
                }));
            }
            NTypoTermSubset::Nothing => {}
        }

        match &self.one_typo_subset {
            NTypoTermSubset::All => {
                let Lazy::Init(OneTypoTerm { split_words: _, one_typo }) = &original.one_typo
                else {
                    panic!()
                };
                result.extend(one_typo.iter().copied().map(Word::Derived))
            }
            NTypoTermSubset::Subset { words, phrases: _ } => {
                let Lazy::Init(OneTypoTerm { split_words: _, one_typo }) = &original.one_typo
                else {
                    panic!()
                };
                result.extend(one_typo.intersection(words).copied().map(Word::Derived));
            }
            NTypoTermSubset::Nothing => {}
        };

        match &self.two_typo_subset {
            NTypoTermSubset::All => {
                let Lazy::Init(TwoTypoTerm { two_typos }) = &original.two_typo else { panic!() };
                result.extend(two_typos.iter().copied().map(Word::Derived));
            }
            NTypoTermSubset::Subset { words, phrases: _ } => {
                let Lazy::Init(TwoTypoTerm { two_typos }) = &original.two_typo else { panic!() };
                result.extend(two_typos.intersection(words).copied().map(Word::Derived));
            }
            NTypoTermSubset::Nothing => {}
        };

        Ok(result)
    }
    pub fn all_phrases(&self, ctx: &mut SearchContext<'_>) -> Result<BTreeSet<Interned<Phrase>>> {
        let mut result = BTreeSet::default();

        if !self.one_typo_subset.is_empty() {
            self.original.compute_fully_if_needed(ctx)?;
        }
        let original = ctx.term_interner.get_mut(self.original);

        let ZeroTypoTerm { phrase, exact: _, prefix_of: _, synonyms, use_prefix_db: _ } =
            &original.zero_typo;
        result.extend(phrase.iter().copied());
        result.extend(synonyms.iter().copied());

        match &self.one_typo_subset {
            NTypoTermSubset::All => {
                let Lazy::Init(OneTypoTerm { split_words, one_typo: _ }) = &original.one_typo
                else {
                    panic!();
                };
                result.extend(split_words.iter().copied());
            }
            NTypoTermSubset::Subset { phrases, .. } => {
                let Lazy::Init(OneTypoTerm { split_words, one_typo: _ }) = &original.one_typo
                else {
                    panic!();
                };
                if let Some(split_words) = split_words {
                    if phrases.contains(split_words) {
                        result.insert(*split_words);
                    }
                }
            }
            NTypoTermSubset::Nothing => {}
        }

        Ok(result)
    }

    pub fn original_phrase(&self, ctx: &SearchContext<'_>) -> Option<Interned<Phrase>> {
        let t = ctx.term_interner.get(self.original);
        if let Some(p) = t.zero_typo.phrase {
            if self.zero_typo_subset.contains_phrase(p) {
                return Some(p);
            }
        }
        None
    }
    pub fn max_typo_cost(&self, ctx: &SearchContext<'_>) -> u8 {
        let t = ctx.term_interner.get(self.original);
        match t.max_levenshtein_distance {
            0 => {
                if t.allows_split_words() {
                    1
                } else {
                    0
                }
            }
            1 => {
                if self.one_typo_subset.is_empty() {
                    0
                } else {
                    1
                }
            }
            2 => {
                if self.two_typo_subset.is_empty() {
                    if self.one_typo_subset.is_empty() {
                        0
                    } else {
                        1
                    }
                } else {
                    2
                }
            }
            _ => panic!(),
        }
    }
    pub fn keep_only_exact_term(&mut self, ctx: &SearchContext<'_>) {
        if let Some(term) = self.exact_term(ctx) {
            match term {
                ExactTerm::Phrase(p) => {
                    self.zero_typo_subset = NTypoTermSubset::Subset {
                        words: BTreeSet::new(),
                        phrases: BTreeSet::from_iter([p]),
                    };
                    self.clear_one_typo_subset();
                    self.clear_two_typo_subset();
                }
                ExactTerm::Word(w) => {
                    self.zero_typo_subset = NTypoTermSubset::Subset {
                        words: BTreeSet::from_iter([w]),
                        phrases: BTreeSet::new(),
                    };
                    self.clear_one_typo_subset();
                    self.clear_two_typo_subset();
                }
            }
        }
    }
    pub fn clear_zero_typo_subset(&mut self) {
        self.zero_typo_subset = NTypoTermSubset::Nothing;
    }
    pub fn clear_one_typo_subset(&mut self) {
        self.one_typo_subset = NTypoTermSubset::Nothing;
    }
    pub fn clear_two_typo_subset(&mut self) {
        self.two_typo_subset = NTypoTermSubset::Nothing;
    }
    pub fn description(&self, ctx: &SearchContext<'_>) -> String {
        let t = ctx.term_interner.get(self.original);
        ctx.word_interner.get(t.original).to_owned()
    }
}

impl ZeroTypoTerm {
    fn is_empty(&self) -> bool {
        let ZeroTypoTerm { phrase, exact: zero_typo, prefix_of, synonyms, use_prefix_db } = self;
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
    fn is_empty(&self) -> bool {
        let Lazy::Init(one_typo) = &self.one_typo else {
            return false;
        };
        let Lazy::Init(two_typo) = &self.two_typo else {
            return false;
        };

        self.zero_typo.is_empty() && one_typo.is_empty() && two_typo.is_empty()
    }
    fn allows_split_words(&self) -> bool {
        self.zero_typo.phrase.is_none()
    }
}

impl Interned<QueryTerm> {
    /// Return the original word from the given query term
    fn original_single_word(self, ctx: &SearchContext<'_>) -> Option<Interned<String>> {
        let self_ = ctx.term_interner.get(self);
        if self_.ngram_words.is_some() {
            None
        } else {
            Some(self_.original)
        }
    }
}

/// A query term coupled with its position in the user's search query.
#[derive(Clone)]
pub struct LocatedQueryTerm {
    pub value: Interned<QueryTerm>,
    pub positions: RangeInclusive<u16>,
}

impl LocatedQueryTerm {
    /// Return `true` iff the term is empty
    pub fn is_empty(&self, interner: &DedupInterner<QueryTerm>) -> bool {
        interner.get(self.value).is_empty()
    }
}

impl QueryTerm {
    pub fn is_cached_prefix(&self) -> bool {
        self.zero_typo.use_prefix_db.is_some()
    }
    pub fn is_prefix(&self) -> bool {
        self.is_prefix
    }
    pub fn original_word(&self, ctx: &SearchContext<'_>) -> String {
        ctx.word_interner.get(self.original).clone()
    }

    pub fn original_phrase(&self) -> Option<Interned<Phrase>> {
        self.zero_typo.phrase
    }

    pub fn all_computed_derivations(&self) -> (Vec<Interned<String>>, Vec<Interned<Phrase>>) {
        let mut words = BTreeSet::new();
        let mut phrases = BTreeSet::new();

        let ZeroTypoTerm { phrase, exact: zero_typo, prefix_of, synonyms, use_prefix_db: _ } =
            &self.zero_typo;
        words.extend(zero_typo.iter().copied());
        words.extend(prefix_of.iter().copied());
        phrases.extend(phrase.iter().copied());
        phrases.extend(synonyms.iter().copied());

        if let Lazy::Init(OneTypoTerm { split_words, one_typo }) = &self.one_typo {
            words.extend(one_typo.iter().copied());
            phrases.extend(split_words.iter().copied());
        };

        if let Lazy::Init(TwoTypoTerm { two_typos }) = &self.two_typo {
            words.extend(two_typos.iter().copied());
        };

        (words.into_iter().collect(), phrases.into_iter().collect())
    }
}
