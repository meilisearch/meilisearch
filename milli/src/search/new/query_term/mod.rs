mod compute_derivations;
mod ntypo_subset;
mod parse_query;
mod phrase;

use super::interner::{DedupInterner, Interned};
use super::{limits, SearchContext};
use crate::Result;
use std::collections::BTreeSet;
use std::ops::RangeInclusive;

pub use ntypo_subset::NTypoTermSubset;
pub use parse_query::{located_query_terms_from_string, make_ngram, number_of_typos_allowed};
pub use phrase::Phrase;

use compute_derivations::partially_initialized_term_from_word;

/**
A set of word derivations attached to a location in the search query.

*/
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
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryTerm {
    original: Interned<String>,
    ngram_words: Option<Vec<Interned<String>>>,
    max_nbr_typos: u8,
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
    zero_typo: Option<Interned<String>>,
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
        let mut result = BTreeSet::default();
        // TODO: a compute_partially funtion
        if !self.one_typo_subset.is_empty() || !self.two_typo_subset.is_empty() {
            self.original.compute_fully_if_needed(ctx)?;
        }

        let original = ctx.term_interner.get_mut(self.original);
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
        let mut result = BTreeSet::default();

        if !self.one_typo_subset.is_empty() {
            // TODO: compute less than fully if possible
            self.original.compute_fully_if_needed(ctx)?;
        }
        let original = ctx.term_interner.get_mut(self.original);

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

    pub fn original_phrase(&self, ctx: &SearchContext) -> Option<Interned<Phrase>> {
        let t = ctx.term_interner.get(self.original);
        if let Some(p) = t.zero_typo.phrase {
            if self.zero_typo_subset.contains_phrase(p) {
                return Some(p);
            }
        }
        None
    }
    pub fn max_nbr_typos(&self, ctx: &SearchContext) -> u8 {
        let t = ctx.term_interner.get(self.original);
        match t.max_nbr_typos {
            0 => 0,
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
    pub fn clear_zero_typo_subset(&mut self) {
        self.zero_typo_subset = NTypoTermSubset::Nothing;
    }
    pub fn clear_one_typo_subset(&mut self) {
        self.one_typo_subset = NTypoTermSubset::Nothing;
    }
    pub fn clear_two_typo_subset(&mut self) {
        self.two_typo_subset = NTypoTermSubset::Nothing;
    }
    pub fn description(&self, ctx: &SearchContext) -> String {
        let t = ctx.term_interner.get(self.original);
        ctx.word_interner.get(t.original).to_owned()
    }
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
    fn is_empty(&self) -> bool {
        let Lazy::Init(one_typo) = &self.one_typo else {
            return false;
        };
        let Lazy::Init(two_typo) = &self.two_typo else {
            return false;
        };

        self.zero_typo.is_empty() && one_typo.is_empty() && two_typo.is_empty()
    }
}

impl Interned<QueryTerm> {
    /// Return the original word from the given query term
    fn original_single_word(self, ctx: &SearchContext) -> Option<Interned<String>> {
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
