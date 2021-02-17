use std::borrow::Cow;

use crate::Index;
use crate::search::word_typos;

use roaring::RoaringBitmap;

use super::query_tree::{Operation, Query, QueryKind};

pub mod typo;

pub trait Criterion {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>>;
}

/// The result of a call to the parent criterion.
pub struct CriterionResult {
    /// The query tree that must be used by the children criterion to fetch candidates.
    pub query_tree: Option<Operation>,
    /// The candidates that this criterion is allowed to return subsets of.
    pub candidates: RoaringBitmap,
    /// Candidates that comes from the current bucket of the initial criterion.
    pub bucket_candidates: Option<RoaringBitmap>,
}

/// Either a set of candidates that defines the candidates
/// that are allowed to be returned,
/// or the candidates that must never be returned.
enum Candidates {
    Allowed(RoaringBitmap),
    Forbidden(RoaringBitmap)
}

impl Candidates {
    fn into_inner(self) -> RoaringBitmap {
        match self {
            Self::Allowed(inner) => inner,
            Self::Forbidden(inner) => inner,
        }
    }
}

impl Default for Candidates {
    fn default() -> Self {
        Self::Forbidden(RoaringBitmap::new())
    }
}
pub trait Context {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>>;
    fn word_pair_proximity_docids(&self, left: &str, right: &str, proximity: u8) -> heed::Result<Option<RoaringBitmap>>;
    fn word_prefix_pair_proximity_docids(&self, left: &str, right: &str, proximity: u8) -> heed::Result<Option<RoaringBitmap>>;
    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>>;
    fn in_prefix_cache(&self, word: &str) -> bool;
}
pub struct HeedContext<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    index: &'t Index,
    words_fst: fst::Set<Cow<'t, [u8]>>,
    words_prefixes_fst: fst::Set<Cow<'t, [u8]>>,
}

impl<'a> Context for HeedContext<'a> {
    fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_docids.get(self.rtxn, &word)
    }

    fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
        self.index.word_prefix_docids.get(self.rtxn, &word)
    }

    fn word_pair_proximity_docids(&self, left: &str, right: &str, proximity: u8) -> heed::Result<Option<RoaringBitmap>> {
        let key = (left, right, proximity);
        self.index.word_pair_proximity_docids.get(self.rtxn, &key)
    }

    fn word_prefix_pair_proximity_docids(&self, left: &str, right: &str, proximity: u8) -> heed::Result<Option<RoaringBitmap>> {
        let key = (left, right, proximity);
        self.index.word_prefix_pair_proximity_docids.get(self.rtxn, &key)
    }

    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>> {
        &self.words_fst
    }

    fn in_prefix_cache(&self, word: &str) -> bool {
        self.words_prefixes_fst.contains(word)
    }
}

impl<'t> HeedContext<'t> {
    pub fn new(rtxn: &'t heed::RoTxn<'t>, index: &'t Index) -> anyhow::Result<Self> {
        let words_fst = index.words_fst(rtxn)?;
        let words_prefixes_fst = index.words_prefixes_fst(rtxn)?;

        Ok(Self {
            rtxn,
            index,
            words_fst,
            words_prefixes_fst,
        })
    }
}

fn all_word_pair_proximity_docids<T: AsRef<str>, U: AsRef<str>>(
    ctx: &dyn Context,
    left_words: &[(T, u8)],
    right_words: &[(U, u8)],
    proximity: u8
) -> anyhow::Result<RoaringBitmap> {
    let mut docids = RoaringBitmap::new();
    for (left, _l_typo) in left_words {
        for (right, _r_typo) in right_words {
            let current_docids = ctx.word_pair_proximity_docids(left.as_ref(), right.as_ref(), proximity)?.unwrap_or_default();
            docids.union_with(&current_docids);
        }
    }
    Ok(docids)
}

fn query_docids(ctx: &dyn Context, query: &Query) -> anyhow::Result<RoaringBitmap> {
    match &query.kind {
        QueryKind::Exact { word, .. } => {
            if query.prefix && ctx.in_prefix_cache(&word) {
                Ok(ctx.word_prefix_docids(&word)?.unwrap_or_default())
            } else if query.prefix {
                let words = word_typos(&word, true, 0, ctx.words_fst())?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    let current_docids = ctx.word_docids(&word)?.unwrap_or_default();
                    docids.union_with(&current_docids);
                }
                Ok(docids)
            } else {
                Ok(ctx.word_docids(&word)?.unwrap_or_default())
            }
        },
        QueryKind::Tolerant { typo, word } => {
            let words = word_typos(&word, query.prefix, *typo, ctx.words_fst())?;
            let mut docids = RoaringBitmap::new();
            for (word, _typo) in words {
                let current_docids = ctx.word_docids(&word)?.unwrap_or_default();
                docids.union_with(&current_docids);
            }
            Ok(docids)
        },
    }
}

fn query_pair_proximity_docids(ctx: &dyn Context, left: &Query, right: &Query, proximity: u8) -> anyhow::Result<RoaringBitmap> {
    let prefix = right.prefix;

    match (&left.kind, &right.kind) {
        (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }) => {
            if prefix && ctx.in_prefix_cache(&right) {
                Ok(ctx.word_prefix_pair_proximity_docids(left.as_str(), right.as_str(), proximity)?.unwrap_or_default())
            } else if prefix {
                let r_words = word_typos(&right, true, 0, ctx.words_fst())?;
                all_word_pair_proximity_docids(ctx, &[(left, 0)], &r_words, proximity)
            } else {
                Ok(ctx.word_pair_proximity_docids(left.as_str(), right.as_str(), proximity)?.unwrap_or_default())
            }
        },
        (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }) => {
            let l_words = word_typos(&left, false, *typo, ctx.words_fst())?;
            if prefix && ctx.in_prefix_cache(&right) {
                let mut docids = RoaringBitmap::new();
                for (left, _) in l_words {
                    let current_docids = ctx.word_prefix_pair_proximity_docids(left.as_ref(), right.as_ref(), proximity)?.unwrap_or_default();
                    docids.union_with(&current_docids);
                }
                Ok(docids)
            } else if prefix {
                let r_words = word_typos(&right, true, 0, ctx.words_fst())?;
                all_word_pair_proximity_docids(ctx, &l_words, &r_words, proximity)
            } else {
                all_word_pair_proximity_docids(ctx, &l_words, &[(right, 0)], proximity)
            }
        },
        (QueryKind::Exact { word: left, .. }, QueryKind::Tolerant { typo, word: right }) => {
            let r_words = word_typos(&right, prefix, *typo, ctx.words_fst())?;
            all_word_pair_proximity_docids(ctx, &[(left, 0)], &r_words, proximity)
        },
        (QueryKind::Tolerant { typo: l_typo, word: left }, QueryKind::Tolerant { typo: r_typo, word: right }) => {
            let l_words = word_typos(&left, false, *l_typo, ctx.words_fst())?;
            let r_words = word_typos(&right, prefix, *r_typo, ctx.words_fst())?;
            all_word_pair_proximity_docids(ctx, &l_words, &r_words, proximity)
        },
    }
}
