use std::borrow::Cow;

use crate::Index;
use crate::search::word_typos;

use roaring::RoaringBitmap;

use super::query_tree::{Operation, Query, QueryKind};

pub mod typo;

pub trait Criterion {
    fn next(&mut self) -> anyhow::Result<Option<(Option<Operation>, RoaringBitmap)>>;
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
    fn query_docids(&self, query: &Query) -> anyhow::Result<RoaringBitmap>;
    fn query_pair_proximity_docids(&self, left: &Query, right: &Query, distance: u8) ->anyhow::Result<RoaringBitmap>;
    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>>;
}
pub struct HeedContext<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    index: &'t Index,
    words_fst: fst::Set<Cow<'t, [u8]>>,
    words_prefixes_fst: fst::Set<Cow<'t, [u8]>>,
}

impl<'a> Context for HeedContext<'a> {
    fn query_docids(&self, query: &Query) -> anyhow::Result<RoaringBitmap> {
        match (&query.kind, query.prefix) {
            (QueryKind::Exact { word, .. }, true) if self.in_prefix_cache(&word) => {
                Ok(self.index.word_prefix_docids.get(self.rtxn, &word)?.unwrap_or_default())
            },
            (QueryKind::Exact { word, .. }, true) => {
                let words = word_typos(&word, true, 0, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    docids.union_with(&self.index.word_docids.get(self.rtxn, &word)?.unwrap_or_default());
                }
                Ok(docids)
            },
            (QueryKind::Exact { word, .. }, false) => {
                Ok(self.index.word_docids.get(self.rtxn, &word)?.unwrap_or_default())
            },
            (QueryKind::Tolerant { typo, word }, prefix) => {
                let words = word_typos(&word, prefix, *typo, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    docids.union_with(&self.index.word_docids.get(self.rtxn, &word)?.unwrap_or_default());
                }
                Ok(docids)
            },
        }
    }

    fn query_pair_proximity_docids(&self, left: &Query, right: &Query, distance: u8) -> anyhow::Result<RoaringBitmap> {
        match (&left.kind, &right.kind, right.prefix) {
            (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }, true) if self.in_prefix_cache(&right) => {
                let key = (left.as_str(), right.as_str(), distance);
                Ok(self.index.word_prefix_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default())
            },
            (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }, true) if self.in_prefix_cache(&right) => {
                let l_words = word_typos(&left, false, *typo, &self.words_fst)?;
                self.all_word_pair_proximity_docids(&l_words, &[(right, 0)], distance)
            },
            (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }, true) => {
                let r_words = word_typos(&right, true, 0, &self.words_fst)?;
                self.all_word_pair_proximity_docids(&[(left, 0)], &r_words, distance)
            },
            (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }, true) => {
                let l_words = word_typos(&left, false, *typo, &self.words_fst)?;
                let r_words = word_typos(&right, true, 0, &self.words_fst)?;
                self.all_word_pair_proximity_docids(&l_words, &r_words, distance)
            },
            (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }, false) => {
                let l_words = word_typos(&left, false, *typo, &self.words_fst)?;
                self.all_word_pair_proximity_docids(&l_words, &[(right, 0)], distance)
            },
            (QueryKind::Exact { word: left, .. }, QueryKind::Tolerant { typo, word: right }, prefix) => {
                let r_words = word_typos(&right, prefix, *typo, &self.words_fst)?;
                self.all_word_pair_proximity_docids(&[(left, 0)], &r_words, distance)
            },
            (QueryKind::Tolerant { typo: l_typo, word: left }, QueryKind::Tolerant { typo: r_typo, word: right }, prefix) => {
                let l_words = word_typos(&left, false, *l_typo, &self.words_fst)?;
                let r_words = word_typos(&right, prefix, *r_typo, &self.words_fst)?;
                self.all_word_pair_proximity_docids(&l_words, &r_words, distance)
            },
            (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }, false) => {
                let key = (left.as_str(), right.as_str(), distance);
                Ok(self.index.word_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default())
            },
        }
    }

    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>> {
        &self.words_fst
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

    fn in_prefix_cache(&self, word: &str) -> bool {
        self.words_prefixes_fst.contains(word)
    }

    fn all_word_pair_proximity_docids<T: AsRef<str>, U: AsRef<str>>(&self, left_words: &[(T, u8)], right_words: &[(U, u8)], distance: u8) -> anyhow::Result<RoaringBitmap> {
        let mut docids = RoaringBitmap::new();
        for (left, _l_typo) in left_words {
            for (right, _r_typo) in right_words {
                let key = (left.as_ref(), right.as_ref(), distance);
                docids.union_with(&self.index.word_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default());
            }
        }
        Ok(docids)
    }
}
