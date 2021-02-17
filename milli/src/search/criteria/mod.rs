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
    fn query_docids(&self, query: &Query) -> anyhow::Result<Option<RoaringBitmap>>;
    fn query_pair_proximity_docids(&self, left: &Query, right: &Query, distance: u8) ->anyhow::Result<Option<RoaringBitmap>>;
    fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>>;
}
pub struct HeedContext<'t> {
    rtxn: &'t heed::RoTxn<'t>,
    index: &'t Index,
    words_fst: fst::Set<Cow<'t, [u8]>>,
    words_prefixes_fst: fst::Set<Cow<'t, [u8]>>,
}

impl<'a> Context for HeedContext<'a> {
    fn query_docids(&self, query: &Query) -> anyhow::Result<Option<RoaringBitmap>> {
        match (&query.kind, query.prefix) {
            (QueryKind::Exact { word, .. }, true) if self.in_prefix_cache(&word) => {
                Ok(self.index.word_prefix_docids.get(self.rtxn, &word)?)
            },
            (QueryKind::Exact { word, .. }, true) => {
                let words = word_typos(&word, true, 0, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    docids.union_with(&self.index.word_docids.get(self.rtxn, &word)?.unwrap_or_default());
                }
                Ok(Some(docids))
            },
            (QueryKind::Exact { word, .. }, false) => {
                Ok(self.index.word_docids.get(self.rtxn, &word)?)
            },
            (QueryKind::Tolerant { typo, word }, prefix) => {
                let words = word_typos(&word, prefix, *typo, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    docids.union_with(&self.index.word_docids.get(self.rtxn, &word)?.unwrap_or_default());
                }
                Ok(Some(docids))
            },
        }
    }

    fn query_pair_proximity_docids(&self, left: &Query, right: &Query, distance: u8) -> anyhow::Result<Option<RoaringBitmap>> {
        // TODO add prefix cache for Tolerant-Exact-true and Exact-Exact-true
        match (&left.kind, &right.kind, right.prefix) {
            (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }, true) if self.in_prefix_cache(&right) => {
                let key = (left.as_str(), right.as_str(), distance);
                Ok(self.index.word_prefix_pair_proximity_docids.get(self.rtxn, &key)?)
            },
            (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }, true) if self.in_prefix_cache(&right) => {
                let words = word_typos(&left, false, *typo, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    let key = (word.as_str(), right.as_str(), distance);
                    docids.union_with(&self.index.word_prefix_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default());
                }
                Ok(Some(docids))
            },
            (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }, true) => {
                let words = word_typos(&right, true, 0, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    let key = (left.as_str(), word.as_str(), distance);
                    docids.union_with(&self.index.word_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default());
                }
                Ok(Some(docids))
            },
            (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }, true) => {
                let l_words = word_typos(&left, false, *typo, &self.words_fst)?;
                let r_words = word_typos(&right, true, 0, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (left, _typo) in l_words {
                    for (right, _typo) in r_words.iter() {
                        let key = (left.as_str(), right.as_str(), distance);
                        docids.union_with(&self.index.word_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default());
                    }
                }
                Ok(Some(docids))
            },
            (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }, false) => {
                let words = word_typos(&left, false, *typo, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    let key = (word.as_str(), right.as_str(), distance);
                    docids.union_with(&self.index.word_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default());
                }
                Ok(Some(docids))
            },
            (QueryKind::Exact { word: left, .. }, QueryKind::Tolerant { typo, word: right }, prefix) => {
                let words = word_typos(&right, prefix, *typo, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (word, _typo) in words {
                    let key = (left.as_str(), word.as_str(), distance);
                    docids.union_with(&self.index.word_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default());
                }
                Ok(Some(docids))
            },
            (QueryKind::Tolerant { typo: l_typo, word: left }, QueryKind::Tolerant { typo: r_typo, word: right }, prefix) => {
                let l_words = word_typos(&left, false, *l_typo, &self.words_fst)?;
                let r_words = word_typos(&right, prefix, *r_typo, &self.words_fst)?;
                let mut docids = RoaringBitmap::new();
                for (left, _typo) in l_words {
                    for (right, _typo) in r_words.iter() {
                        let key = (left.as_str(), right.as_str(), distance);
                        docids.union_with(&self.index.word_pair_proximity_docids.get(self.rtxn, &key)?.unwrap_or_default());
                    }
                }
                Ok(Some(docids))
            },
            (QueryKind::Exact { word: left, .. }, QueryKind::Exact { word: right, .. }, false) => {
                let key = (left.as_str(), right.as_str(), distance);
                Ok(self.index.word_pair_proximity_docids.get(self.rtxn, &key)?)
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
}
