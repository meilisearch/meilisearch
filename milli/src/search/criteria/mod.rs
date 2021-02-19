use std::borrow::Cow;

use crate::Index;
use crate::search::word_derivations;

use roaring::RoaringBitmap;

use super::query_tree::{Operation, Query, QueryKind};

pub mod typo;
pub mod words;

pub trait Criterion {
    fn next(&mut self) -> anyhow::Result<Option<CriterionResult>>;
}

/// The result of a call to the parent criterion.
#[derive(Debug, Clone, PartialEq)]
pub struct CriterionResult {
    /// The query tree that must be used by the children criterion to fetch candidates.
    pub query_tree: Option<Operation>,
    /// The candidates that this criterion is allowed to return subsets of.
    pub candidates: RoaringBitmap,
    /// Candidates that comes from the current bucket of the initial criterion.
    pub bucket_candidates: RoaringBitmap,
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
                let words = word_derivations(&word, true, 0, ctx.words_fst())?;
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
            let words = word_derivations(&word, query.prefix, *typo, ctx.words_fst())?;
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
                let r_words = word_derivations(&right, true, 0, ctx.words_fst())?;
                all_word_pair_proximity_docids(ctx, &[(left, 0)], &r_words, proximity)
            } else {
                Ok(ctx.word_pair_proximity_docids(left.as_str(), right.as_str(), proximity)?.unwrap_or_default())
            }
        },
        (QueryKind::Tolerant { typo, word: left }, QueryKind::Exact { word: right, .. }) => {
            let l_words = word_derivations(&left, false, *typo, ctx.words_fst())?;
            if prefix && ctx.in_prefix_cache(&right) {
                let mut docids = RoaringBitmap::new();
                for (left, _) in l_words {
                    let current_docids = ctx.word_prefix_pair_proximity_docids(left.as_ref(), right.as_ref(), proximity)?.unwrap_or_default();
                    docids.union_with(&current_docids);
                }
                Ok(docids)
            } else if prefix {
                let r_words = word_derivations(&right, true, 0, ctx.words_fst())?;
                all_word_pair_proximity_docids(ctx, &l_words, &r_words, proximity)
            } else {
                all_word_pair_proximity_docids(ctx, &l_words, &[(right, 0)], proximity)
            }
        },
        (QueryKind::Exact { word: left, .. }, QueryKind::Tolerant { typo, word: right }) => {
            let r_words = word_derivations(&right, prefix, *typo, ctx.words_fst())?;
            all_word_pair_proximity_docids(ctx, &[(left, 0)], &r_words, proximity)
        },
        (QueryKind::Tolerant { typo: l_typo, word: left }, QueryKind::Tolerant { typo: r_typo, word: right }) => {
            let l_words = word_derivations(&left, false, *l_typo, ctx.words_fst())?;
            let r_words = word_derivations(&right, prefix, *r_typo, ctx.words_fst())?;
            all_word_pair_proximity_docids(ctx, &l_words, &r_words, proximity)
        },
    }
}

#[cfg(test)]
pub mod test {
    use maplit::hashmap;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::*;
    use std::collections::HashMap;

    fn s(s: &str) -> String { s.to_string() }
    pub struct TestContext<'t> {
        words_fst: fst::Set<Cow<'t, [u8]>>,
        word_docids: HashMap<String, RoaringBitmap>,
        word_prefix_docids: HashMap<String, RoaringBitmap>,
        word_pair_proximity_docids: HashMap<(String, String, i32), RoaringBitmap>,
        word_prefix_pair_proximity_docids: HashMap<(String, String, i32), RoaringBitmap>,
    }

    impl<'a> Context for TestContext<'a> {
        fn word_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.word_docids.get(&word.to_string()).cloned())
        }

        fn word_prefix_docids(&self, word: &str) -> heed::Result<Option<RoaringBitmap>> {
            Ok(self.word_prefix_docids.get(&word.to_string()).cloned())
        }

        fn word_pair_proximity_docids(&self, left: &str, right: &str, proximity: u8) -> heed::Result<Option<RoaringBitmap>> {
            let key = (left.to_string(), right.to_string(), proximity.into());
            Ok(self.word_pair_proximity_docids.get(&key).cloned())
        }

        fn word_prefix_pair_proximity_docids(&self, left: &str, right: &str, proximity: u8) -> heed::Result<Option<RoaringBitmap>> {
            let key = (left.to_string(), right.to_string(), proximity.into());
            Ok(self.word_prefix_pair_proximity_docids.get(&key).cloned())
        }

        fn words_fst<'t>(&self) -> &'t fst::Set<Cow<[u8]>> {
            &self.words_fst
        }

        fn in_prefix_cache(&self, word: &str) -> bool {
            self.word_prefix_docids.contains_key(&word.to_string())
        }
    }

    impl<'a> Default for TestContext<'a> {
        fn default() -> TestContext<'a> {
            let mut rng = StdRng::seed_from_u64(102);
            let rng = &mut rng;

            fn random_postings<R: Rng>(rng: &mut R, len: usize) -> RoaringBitmap {
                let mut values = Vec::<u32>::with_capacity(len);
                while values.len() != len {
                    values.push(rng.gen());
                }
                values.sort_unstable();

                RoaringBitmap::from_sorted_iter(values.into_iter())
            }

            let word_docids = hashmap!{
                s("hello")      => random_postings(rng,   1500),
                s("hi")         => random_postings(rng,   4000),
                s("word")       => random_postings(rng,   2500),
                s("split")      => random_postings(rng,    400),
                s("ngrams")     => random_postings(rng,   1400),
                s("world")      => random_postings(rng, 15_000),
                s("earth")      => random_postings(rng,   8000),
                s("2021")       => random_postings(rng,    100),
                s("2020")       => random_postings(rng,    500),
                s("is")         => random_postings(rng, 50_000),
                s("this")       => random_postings(rng, 50_000),
                s("good")       => random_postings(rng,   1250),
                s("morning")    => random_postings(rng,    125),
            };

            let word_prefix_docids = hashmap!{
                s("h")   => &word_docids[&s("hello")] | &word_docids[&s("hi")],
                s("wor") => &word_docids[&s("word")]  | &word_docids[&s("world")],
                s("20")  => &word_docids[&s("2020")]  | &word_docids[&s("2021")],
            };

            let hello_world = &word_docids[&s("hello")] & &word_docids[&s("world")];
            let hello_world_split = (hello_world.len() / 2) as usize;
            let hello_world_1 = hello_world.iter().take(hello_world_split).collect();
            let hello_world_2 = hello_world.iter().skip(hello_world_split).collect();

            let hello_word = &word_docids[&s("hello")] & &word_docids[&s("word")];
            let hello_word_split = (hello_word.len() / 2) as usize;
            let hello_word_4 = hello_word.iter().take(hello_word_split).collect();
            let hello_word_6 = hello_word.iter().skip(hello_word_split).take(hello_word_split/2).collect();
            let hello_word_7 = hello_word.iter().skip(hello_word_split + hello_word_split/2).collect();
            let word_pair_proximity_docids = hashmap!{
                (s("good"), s("morning"), 1)   => &word_docids[&s("good")] & &word_docids[&s("morning")],
                (s("hello"), s("world"), 1)   => hello_world_1,
                (s("hello"), s("world"), 4)   => hello_world_2,
                (s("this"), s("is"), 1)   => &word_docids[&s("this")] & &word_docids[&s("is")],
                (s("is"), s("2021"), 1)   => &word_docids[&s("this")] & &word_docids[&s("is")] & &word_docids[&s("2021")],
                (s("is"), s("2020"), 1)   => &word_docids[&s("this")] & &word_docids[&s("is")] & (&word_docids[&s("2020")] - &word_docids[&s("2021")]),
                (s("this"), s("2021"), 2)   => &word_docids[&s("this")] & &word_docids[&s("is")] & &word_docids[&s("2021")],
                (s("this"), s("2020"), 2)   => &word_docids[&s("this")] & &word_docids[&s("is")] & (&word_docids[&s("2020")] - &word_docids[&s("2021")]),
                (s("word"), s("split"), 1)   => &word_docids[&s("word")] & &word_docids[&s("split")],
                (s("world"), s("split"), 1)   => (&word_docids[&s("world")] & &word_docids[&s("split")]) - &word_docids[&s("word")],
                (s("hello"), s("word"), 4) => hello_word_4,
                (s("hello"), s("word"), 6) => hello_word_6,
                (s("hello"), s("word"), 7) => hello_word_7,
                (s("split"), s("ngrams"), 3)   => (&word_docids[&s("split")] & &word_docids[&s("ngrams")]) - &word_docids[&s("word")],
                (s("split"), s("ngrams"), 5)   => &word_docids[&s("split")] & &word_docids[&s("ngrams")] & &word_docids[&s("word")],
                (s("this"), s("ngrams"), 1)   => (&word_docids[&s("split")] & &word_docids[&s("this")] & &word_docids[&s("ngrams")] ) - &word_docids[&s("word")],
                (s("this"), s("ngrams"), 2)   => &word_docids[&s("split")] & &word_docids[&s("this")] & &word_docids[&s("ngrams")] & &word_docids[&s("word")],
            };

            let word_prefix_pair_proximity_docids = hashmap!{
                (s("hello"), s("wor"), 1) => word_pair_proximity_docids.get(&(s("hello"), s("world"), 1)).unwrap().clone(),
                (s("hello"), s("wor"), 4) => word_pair_proximity_docids.get(&(s("hello"), s("world"), 4)).unwrap() | word_pair_proximity_docids.get(&(s("hello"), s("word"), 4)).unwrap(),
                (s("hello"), s("wor"), 6) => word_pair_proximity_docids.get(&(s("hello"), s("word"), 6)).unwrap().clone(),
                (s("hello"), s("wor"), 7) => word_pair_proximity_docids.get(&(s("hello"), s("word"), 7)).unwrap().clone(),
                (s("is"), s("20"), 1) => word_pair_proximity_docids.get(&(s("is"), s("2020"), 1)).unwrap() | word_pair_proximity_docids.get(&(s("is"), s("2021"), 1)).unwrap(),
                (s("this"), s("20"), 2) => word_pair_proximity_docids.get(&(s("this"), s("2020"), 2)).unwrap() | word_pair_proximity_docids.get(&(s("this"), s("2021"), 2)).unwrap(),
            };

            let mut keys = word_docids.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let words_fst = fst::Set::from_iter(keys).unwrap().map_data(|v| Cow::Owned(v)).unwrap();

            TestContext {
                words_fst,
                word_docids,
                word_prefix_docids,
                word_pair_proximity_docids,
                word_prefix_pair_proximity_docids,
            }
        }
    }
}
