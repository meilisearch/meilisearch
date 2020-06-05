mod query_tokens;

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::time::Instant;

use cow_utils::CowUtils;
use fst::{IntoStreamer, Streamer};
use fxhash::FxHasher32;
use heed::types::*;
use heed::{PolyDatabase, Database};
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use once_cell::sync::Lazy;
use roaring::RoaringBitmap;

use self::query_tokens::{QueryTokens, QueryToken};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32 = smallvec::SmallVec<[u8; 32]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;
pub type AttributeId = u32;

#[derive(Clone)]
pub struct Index {
    pub main: PolyDatabase,
    pub postings_attrs: Database<Str, ByteSlice>,
    pub postings_ids: Database<ByteSlice, ByteSlice>,
    pub prefix_postings_ids: Database<ByteSlice, ByteSlice>,
    pub documents: Database<OwnedType<BEU32>, ByteSlice>,
}

impl Index {
    pub fn new(env: &heed::Env) -> heed::Result<Index> {
        let main = env.create_poly_database(None)?;
        let postings_attrs = env.create_database(Some("postings-attrs"))?;
        let postings_ids = env.create_database(Some("postings-ids"))?;
        let prefix_postings_ids = env.create_database(Some("prefix-postings-ids"))?;
        let documents = env.create_database(Some("documents"))?;

        Ok(Index { main, postings_attrs, postings_ids, prefix_postings_ids, documents })
    }

    pub fn headers<'t>(&self, rtxn: &'t heed::RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, "headers")
    }

    pub fn search(&self, rtxn: &heed::RoTxn, query: &str) -> anyhow::Result<Vec<DocumentId>> {
        let fst = match self.main.get::<_, Str, ByteSlice>(rtxn, "words-fst")? {
            Some(bytes) => fst::Set::new(bytes)?,
            None => return Ok(Vec::new()),
        };

        let (lev0, lev1, lev2) = (&LEVDIST0, &LEVDIST1, &LEVDIST2);

        let words: Vec<_> = QueryTokens::new(query).collect();
        let ends_with_whitespace = query.chars().last().map_or(false, char::is_whitespace);
        let number_of_words = words.len();
        let dfas: Vec<_> = words.into_iter().enumerate().map(|(i, word)| {
            let (word, quoted) = match word {
                QueryToken::Free(word) => (word.cow_to_lowercase(), false),
                QueryToken::Quoted(word) => (Cow::Borrowed(word), true),
            };
            let is_last = i + 1 == number_of_words;
            let is_prefix = is_last && !ends_with_whitespace && !quoted;
            let lev = match word.len() {
                0..=4 => if quoted { lev0 } else { lev0 },
                5..=8 => if quoted { lev0 } else { lev1 },
                _     => if quoted { lev0 } else { lev2 },
            };

            let dfa = if is_prefix {
                lev.build_prefix_dfa(&word)
            } else {
                lev.build_dfa(&word)
            };

            (word, is_prefix, dfa)
        })
        .collect();

        let mut intersect_attrs: Option<RoaringBitmap> = None;
        for (_word, _is_prefix, dfa) in &dfas {
            let mut union_result = RoaringBitmap::default();
            let mut stream = fst.search(dfa).into_stream();
            while let Some(word) = stream.next() {
                let word = std::str::from_utf8(word)?;
                if let Some(attrs) = self.postings_attrs.get(rtxn, word)? {
                    let right = RoaringBitmap::deserialize_from(attrs)?;
                    union_result.union_with(&right);
                }
            }

            match &mut intersect_attrs {
                Some(left) => left.intersect_with(&union_result),
                None => intersect_attrs = Some(union_result),
            }
        }

        eprintln!("we should only look for documents with attrs {:?}", intersect_attrs);

        let mut intersect_docids: Option<RoaringBitmap> = None;
        // TODO would be faster to store and use the words
        //      seen in the previous attrs loop
        for (word, is_prefix, dfa) in &dfas {
            let mut union_result = RoaringBitmap::default();
            for attr in intersect_attrs.as_ref().unwrap_or(&RoaringBitmap::default()) {
                let before = Instant::now();

                let mut count = 0;
                if word.len() <= 4 && *is_prefix {
                    let mut key = word.as_bytes()[..word.len().min(5)].to_vec();
                    key.extend_from_slice(&attr.to_be_bytes());
                    if let Some(ids) = self.prefix_postings_ids.get(rtxn, &key)? {
                        let right = RoaringBitmap::deserialize_from(ids)?;
                        union_result.union_with(&right);
                        count = 1;
                    }
                } else {
                    let mut stream = fst.search(dfa).into_stream();
                    while let Some(word) = stream.next() {
                        count += 1;
                        let word = std::str::from_utf8(word)?;
                        let mut key = word.as_bytes().to_vec();
                        key.extend_from_slice(&attr.to_be_bytes());
                        if let Some(ids) = self.postings_ids.get(rtxn, &key)? {
                            let right = RoaringBitmap::deserialize_from(ids)?;
                            union_result.union_with(&right);
                        }
                    }
                }

                eprintln!("with {:?} similar words (for attr {}) union for {:?} gives {:?} took {:.02?}",
                    count, attr, word, union_result.len(), before.elapsed());
            }

            match &mut intersect_docids {
                Some(left) => {
                    let before = Instant::now();
                    let left_len = left.len();
                    left.intersect_with(&union_result);
                    eprintln!("intersect between {:?} and {:?} gives {:?} took {:.02?}",
                        left_len, union_result.len(), left.len(), before.elapsed());
                },
                None => intersect_docids = Some(union_result),
            }
        }

        eprintln!("{} candidates", intersect_docids.as_ref().map_or(0, |r| r.len()));

        Ok(intersect_docids.unwrap_or_default().iter().take(20).collect())
    }
}
