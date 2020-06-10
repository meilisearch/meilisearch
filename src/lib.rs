mod best_proximity;
mod query_tokens;

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use cow_utils::CowUtils;
use fst::{IntoStreamer, Streamer};
use fxhash::FxHasher32;
use heed::types::*;
use heed::{PolyDatabase, Database};
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use once_cell::sync::Lazy;
use roaring::RoaringBitmap;

use self::query_tokens::{QueryTokens, QueryToken};
use self::best_proximity::BestProximity;

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
    pub prefix_postings_attrs: Database<ByteSlice, ByteSlice>,
    pub postings_ids: Database<ByteSlice, ByteSlice>,
    pub prefix_postings_ids: Database<ByteSlice, ByteSlice>,
    pub documents: Database<OwnedType<BEU32>, ByteSlice>,
}

impl Index {
    pub fn new(env: &heed::Env) -> heed::Result<Index> {
        let main = env.create_poly_database(None)?;
        let postings_attrs = env.create_database(Some("postings-attrs"))?;
        let prefix_postings_attrs = env.create_database(Some("prefix-postings-attrs"))?;
        let postings_ids = env.create_database(Some("postings-ids"))?;
        let prefix_postings_ids = env.create_database(Some("prefix-postings-ids"))?;
        let documents = env.create_database(Some("documents"))?;

        Ok(Index { main, postings_attrs, prefix_postings_attrs, postings_ids, prefix_postings_ids, documents })
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
        let dfas = words.into_iter().enumerate().map(|(i, word)| {
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
        });

        let mut words_positions = Vec::new();
        let mut positions = Vec::new();

        for (word, is_prefix, dfa) in dfas {
            let mut count = 0;
            let mut union_positions = RoaringBitmap::default();
            if false && word.len() <= 4 && is_prefix {
                if let Some(ids) = self.prefix_postings_attrs.get(rtxn, word.as_bytes())? {
                    let right = RoaringBitmap::deserialize_from(ids)?;
                    union_positions.union_with(&right);
                    count = 1;
                }
            } else {
                let mut stream = fst.search(&dfa).into_stream();
                while let Some(word) = stream.next() {
                    let word = std::str::from_utf8(word)?;
                    if let Some(attrs) = self.postings_attrs.get(rtxn, word)? {
                        let right = RoaringBitmap::deserialize_from(attrs)?;
                        union_positions.union_with(&right);
                        count += 1;
                    }
                }
            }

            eprintln!("{} words for {:?} we have found positions {:?}", count, word, union_positions);
            words_positions.push((word, is_prefix, dfa));
            positions.push(union_positions.iter().collect());
        }

        let mut documents = Vec::new();

        for (_proximity, positions) in BestProximity::new(positions) {
            let mut same_proximity_union = RoaringBitmap::default();

            for positions in positions {
                let mut intersect_docids: Option<RoaringBitmap> = None;
                for ((word, is_prefix, dfa), pos) in words_positions.iter().zip(positions) {
                    let mut count = 0;
                    let mut union_docids = RoaringBitmap::default();

                    // TODO re-enable the prefixes system
                    if false && word.len() <= 4 && *is_prefix {
                        let mut key = word.as_bytes()[..word.len().min(5)].to_vec();
                        key.extend_from_slice(&pos.to_be_bytes());
                        if let Some(ids) = self.prefix_postings_ids.get(rtxn, &key)? {
                            let right = RoaringBitmap::deserialize_from(ids)?;
                            union_docids.union_with(&right);
                            count = 1;
                        }
                    } else {
                        let mut stream = fst.search(dfa).into_stream();
                        while let Some(word) = stream.next() {
                            let word = std::str::from_utf8(word)?;
                            let mut key = word.as_bytes().to_vec();
                            key.extend_from_slice(&pos.to_be_bytes());
                            if let Some(attrs) = self.postings_ids.get(rtxn, &key)? {
                                let right = RoaringBitmap::deserialize_from(attrs)?;
                                union_docids.union_with(&right);
                                count += 1;
                            }
                        }
                    }

                    let _ = count;

                    match &mut intersect_docids {
                        Some(left) => left.intersect_with(&union_docids),
                        None => intersect_docids = Some(union_docids),
                    }
                }

                if let Some(intersect_docids) = intersect_docids {
                    same_proximity_union.union_with(&intersect_docids);
                }
            }

            documents.push(same_proximity_union);

            // We found enough documents we can stop here
            if documents.iter().map(RoaringBitmap::len).sum::<u64>() >= 20 {
                break
            }
        }

        eprintln!("{} candidates", documents.iter().map(RoaringBitmap::len).sum::<u64>());
        Ok(documents.iter().flatten().take(20).collect())
    }
}
