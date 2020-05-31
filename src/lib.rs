use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::time::Instant;

use cow_utils::CowUtils;
use fst::{IntoStreamer, Streamer};
use fxhash::FxHasher32;
use heed::types::*;
use heed::{PolyDatabase, Database};
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use once_cell::sync::OnceCell;
use roaring::RoaringBitmap;
use slice_group_by::StrGroupBy;

static LEVDIST0: OnceCell<LevBuilder> = OnceCell::new();
static LEVDIST1: OnceCell<LevBuilder> = OnceCell::new();
static LEVDIST2: OnceCell<LevBuilder> = OnceCell::new();

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32 = smallvec::SmallVec<[u8; 32]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;

pub fn alphanumeric_tokens(string: &str) -> impl Iterator<Item = &str> {
    let is_alphanumeric = |s: &&str| s.chars().next().map_or(false, char::is_alphanumeric);
    string.linear_group_by_key(|c| c.is_alphanumeric()).filter(is_alphanumeric)
}

#[derive(Clone)]
pub struct Index {
    pub main: PolyDatabase,
    pub postings_ids: Database<Str, ByteSlice>,
    pub prefix_postings_ids: Database<Str, ByteSlice>,
    pub documents: Database<OwnedType<BEU32>, ByteSlice>,
}

impl Index {
    pub fn new(env: &heed::Env) -> heed::Result<Index> {
        let main = env.create_poly_database(None)?;
        let postings_ids = env.create_database(Some("postings-ids"))?;
        let prefix_postings_ids = env.create_database(Some("prefix-postings-ids"))?;
        let documents = env.create_database(Some("documents"))?;

        Ok(Index { main, postings_ids, prefix_postings_ids, documents })
    }

    pub fn headers<'t>(&self, rtxn: &'t heed::RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, "headers")
    }

    pub fn search(&self, rtxn: &heed::RoTxn, query: &str) -> anyhow::Result<Vec<DocumentId>> {
        let fst = match self.main.get::<_, Str, ByteSlice>(rtxn, "words-fst")? {
            Some(bytes) => fst::Set::new(bytes)?,
            None => return Ok(Vec::new()),
        };

        // Building these factories is not free.
        let lev0 = LEVDIST0.get_or_init(|| LevBuilder::new(0, true));
        let lev1 = LEVDIST1.get_or_init(|| LevBuilder::new(1, true));
        let lev2 = LEVDIST2.get_or_init(|| LevBuilder::new(2, true));

        let words: Vec<_> = alphanumeric_tokens(query).collect();
        let number_of_words = words.len();
        let dfas = words.into_iter().enumerate().map(|(i, word)| {
            let word = word.cow_to_lowercase();
            let is_last = i + 1 == number_of_words;
            let dfa = match word.len() {
                0..=4 => if is_last { lev0.build_prefix_dfa(&word) } else { lev0.build_dfa(&word) },
                5..=8 => if is_last { lev1.build_prefix_dfa(&word) } else { lev1.build_dfa(&word) },
                _     => if is_last { lev2.build_prefix_dfa(&word) } else { lev2.build_dfa(&word) },
            };
            (word, dfa)
        });

        let mut intersect_result: Option<RoaringBitmap> = None;
        for (word, dfa) in dfas {
            let before = Instant::now();

            let mut union_result = RoaringBitmap::default();
            if word.len() <= 4 {
                if let Some(ids) = self.prefix_postings_ids.get(rtxn, &word[..word.len().min(4)])? {
                    union_result = RoaringBitmap::deserialize_from(ids)?;
                }
            } else {
                let mut stream = fst.search(dfa).into_stream();
                while let Some(word) = stream.next() {
                    let word = std::str::from_utf8(word)?;
                    if let Some(ids) = self.postings_ids.get(rtxn, word)? {
                        let right = RoaringBitmap::deserialize_from(ids)?;
                        union_result.union_with(&right);
                    }
                }
            }
            eprintln!("union for {:?} took {:.02?}", word, before.elapsed());

            intersect_result = match intersect_result.take() {
                Some(mut left) => {
                    let before = Instant::now();
                    let left_len = left.len();
                    left.intersect_with(&union_result);
                    eprintln!("intersect between {:?} and {:?} took {:.02?}",
                        left_len, union_result.len(), before.elapsed());
                    Some(left)
                },
                None => Some(union_result),
            };
        }

        Ok(intersect_result.unwrap_or_default().iter().take(20).collect())
    }
}
