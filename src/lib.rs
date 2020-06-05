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
use once_cell::sync::OnceCell;
use roaring::RoaringBitmap;

use self::query_tokens::{QueryTokens, QueryToken};

static LEVDIST0: OnceCell<LevBuilder> = OnceCell::new();
static LEVDIST1: OnceCell<LevBuilder> = OnceCell::new();
static LEVDIST2: OnceCell<LevBuilder> = OnceCell::new();

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32 = smallvec::SmallVec<[u8; 32]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;

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
            let dfa = match word.len() {
                0..=4 => if is_prefix { lev0.build_prefix_dfa(&word) } else if quoted { lev0.build_dfa(&word) } else { lev0.build_dfa(&word) },
                5..=8 => if is_prefix { lev1.build_prefix_dfa(&word) } else if quoted { lev0.build_dfa(&word) } else { lev1.build_dfa(&word) },
                _     => if is_prefix { lev2.build_prefix_dfa(&word) } else if quoted { lev0.build_dfa(&word) } else { lev2.build_dfa(&word) },
            };
            (word, is_prefix, dfa)
        });

        let mut intersect_result: Option<RoaringBitmap> = None;
        for (word, is_prefix, dfa) in dfas {
            let before = Instant::now();

            let mut union_result = RoaringBitmap::default();
            let count = if word.len() <= 4 && is_prefix {
                if let Some(ids) = self.prefix_postings_ids.get(rtxn, &word[..word.len().min(5)])? {
                    union_result = RoaringBitmap::deserialize_from(ids)?;
                }
                1
            } else {
                let mut count = 0;
                let mut stream = fst.search(dfa).into_stream();
                while let Some(word) = stream.next() {
                    count += 1;
                    let word = std::str::from_utf8(word)?;
                    if let Some(ids) = self.postings_ids.get(rtxn, word)? {
                        let right = RoaringBitmap::deserialize_from(ids)?;
                        union_result.union_with(&right);
                    }
                }
                count
            };
            eprintln!("with {:?} words union for {:?} gives {:?} took {:.02?}",
                count, word, union_result.len(), before.elapsed());

            match &mut intersect_result {
                Some(left) => {
                    let before = Instant::now();
                    let left_len = left.len();
                    left.intersect_with(&union_result);
                    eprintln!("intersect between {:?} and {:?} gives {:?} took {:.02?}",
                        left_len, union_result.len(), left.len(), before.elapsed());
                },
                None => intersect_result = Some(union_result),
            }
        }

        eprintln!("{} candidates", intersect_result.as_ref().map_or(0, |r| r.len()));

        Ok(intersect_result.unwrap_or_default().iter().take(20).collect())
    }
}
