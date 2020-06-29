mod best_proximity;
mod heed_codec;
mod iter_shortest_paths;
mod query_tokens;
pub mod cache;

use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::time::Instant;

use cow_utils::CowUtils;
use fst::{IntoStreamer, Streamer};
use fxhash::{FxHasher32, FxHasher64};
use heed::types::*;
use heed::{PolyDatabase, Database};
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use once_cell::sync::Lazy;
use roaring::RoaringBitmap;

use self::best_proximity::BestProximity;
use self::heed_codec::RoaringBitmapCodec;
use self::query_tokens::{QueryTokens, QueryToken};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type FastMap8<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher64>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
pub type BEU32 = heed::zerocopy::U32<heed::byteorder::BE>;
pub type DocumentId = u32;
pub type AttributeId = u32;

#[derive(Clone)]
pub struct Index {
    /// Contains many different types (e.g. the documents CSV headers).
    pub main: PolyDatabase,
    /// A word and all the positions where it appears in the whole dataset.
    pub word_positions: Database<Str, RoaringBitmapCodec>,
    pub prefix_word_positions: Database<Str, RoaringBitmapCodec>,
    /// Maps a word at a position (u32) and all the documents ids where it appears.
    pub word_position_docids: Database<ByteSlice, RoaringBitmapCodec>,
    pub prefix_word_position_docids: Database<ByteSlice, RoaringBitmapCodec>,
    /// Maps a word and an attribute (u32) to all the documents ids that it appears in.
    pub word_attribute_docids: Database<ByteSlice, RoaringBitmapCodec>,
    /// Maps an internal document to the content of the document in CSV.
    pub documents: Database<OwnedType<BEU32>, ByteSlice>,
}

impl Index {
    pub fn new(env: &heed::Env) -> heed::Result<Index> {
        Ok(Index {
            main: env.create_poly_database(None)?,
            word_positions: env.create_database(Some("word-positions"))?,
            prefix_word_positions: env.create_database(Some("prefix-word-positions"))?,
            word_position_docids: env.create_database(Some("word-position-docids"))?,
            prefix_word_position_docids: env.create_database(Some("prefix-word-position-docids"))?,
            word_attribute_docids: env.create_database(Some("word-attribute-docids"))?,
            documents: env.create_database(Some("documents"))?,
        })
    }

    pub fn put_headers(&self, wtxn: &mut heed::RwTxn, headers: &[u8]) -> anyhow::Result<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(wtxn, "headers", headers)?)
    }

    pub fn headers<'t>(&self, rtxn: &'t heed::RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, "headers")
    }

    pub fn put_fst<A: AsRef<[u8]>>(&self, wtxn: &mut heed::RwTxn, fst: &fst::Set<A>) -> anyhow::Result<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(wtxn, "words-fst", fst.as_fst().as_bytes())?)
    }

    pub fn fst<'t>(&self, rtxn: &'t heed::RoTxn) -> anyhow::Result<Option<fst::Set<&'t [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, "words-fst")? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn search(&self, rtxn: &heed::RoTxn, query: &str) -> anyhow::Result<Vec<DocumentId>> {
        let fst = match self.fst(rtxn)? {
            Some(fst) => fst,
            None => return Ok(vec![]),
        };

        let (lev0, lev1, lev2) = (&LEVDIST0, &LEVDIST1, &LEVDIST2);

        let words: Vec<_> = QueryTokens::new(query).collect();
        let ends_with_whitespace = query.chars().last().map_or(false, char::is_whitespace);
        let number_of_words = words.len();
        let dfas = words.into_iter().enumerate().map(|(i, word)| {
            let (word, quoted) = match word {
                QueryToken::Free(word) => (word.cow_to_lowercase(), word.len() <= 3),
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

        let mut words = Vec::new();
        let mut positions = Vec::new();
        let before = Instant::now();

        for (word, _is_prefix, dfa) in dfas {
            let before = Instant::now();

            let mut count = 0;
            let mut union_positions = RoaringBitmap::default();
            let mut derived_words = Vec::new();
            // TODO re-enable the prefixes system
            let mut stream = fst.search(&dfa).into_stream();
            while let Some(word) = stream.next() {
                let word = std::str::from_utf8(word)?;
                if let Some(right) = self.word_positions.get(rtxn, word)? {
                    union_positions.union_with(&right);
                    derived_words.push((word.as_bytes().to_vec(), right));
                    count += 1;
                }
            }

            eprintln!("{} words for {:?} we have found positions {:?} in {:.02?}",
                count, word, union_positions, before.elapsed());
            words.push(derived_words);
            positions.push(union_positions.iter().collect());
        }

        let mut words_attributes_docids = Vec::new();
        let number_attributes: u32 = 6;

        for i in 0..number_attributes {
            let mut intersect_docids: Option<RoaringBitmap> = None;
            for derived_words in &words {
                let mut union_docids = RoaringBitmap::new();
                for (word, _) in derived_words {
                    // generate the key with the attribute number.
                    let mut key = word.to_vec();
                    key.extend_from_slice(&i.to_be_bytes());

                    if let Some(right) = self.word_attribute_docids.get(rtxn, &key)? {
                        union_docids.union_with(&right);
                    }
                }
                match &mut intersect_docids {
                    Some(left) => left.intersect_with(&union_docids),
                    None => intersect_docids = Some(union_docids),
                }
            }
            words_attributes_docids.push(intersect_docids);
        }

        eprintln!("The documents you must find for each attribute: {:?}", words_attributes_docids);

        eprintln!("Retrieving words positions took {:.02?}", before.elapsed());

        // Returns the union of the same position for all the derived words.
        let unions_word_pos = |word: usize, pos: u32| {
            let mut union_docids = RoaringBitmap::new();
            for (word, attrs) in &words[word] {
                if attrs.contains(pos) {
                    let mut key = word.clone();
                    key.extend_from_slice(&pos.to_be_bytes());
                    if let Some(right) = self.word_position_docids.get(rtxn, &key).unwrap() {
                        union_docids.union_with(&right);
                    }
                }
            }
            union_docids
        };

        let mut union_cache = HashMap::new();
        let mut intersect_cache = HashMap::new();
        // Returns `true` if there is documents in common between the two words and positions given.
        let mut contains_documents = |(lword, lpos), (rword, rpos), union_cache: &mut HashMap<_, _>, words_attributes_docids: &[_]| {
            let proximity = best_proximity::positions_proximity(lpos, rpos);

            if proximity == 0 { return false }

            // We retrieve or compute the intersection between the two given words and positions.
            *intersect_cache.entry(((lword, lpos), (rword, rpos))).or_insert_with(|| {
                // We retrieve or compute the unions for the two words and positions.
                union_cache.entry((lword, lpos)).or_insert_with(|| unions_word_pos(lword, lpos));
                union_cache.entry((rword, rpos)).or_insert_with(|| unions_word_pos(rword, rpos));

                // TODO is there a way to avoid this double gets?
                let lunion_docids = union_cache.get(&(lword, lpos)).unwrap();
                let runion_docids = union_cache.get(&(rword, rpos)).unwrap();

                if proximity <= 7 {
                    let lattr = lpos / 1000;
                    if let Some(docids) = &words_attributes_docids[lattr as usize] {
                        if lunion_docids.is_disjoint(&docids) { return false }
                        if runion_docids.is_disjoint(&docids) { return false }
                    }
                }

                !lunion_docids.is_disjoint(&runion_docids)
            })
        };

        let mut documents = Vec::new();
        let mut iter = BestProximity::new(positions);
        while let Some((proximity, mut positions)) = iter.next(|l, r| contains_documents(l, r, &mut union_cache, &words_attributes_docids)) {
            positions.sort_unstable();

            let same_prox_before = Instant::now();
            let mut same_proximity_union = RoaringBitmap::default();

            for positions in positions {
                let before = Instant::now();

                let mut intersect_docids: Option<RoaringBitmap> = None;
                for (word, pos) in positions.iter().enumerate() {
                    let before = Instant::now();
                    let union_docids = union_cache.entry((word, *pos)).or_insert_with(|| unions_word_pos(word, *pos));

                    let before_intersect = Instant::now();
                    match &mut intersect_docids {
                        Some(left) => left.intersect_with(&union_docids),
                        None => intersect_docids = Some(union_docids.clone()),
                    }

                    eprintln!("retrieving words took {:.02?} and took {:.02?} to intersect",
                        before.elapsed(), before_intersect.elapsed());
                }

                eprintln!("for proximity {:?} {:?} we took {:.02?} to find {} documents",
                    proximity, positions, before.elapsed(),
                    intersect_docids.as_ref().map_or(0, |rb| rb.len()));

                if let Some(intersect_docids) = intersect_docids {
                    same_proximity_union.union_with(&intersect_docids);
                }

                // We found enough documents we can stop here
                if documents.iter().map(RoaringBitmap::len).sum::<u64>() + same_proximity_union.len() >= 20 {
                    eprintln!("proximity {} took a total of {:.02?}", proximity, same_prox_before.elapsed());
                    break;
                }
            }

            // We achieve to find valid documents ids so we remove them from the candidate list.
            for docids in &mut words_attributes_docids {
                if let Some(docids) = docids {
                    docids.difference_with(&same_proximity_union);
                }
            }

            documents.push(same_proximity_union);

            // We remove the double occurences of documents.
            for i in 0..documents.len() {
                if let Some((docs, others)) = documents[..=i].split_last_mut() {
                    others.iter().for_each(|other| docs.difference_with(other));
                }
            }
            documents.retain(|rb| !rb.is_empty());

            eprintln!("documents: {:?}", documents);
            eprintln!("proximity {} took a total of {:.02?}", proximity, same_prox_before.elapsed());

            // We found enough documents we can stop here.
            if documents.iter().map(RoaringBitmap::len).sum::<u64>() >= 20 {
                break;
            }
        }

        eprintln!("{} candidates", documents.iter().map(RoaringBitmap::len).sum::<u64>());
        Ok(documents.iter().flatten().take(20).collect())
    }
}
