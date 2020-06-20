mod best_proximity;
mod iter_shortest_paths;
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
use self::best_proximity::BestProximity;

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

pub type FastMap4<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher32>>;
pub type SmallString32 = smallstr::SmallString<[u8; 32]>;
pub type SmallVec32<T> = smallvec::SmallVec<[T; 32]>;
pub type SmallVec16<T> = smallvec::SmallVec<[T; 16]>;
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
                if let Some(attrs) = self.postings_attrs.get(rtxn, word)? {
                    let right = RoaringBitmap::deserialize_from_slice(attrs)?;
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

        eprintln!("Retrieving words positions took {:.02?}", before.elapsed());

        let mut documents = Vec::new();

        let mut debug_intersects = HashMap::new();
        let mut intersect_cache = HashMap::new();
        let mut lunion_docids = RoaringBitmap::default();
        let mut runion_docids = RoaringBitmap::default();
        let contains_documents = |(lword, lpos): (usize, u32), (rword, rpos): (usize, u32)| {
            let proximity = best_proximity::positions_proximity(lpos, rpos);
            if proximity == 0 { return false }

            *intersect_cache.entry(((lword, lpos), (rword, rpos))).or_insert_with(|| {
                let (nb_words, nb_docs_intersect, lnblookups, lnbbitmaps, rnblookups, rnbbitmaps) =
                    debug_intersects.entry((lword, lpos, rword, rpos, proximity)).or_default();

                let left = &words[lword];
                let right = &words[rword];

                *nb_words = left.len() + right.len();

                let mut l_lookups = 0;
                let mut l_bitmaps = 0;
                let mut r_lookups = 0;
                let mut r_bitmaps = 0;

                // This for the left word
                lunion_docids.clear();
                for (word, attrs) in left {
                    if attrs.contains(lpos) {
                        l_lookups += 1;
                        let mut key = word.clone();
                        key.extend_from_slice(&lpos.to_be_bytes());
                        if let Some(attrs) = self.postings_ids.get(rtxn, &key).unwrap() {
                            l_bitmaps += 1;
                            let right = RoaringBitmap::deserialize_from_slice(attrs).unwrap();
                            lunion_docids.union_with(&right);
                        }
                    }
                }

                // This for the right word
                runion_docids.clear();
                for (word, attrs) in right {
                    if attrs.contains(rpos) {
                        r_lookups += 1;
                        let mut key = word.clone();
                        key.extend_from_slice(&rpos.to_be_bytes());
                        if let Some(attrs) = self.postings_ids.get(rtxn, &key).unwrap() {
                            r_bitmaps += 1;
                            let right = RoaringBitmap::deserialize_from_slice(attrs).unwrap();
                            runion_docids.union_with(&right);
                        }
                    }
                }

                let intersect_docids = &mut lunion_docids;
                intersect_docids.intersect_with(&runion_docids);

                *lnblookups = l_lookups;
                *lnbbitmaps = l_bitmaps;
                *rnblookups = r_lookups;
                *rnbbitmaps = r_bitmaps;
                *nb_docs_intersect += intersect_docids.len();

                !intersect_docids.is_empty()
            })
        };

        for (proximity, mut positions) in BestProximity::new(positions, contains_documents) {
            positions.sort_unstable();

            let same_prox_before = Instant::now();
            let mut same_proximity_union = RoaringBitmap::default();

            for positions in positions {
                let before = Instant::now();

                let mut intersect_docids: Option<RoaringBitmap> = None;
                for (derived_words, pos) in words.iter().zip(positions.clone()) {
                    let mut count = 0;
                    let mut union_docids = RoaringBitmap::default();

                    let before = Instant::now();

                    // TODO re-enable the prefixes system
                    for (word, attrs) in derived_words.iter() {
                        if attrs.contains(pos) {
                            let mut key = word.clone();
                            key.extend_from_slice(&pos.to_be_bytes());
                            if let Some(attrs) = self.postings_ids.get(rtxn, &key)? {
                                let right = RoaringBitmap::deserialize_from_slice(attrs)?;
                                union_docids.union_with(&right);
                                count += 1;
                            }
                        }
                    }

                    let before_intersect = Instant::now();

                    match &mut intersect_docids {
                        Some(left) => left.intersect_with(&union_docids),
                        None => intersect_docids = Some(union_docids),
                    }

                    eprintln!("retrieving {} word took {:.02?} and took {:.02?} to intersect",
                        count, before.elapsed(), before_intersect.elapsed());
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

        if cfg!(feature = "intersect-to-csv") {
            debug_intersects_to_csv(debug_intersects);
        }

        eprintln!("{} candidates", documents.iter().map(RoaringBitmap::len).sum::<u64>());
        Ok(documents.iter().flatten().take(20).collect())
    }
}

fn debug_intersects_to_csv(intersects: HashMap<(usize, u32, usize, u32, u32), (usize, u64, usize, usize, usize, usize)>) {
    let mut wrt = csv::Writer::from_path("intersects-stats.csv").unwrap();
    wrt.write_record(&[
        "proximity",
        "lword",
        "lpos",
        "rword",
        "rpos",
        "nb_derived_words",
        "nb_docs_intersect",
        "lnblookups",
        "lnbbitmaps",
        "rnblookups",
        "rnbbitmaps",
    ]).unwrap();

    for ((lword, lpos, rword, rpos, proximity), vals) in intersects {
        let (
            nb_derived_words,
            nb_docs_intersect,
            lnblookups,
            lnbbitmaps,
            rnblookups,
            rnbbitmaps,
        ) = vals;

        let proximity = proximity.to_string();
        let lword = lword.to_string();
        let lpos = lpos.to_string();
        let rword = rword.to_string();
        let rpos = rpos.to_string();
        let nb_derived_words = nb_derived_words.to_string();
        let nb_docs_intersect = nb_docs_intersect.to_string();
        let lnblookups = lnblookups.to_string();
        let lnbbitmaps = lnbbitmaps.to_string();
        let rnblookups = rnblookups.to_string();
        let rnbbitmaps = rnbbitmaps.to_string();

        wrt.write_record(&[
            &proximity,
            &lword,
            &lpos,
            &rword,
            &rpos,
            &nb_derived_words,
            &nb_docs_intersect,
            &lnblookups,
            &lnbbitmaps,
            &rnblookups,
            &rnbbitmaps,
        ]).unwrap();
    }
}
