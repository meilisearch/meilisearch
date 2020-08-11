mod best_proximity;
mod heed_codec;
mod iter_shortest_paths;
mod query_tokens;
mod transitive_arc;

use std::collections::{HashSet, HashMap};
use std::fs::{File, OpenOptions};
use std::hash::BuildHasherDefault;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use cow_utils::CowUtils;
use fst::{IntoStreamer, Streamer};
use fxhash::{FxHasher32, FxHasher64};
use heed::types::*;
use heed::{PolyDatabase, Database};
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use log::debug;
use memmap::Mmap;
use once_cell::sync::Lazy;
use oxidized_mtbl as omtbl;
use roaring::RoaringBitmap;

use self::best_proximity::BestProximity;
use self::heed_codec::RoaringBitmapCodec;
use self::query_tokens::{QueryTokens, QueryToken};
use self::transitive_arc::TransitiveArc;

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
pub type Attribute = u32;
pub type Position = u32;

#[derive(Clone)]
pub struct Index {
    // The database path, where the LMDB and MTBL files are.
    path: PathBuf,
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
    /// The MTBL store that contains the documents content.
    documents: omtbl::Reader<TransitiveArc<Mmap>>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(env: &heed::Env, path: P) -> anyhow::Result<Index> {
        let documents_path = path.as_ref().join("documents.mtbl");
        let mut documents = OpenOptions::new().create(true).write(true).read(true).open(documents_path)?;
        // If the file is empty we must initialize it like an empty MTBL database.
        if documents.metadata()?.len() == 0 {
            omtbl::Writer::new(&mut documents).finish()?;
        }
        let documents = unsafe { memmap::Mmap::map(&documents)? };

        Ok(Index {
            path: path.as_ref().to_path_buf(),
            main: env.create_poly_database(None)?,
            word_positions: env.create_database(Some("word-positions"))?,
            prefix_word_positions: env.create_database(Some("prefix-word-positions"))?,
            word_position_docids: env.create_database(Some("word-position-docids"))?,
            prefix_word_position_docids: env.create_database(Some("prefix-word-position-docids"))?,
            word_attribute_docids: env.create_database(Some("word-attribute-docids"))?,
            documents: omtbl::Reader::new(TransitiveArc(Arc::new(documents)))?,
        })
    }

    pub fn refresh_documents(&mut self) -> anyhow::Result<()> {
        let documents_path = self.path.join("documents.mtbl");
        let documents = File::open(&documents_path)?;
        let documents = unsafe { memmap::Mmap::map(&documents)? };
        self.documents = omtbl::Reader::new(TransitiveArc(Arc::new(documents)))?;
        Ok(())
    }

    pub fn put_headers(&self, wtxn: &mut heed::RwTxn, headers: &[u8]) -> anyhow::Result<()> {
        Ok(self.main.put::<_, Str, ByteSlice>(wtxn, "headers", headers)?)
    }

    pub fn headers<'t>(&self, rtxn: &'t heed::RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, "headers")
    }

    pub fn number_of_attributes<'t>(&self, rtxn: &'t heed::RoTxn) -> anyhow::Result<Option<usize>> {
        match self.headers(rtxn)? {
            Some(headers) => {
                let mut rdr = csv::Reader::from_reader(headers);
                let headers = rdr.headers()?;
                Ok(Some(headers.len()))
            }
            None => Ok(None),
        }
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

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn documents<I: IntoIterator<Item=DocumentId>>(&self, iter: I) -> anyhow::Result<Vec<(DocumentId, Vec<u8>)>> {
        iter.into_iter().map(|id| {
            let key = id.to_be_bytes();
            let content = self.documents.clone().get(&key)?.with_context(|| format!("Could not find document {}.", id))?;
            Ok((id, content.as_ref().to_vec()))
        })
        .collect()
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self) -> usize {
        self.documents.metadata().count_entries as usize
    }

    pub fn search(&self, rtxn: &heed::RoTxn, query: &str) -> anyhow::Result<(HashSet<String>, Vec<DocumentId>)> {
        let fst = match self.fst(rtxn)? {
            Some(fst) => fst,
            None => return Ok(Default::default()),
        };

        let (lev0, lev1, lev2) = (&LEVDIST0, &LEVDIST1, &LEVDIST2);

        let words: Vec<_> = QueryTokens::new(query).collect();
        let ends_with_whitespace = query.chars().last().map_or(false, char::is_whitespace);
        let number_of_words = words.len();
        let dfas = words.into_iter().enumerate().map(|(i, word)| {
            let (word, quoted) = match word {
                QueryToken::Free(word) => (word.cow_to_lowercase(), word.len() <= 3),
                QueryToken::Quoted(word) => (word.cow_to_lowercase(), true),
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
            let mut stream = fst.search_with_state(&dfa).into_stream();
            while let Some((word, state)) = stream.next() {
                let word = std::str::from_utf8(word)?;
                let distance = dfa.distance(state);
                debug!("found {:?} at distance of {}", word, distance.to_u8());
                if let Some(positions) = self.word_positions.get(rtxn, word)? {
                    union_positions.union_with(&positions);
                    derived_words.push((word.as_bytes().to_vec(), distance.to_u8(), positions));
                    count += 1;
                }
            }

            debug!("{} words for {:?} we have found positions {:?} in {:.02?}",
                count, word, union_positions, before.elapsed());
            words.push(derived_words);
            positions.push(union_positions.iter().collect());
        }

        // We compute the docids candidates for these words (and derived words).
        // We do a union between all the docids of each of the words and derived words,
        // we got N unions (where N is the number of query words), we then intersect them.
        // TODO we must store the words documents ids to avoid these unions.
        let mut candidates = RoaringBitmap::new();
        let number_of_attributes = self.number_of_attributes(rtxn)?.map_or(0, |n| n as u32);
        for (i, derived_words) in words.iter().enumerate() {
            let mut union_docids = RoaringBitmap::new();
            for (word, _distance, _positions) in derived_words {
                for attr in 0..number_of_attributes {
                    let mut key = word.to_vec();
                    key.extend_from_slice(&attr.to_be_bytes());
                    if let Some(right) = self.word_attribute_docids.get(rtxn, &key)? {
                        union_docids.union_with(&right);
                    }
                }
            }
            if i == 0 {
                candidates = union_docids;
            } else {
                candidates.intersect_with(&union_docids);
            }
        }

        debug!("The candidates are {:?}", candidates);
        debug!("Retrieving words positions took {:.02?}", before.elapsed());

        // Returns the union of the same position for all the derived words.
        let unions_word_pos = |word: usize, pos: u32| {
            let mut union_docids = RoaringBitmap::new();
            for (word, _distance, attrs) in &words[word] {
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

        // Returns the union of the same attribute for all the derived words.
        let unions_word_attr = |word: usize, attr: u32| {
            let mut union_docids = RoaringBitmap::new();
            for (word, _distance, _) in &words[word] {
                let mut key = word.clone();
                key.extend_from_slice(&attr.to_be_bytes());
                if let Some(right) = self.word_attribute_docids.get(rtxn, &key).unwrap() {
                    union_docids.union_with(&right);
                }
            }
            union_docids
        };

        let mut union_cache = HashMap::new();
        let mut intersect_cache = HashMap::new();

        let mut attribute_union_cache = HashMap::new();
        let mut attribute_intersect_cache = HashMap::new();

        // Returns `true` if there is documents in common between the two words and positions given.
        let mut contains_documents = |(lword, lpos), (rword, rpos), union_cache: &mut HashMap<_, _>, candidates: &RoaringBitmap| {
            if lpos == rpos { return false }

            let (lattr, _) = best_proximity::extract_position(lpos);
            let (rattr, _) = best_proximity::extract_position(rpos);

            if lattr == rattr {
                // We retrieve or compute the intersection between the two given words and positions.
                *intersect_cache.entry(((lword, lpos), (rword, rpos))).or_insert_with(|| {
                    // We retrieve or compute the unions for the two words and positions.
                    union_cache.entry((lword, lpos)).or_insert_with(|| unions_word_pos(lword, lpos));
                    union_cache.entry((rword, rpos)).or_insert_with(|| unions_word_pos(rword, rpos));

                    // TODO is there a way to avoid this double gets?
                    let lunion_docids = union_cache.get(&(lword, lpos)).unwrap();
                    let runion_docids = union_cache.get(&(rword, rpos)).unwrap();

                    // We first check that the docids of these unions are part of the candidates.
                    if lunion_docids.is_disjoint(candidates) { return false }
                    if runion_docids.is_disjoint(candidates) { return false }

                    !lunion_docids.is_disjoint(&runion_docids)
                })
            } else {
                *attribute_intersect_cache.entry(((lword, lattr), (rword, rattr))).or_insert_with(|| {
                    // We retrieve or compute the unions for the two words and positions.
                    attribute_union_cache.entry((lword, lattr)).or_insert_with(|| unions_word_attr(lword, lattr));
                    attribute_union_cache.entry((rword, rattr)).or_insert_with(|| unions_word_attr(rword, rattr));

                    // TODO is there a way to avoid this double gets?
                    let lunion_docids = attribute_union_cache.get(&(lword, lattr)).unwrap();
                    let runion_docids = attribute_union_cache.get(&(rword, rattr)).unwrap();

                    // We first check that the docids of these unions are part of the candidates.
                    if lunion_docids.is_disjoint(candidates) { return false }
                    if runion_docids.is_disjoint(candidates) { return false }

                    !lunion_docids.is_disjoint(&runion_docids)
                })
            }
        };

        let mut documents = Vec::new();
        let mut iter = BestProximity::new(positions);
        while let Some((proximity, mut positions)) = iter.next(|l, r| contains_documents(l, r, &mut union_cache, &candidates)) {
            positions.sort_unstable();

            let same_prox_before = Instant::now();
            let mut same_proximity_union = RoaringBitmap::default();

            for positions in positions {
                let before = Instant::now();

                // Precompute the potentially missing unions
                positions.iter().enumerate().for_each(|(word, pos)| {
                    union_cache.entry((word, *pos)).or_insert_with(|| unions_word_pos(word, *pos));
                });

                // Retrieve the unions along with the popularity of it.
                let mut to_intersect: Vec<_> = positions.iter()
                    .enumerate()
                    .map(|(word, pos)| {
                        let docids = union_cache.get(&(word, *pos)).unwrap();
                        (docids.len(), docids)
                    })
                    .collect();

                // Sort the unions by popuarity to help reduce
                // the number of documents as soon as possible.
                to_intersect.sort_unstable_by_key(|(l, _)| *l);
                let elapsed_retrieving = before.elapsed();

                let before_intersect = Instant::now();
                let intersect_docids: Option<RoaringBitmap> = to_intersect.into_iter()
                    .fold(None, |acc, (_, union_docids)| {
                        match acc {
                            Some(mut left) => {
                                left.intersect_with(&union_docids);
                                Some(left)
                            },
                            None => Some(union_docids.clone()),
                        }
                    });

                debug!("retrieving words took {:.02?} and took {:.02?} to intersect",
                    elapsed_retrieving, before_intersect.elapsed());

                debug!("for proximity {:?} {:?} we took {:.02?} to find {} documents",
                    proximity, positions, before.elapsed(),
                    intersect_docids.as_ref().map_or(0, |rb| rb.len()));

                if let Some(intersect_docids) = intersect_docids {
                    same_proximity_union.union_with(&intersect_docids);
                }

                // We found enough documents we can stop here
                if documents.iter().map(RoaringBitmap::len).sum::<u64>() + same_proximity_union.len() >= 20 {
                    debug!("proximity {} took a total of {:.02?}", proximity, same_prox_before.elapsed());
                    break;
                }
            }

            // We achieve to find valid documents ids so we remove them from the candidates list.
            candidates.difference_with(&same_proximity_union);

            documents.push(same_proximity_union);

            // We remove the double occurences of documents.
            for i in 0..documents.len() {
                if let Some((docs, others)) = documents[..=i].split_last_mut() {
                    others.iter().for_each(|other| docs.difference_with(other));
                }
            }
            documents.retain(|rb| !rb.is_empty());

            debug!("documents: {:?}", documents);
            debug!("proximity {} took a total of {:.02?}", proximity, same_prox_before.elapsed());

            // We found enough documents we can stop here.
            if documents.iter().map(RoaringBitmap::len).sum::<u64>() >= 20 {
                break;
            }
        }

        debug!("{} final candidates", documents.iter().map(RoaringBitmap::len).sum::<u64>());
        let words = words.into_iter().flatten().map(|(w, _distance, _)| String::from_utf8(w).unwrap()).collect();
        let documents = documents.iter().flatten().take(20).collect();

        Ok((words, documents))
    }
}
