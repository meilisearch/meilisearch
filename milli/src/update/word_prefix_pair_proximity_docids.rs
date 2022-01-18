use std::collections::{HashMap, HashSet};

use fst::{IntoStreamer, Streamer};
use grenad::{CompressionType, MergerBuilder};
use heed::BytesDecode;
use log::debug;
use slice_group_by::GroupBy;

use crate::update::index_documents::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_lmdb_database, CursorClonableMmap,
    MergeFn, WriteMethod,
};
use crate::{Index, Result, StrStrU8Codec};

pub struct WordPrefixPairProximityDocids<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    max_proximity: u8,
    max_prefix_length: usize,
}

impl<'t, 'u, 'i> WordPrefixPairProximityDocids<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> WordPrefixPairProximityDocids<'t, 'u, 'i> {
        WordPrefixPairProximityDocids {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_nb_chunks: None,
            max_memory: None,
            max_proximity: 4,
            max_prefix_length: 2,
        }
    }

    /// Set the maximum proximity required to make a prefix be part of the words prefixes
    /// database. If two words are too far from the threshold the associated documents will
    /// not be part of the prefix database.
    ///
    /// Default value is 4. This value must be lower or equal than 7 and will be clamped
    /// to this bound otherwise.
    pub fn max_proximity(&mut self, value: u8) -> &mut Self {
        self.max_proximity = value.max(7);
        self
    }

    /// Set the maximum length the prefix of a word pair is allowed to have to be part of the words
    /// prefixes database. If the prefix length is higher than the threshold, the associated documents
    /// will not be part of the prefix database.
    ///
    /// Default value is 2.
    pub fn max_prefix_length(&mut self, value: usize) -> &mut Self {
        self.max_prefix_length = value;
        self
    }

    #[logging_timer::time("WordPrefixPairProximityDocids::{}")]
    pub fn execute<A: AsRef<[u8]>>(
        self,
        new_word_pair_proximity_docids: Vec<grenad::Reader<CursorClonableMmap>>,
        old_prefix_fst: &fst::Set<A>,
    ) -> Result<()> {
        debug!("Computing and writing the word prefix pair proximity docids into LMDB on disk...");

        // We retrieve and merge the created word pair proximities docids entries
        // for the newly added documents.
        let mut wppd_merger = MergerBuilder::new(merge_cbo_roaring_bitmaps);
        wppd_merger.extend(new_word_pair_proximity_docids);
        let mut wppd_iter = wppd_merger.build().into_merger_iter()?;

        let mut word_prefix_pair_proximity_docids_sorter = create_sorter(
            merge_cbo_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        let prefix_fst = self.index.words_prefixes_fst(self.wtxn)?;
        let prefix_fst_keys = prefix_fst.into_stream().into_strs()?;
        let prefix_fst_keys: Vec<_> =
            prefix_fst_keys.as_slice().linear_group_by_key(|x| x.chars().nth(0).unwrap()).collect();

        // We compute the set of prefixes that are no more part of the prefix fst.
        let suppr_pw = stream_into_hashset(old_prefix_fst.op().add(&prefix_fst).difference());

        let mut buffer = Vec::new();
        let mut current_prefixes: Option<&&[String]> = None;
        let mut prefixes_cache = HashMap::new();
        while let Some((key, data)) = wppd_iter.next()? {
            let (w1, w2, prox) = StrStrU8Codec::bytes_decode(key).ok_or(heed::Error::Decoding)?;
            if prox > self.max_proximity {
                continue;
            }

            current_prefixes = match current_prefixes.take() {
                Some(prefixes) if w2.starts_with(&prefixes[0]) => Some(prefixes),
                _otherwise => {
                    write_prefixes_in_sorter(
                        &mut prefixes_cache,
                        &mut word_prefix_pair_proximity_docids_sorter,
                    )?;
                    prefix_fst_keys.iter().find(|prefixes| w2.starts_with(&prefixes[0]))
                }
            };

            if let Some(prefixes) = current_prefixes {
                buffer.clear();
                buffer.extend_from_slice(w1.as_bytes());
                buffer.push(0);
                for prefix in prefixes.iter() {
                    if prefix.len() <= self.max_prefix_length && w2.starts_with(prefix) {
                        buffer.truncate(w1.len() + 1);
                        buffer.extend_from_slice(prefix.as_bytes());
                        buffer.push(prox);

                        match prefixes_cache.get_mut(&buffer) {
                            Some(value) => value.push(data.to_owned()),
                            None => {
                                prefixes_cache.insert(buffer.clone(), vec![data.to_owned()]);
                            }
                        }
                    }
                }
            }
        }

        write_prefixes_in_sorter(
            &mut prefixes_cache,
            &mut word_prefix_pair_proximity_docids_sorter,
        )?;

        drop(prefix_fst);

        // All of the word prefix pairs in the database that have a w2
        // that is contained in the `suppr_pw` set must be removed as well.
        let mut iter =
            self.index.word_prefix_pair_proximity_docids.iter_mut(self.wtxn)?.lazily_decode_data();
        while let Some(((_, w2, _), _)) = iter.next().transpose()? {
            if suppr_pw.contains(w2.as_bytes()) {
                // Delete this entry as the w2 prefix is no more in the words prefix fst.
                unsafe { iter.del_current()? };
            }
        }

        drop(iter);

        // We finally write and merge the new word prefix pair proximity docids
        // in the LMDB database.
        sorter_into_lmdb_database(
            self.wtxn,
            *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
            word_prefix_pair_proximity_docids_sorter,
            merge_cbo_roaring_bitmaps,
            WriteMethod::GetMergePut,
        )?;

        Ok(())
    }
}

fn write_prefixes_in_sorter(
    prefixes: &mut HashMap<Vec<u8>, Vec<Vec<u8>>>,
    sorter: &mut grenad::Sorter<MergeFn>,
) -> Result<()> {
    for (key, data_slices) in prefixes.drain() {
        for data in data_slices {
            sorter.insert(&key, data)?;
        }
    }

    Ok(())
}

/// Converts an fst Stream into an HashSet.
fn stream_into_hashset<'f, I, S>(stream: I) -> HashSet<Vec<u8>>
where
    I: for<'a> IntoStreamer<'a, Into = S, Item = &'a [u8]>,
    S: 'f + for<'a> Streamer<'a, Item = &'a [u8]>,
{
    let mut hashset = HashSet::new();
    let mut stream = stream.into_stream();
    while let Some(value) = stream.next() {
        hashset.insert(value.to_owned());
    }
    hashset
}
