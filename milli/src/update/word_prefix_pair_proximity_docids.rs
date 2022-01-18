use std::collections::HashMap;

use fst::IntoStreamer;
use grenad::CompressionType;
use heed::types::ByteSlice;
use log::debug;
use slice_group_by::GroupBy;

use crate::update::index_documents::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_lmdb_database, MergeFn, WriteMethod,
};
use crate::{Index, Result};

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
    pub fn execute<A: AsRef<[u8]>>(self, old_prefix_fst: &fst::Set<A>) -> Result<()> {
        debug!("Computing and writing the word prefix pair proximity docids into LMDB on disk...");

        self.index.word_prefix_pair_proximity_docids.clear(self.wtxn)?;

        // Here we create a sorter akin to the previous one.
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

        let mut db =
            self.index.word_pair_proximity_docids.remap_data_type::<ByteSlice>().iter(self.wtxn)?;

        let mut buffer = Vec::new();
        let mut current_prefixes: Option<&&[String]> = None;
        let mut prefixes_cache = HashMap::new();
        while let Some(((w1, w2, prox), data)) = db.next().transpose()? {
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
                            Some(value) => value.push(data),
                            None => {
                                prefixes_cache.insert(buffer.clone(), vec![data]);
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
        drop(db);

        // We finally write the word prefix pair proximity docids into the LMDB database.
        sorter_into_lmdb_database(
            self.wtxn,
            *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
            word_prefix_pair_proximity_docids_sorter,
            merge_cbo_roaring_bitmaps,
            WriteMethod::Append,
        )?;

        Ok(())
    }
}

fn write_prefixes_in_sorter(
    prefixes: &mut HashMap<Vec<u8>, Vec<&[u8]>>,
    sorter: &mut grenad::Sorter<MergeFn>,
) -> Result<()> {
    for (key, data_slices) in prefixes.drain() {
        for data in data_slices {
            sorter.insert(&key, data)?;
        }
    }

    Ok(())
}
