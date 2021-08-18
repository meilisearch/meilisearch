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
    threshold: u32,
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
            threshold: 100,
        }
    }

    /// Set the number of words required to make a prefix be part of the words prefixes
    /// database. If a word prefix is supposed to match more than this number of words in the
    /// dictionnary, therefore this prefix is added to the words prefixes datastructures.
    ///
    /// Default value is 100. This value must be higher than 50 and will be clamped
    /// to these bound otherwise.
    pub fn threshold(&mut self, value: u32) -> &mut Self {
        self.threshold = value.max(50);
        self
    }

    pub fn execute(self) -> Result<()> {
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
        let prefix_fst_keys = prefix_fst.into_stream().into_bytes();
        let prefix_fst_keys: Vec<_> = prefix_fst_keys
            .as_slice()
            .linear_group_by_key(|x| std::str::from_utf8(&x).unwrap().chars().nth(0).unwrap())
            .collect();

        let mut db =
            self.index.word_pair_proximity_docids.remap_data_type::<ByteSlice>().iter(self.wtxn)?;

        let mut buffer = Vec::new();
        let mut current_prefixes: Option<&&[Vec<u8>]> = None;
        let mut prefixes_cache = HashMap::new();
        while let Some(((w1, w2, prox), data)) = db.next().transpose()? {
            current_prefixes = match current_prefixes.take() {
                Some(prefixes) if w2.as_bytes().starts_with(&prefixes[0]) => Some(prefixes),
                _otherwise => {
                    write_prefixes_in_sorter(
                        &mut prefixes_cache,
                        &mut word_prefix_pair_proximity_docids_sorter,
                        self.threshold,
                    )?;
                    prefix_fst_keys.iter().find(|prefixes| w2.as_bytes().starts_with(&prefixes[0]))
                }
            };

            if let Some(prefixes) = current_prefixes {
                buffer.clear();
                buffer.extend_from_slice(w1.as_bytes());
                buffer.push(0);
                for prefix in prefixes.iter().filter(|prefix| w2.as_bytes().starts_with(prefix)) {
                    buffer.truncate(w1.len() + 1);
                    buffer.extend_from_slice(prefix);
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

        write_prefixes_in_sorter(
            &mut prefixes_cache,
            &mut word_prefix_pair_proximity_docids_sorter,
            self.threshold,
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
    min_word_per_prefix: u32,
) -> Result<()> {
    for (i, (key, data_slices)) in prefixes.drain().enumerate() {
        // if the number of words prefixed by the prefix is higher than the threshold,
        // we insert it in the sorter.
        if data_slices.len() > min_word_per_prefix as usize {
            for data in data_slices {
                sorter.insert(&key, data)?;
            }
        // if the first prefix isn't elligible for insertion,
        // then the other prefixes can't be elligible.
        } else if i == 0 {
            break;
        }
    }

    Ok(())
}
