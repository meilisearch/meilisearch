use std::collections::{HashMap, HashSet};

use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::BytesDecode;
use log::debug;
use slice_group_by::GroupBy;

use crate::update::index_documents::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_lmdb_database, valid_lmdb_key,
    CursorClonableMmap, MergeFn,
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
    pub fn execute(
        self,
        new_word_pair_proximity_docids: grenad::Reader<CursorClonableMmap>,
        new_prefix_fst_words: &[String],
        common_prefix_fst_words: &[&[String]],
        del_prefix_fst_words: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        debug!("Computing and writing the word prefix pair proximity docids into LMDB on disk...");

        let new_prefix_fst_words: Vec<_> =
            new_prefix_fst_words.linear_group_by_key(|x| x.chars().nth(0).unwrap()).collect();

        let mut new_wppd_iter = new_word_pair_proximity_docids.into_cursor()?;
        let mut word_prefix_pair_proximity_docids_sorter = create_sorter(
            merge_cbo_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        if !common_prefix_fst_words.is_empty() {
            // We compute the prefix docids associated with the common prefixes between
            // the old and new word prefix fst.
            let mut buffer = Vec::new();
            let mut current_prefixes: Option<&&[String]> = None;
            let mut prefixes_cache = HashMap::new();
            while let Some((key, data)) = new_wppd_iter.move_on_next()? {
                let (w1, w2, prox) =
                    StrStrU8Codec::bytes_decode(key).ok_or(heed::Error::Decoding)?;
                if prox > self.max_proximity {
                    continue;
                }

                insert_current_prefix_data_in_sorter(
                    &mut buffer,
                    &mut current_prefixes,
                    &mut prefixes_cache,
                    &mut word_prefix_pair_proximity_docids_sorter,
                    common_prefix_fst_words,
                    self.max_prefix_length,
                    w1,
                    w2,
                    prox,
                    data,
                )?;
            }

            write_prefixes_in_sorter(
                &mut prefixes_cache,
                &mut word_prefix_pair_proximity_docids_sorter,
            )?;
        }

        if !new_prefix_fst_words.is_empty() {
            // We compute the prefix docids associated with the newly added prefixes
            // in the new word prefix fst.
            let mut db_iter = self
                .index
                .word_pair_proximity_docids
                .remap_data_type::<ByteSlice>()
                .iter(self.wtxn)?;

            let mut buffer = Vec::new();
            let mut current_prefixes: Option<&&[String]> = None;
            let mut prefixes_cache = HashMap::new();
            while let Some(((w1, w2, prox), data)) = db_iter.next().transpose()? {
                if prox > self.max_proximity {
                    continue;
                }

                insert_current_prefix_data_in_sorter(
                    &mut buffer,
                    &mut current_prefixes,
                    &mut prefixes_cache,
                    &mut word_prefix_pair_proximity_docids_sorter,
                    &new_prefix_fst_words,
                    self.max_prefix_length,
                    w1,
                    w2,
                    prox,
                    data,
                )?;
            }

            write_prefixes_in_sorter(
                &mut prefixes_cache,
                &mut word_prefix_pair_proximity_docids_sorter,
            )?;
        }

        // All of the word prefix pairs in the database that have a w2
        // that is contained in the `suppr_pw` set must be removed as well.
        if !del_prefix_fst_words.is_empty() {
            let mut iter = self
                .index
                .word_prefix_pair_proximity_docids
                .remap_data_type::<ByteSlice>()
                .iter_mut(self.wtxn)?;
            while let Some(((_, w2, _), _)) = iter.next().transpose()? {
                if del_prefix_fst_words.contains(w2.as_bytes()) {
                    // Delete this entry as the w2 prefix is no more in the words prefix fst.
                    unsafe { iter.del_current()? };
                }
            }
        }

        // We finally write and merge the new word prefix pair proximity docids
        // in the LMDB database.
        sorter_into_lmdb_database(
            self.wtxn,
            *self.index.word_prefix_pair_proximity_docids.as_polymorph(),
            word_prefix_pair_proximity_docids_sorter,
            merge_cbo_roaring_bitmaps,
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
            if valid_lmdb_key(&key) {
                sorter.insert(&key, data)?;
            }
        }
    }

    Ok(())
}

/// Computes the current prefix based on the previous and the currently iterated value
/// i.e. w1, w2, prox. It also makes sure to follow the `max_prefix_length` setting.
///
/// Uses the current prefixes values to insert the associated data i.e. RoaringBitmap,
/// into the sorter that will, later, be inserted in the LMDB database.
fn insert_current_prefix_data_in_sorter<'a>(
    buffer: &mut Vec<u8>,
    current_prefixes: &mut Option<&'a &'a [String]>,
    prefixes_cache: &mut HashMap<Vec<u8>, Vec<Vec<u8>>>,
    word_prefix_pair_proximity_docids_sorter: &mut grenad::Sorter<MergeFn>,
    prefix_fst_keys: &'a [&'a [std::string::String]],
    max_prefix_length: usize,
    w1: &str,
    w2: &str,
    prox: u8,
    data: &[u8],
) -> Result<()> {
    *current_prefixes = match current_prefixes.take() {
        Some(prefixes) if w2.starts_with(&prefixes[0]) => Some(prefixes),
        _otherwise => {
            write_prefixes_in_sorter(prefixes_cache, word_prefix_pair_proximity_docids_sorter)?;
            prefix_fst_keys.iter().find(|prefixes| w2.starts_with(&prefixes[0]))
        }
    };

    if let Some(prefixes) = current_prefixes {
        buffer.clear();
        buffer.extend_from_slice(w1.as_bytes());
        buffer.push(0);
        for prefix in prefixes.iter() {
            if prefix.len() <= max_prefix_length && w2.starts_with(prefix) {
                buffer.truncate(w1.len() + 1);
                buffer.extend_from_slice(prefix.as_bytes());
                buffer.push(prox);

                match prefixes_cache.get_mut(buffer.as_slice()) {
                    Some(value) => value.push(data.to_owned()),
                    None => {
                        prefixes_cache.insert(buffer.clone(), vec![data.to_owned()]);
                    }
                }
            }
        }
    }

    Ok(())
}
