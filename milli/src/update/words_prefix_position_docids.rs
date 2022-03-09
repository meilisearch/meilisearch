use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::{cmp, str};

use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::{BytesDecode, BytesEncode};
use log::debug;

use crate::error::SerializationError;
use crate::heed_codec::StrBEU32Codec;
use crate::index::main_key::WORDS_PREFIXES_FST_KEY;
use crate::update::index_documents::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_lmdb_database, CursorClonableMmap,
    MergeFn,
};
use crate::{Index, Result};

pub struct WordPrefixPositionDocids<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
    level_group_size: NonZeroU32,
    min_level_size: NonZeroU32,
}

impl<'t, 'u, 'i> WordPrefixPositionDocids<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> WordPrefixPositionDocids<'t, 'u, 'i> {
        WordPrefixPositionDocids {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_nb_chunks: None,
            max_memory: None,
            level_group_size: NonZeroU32::new(4).unwrap(),
            min_level_size: NonZeroU32::new(5).unwrap(),
        }
    }

    pub fn level_group_size(&mut self, value: NonZeroU32) -> &mut Self {
        self.level_group_size = NonZeroU32::new(cmp::max(value.get(), 2)).unwrap();
        self
    }

    pub fn min_level_size(&mut self, value: NonZeroU32) -> &mut Self {
        self.min_level_size = value;
        self
    }

    #[logging_timer::time("WordPrefixPositionDocids::{}")]
    pub fn execute(
        self,
        new_word_position_docids: grenad::Reader<CursorClonableMmap>,
        new_prefix_fst_words: &[String],
        common_prefix_fst_words: &[&[String]],
        del_prefix_fst_words: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        debug!("Computing and writing the word levels positions docids into LMDB on disk...");

        let mut prefix_position_docids_sorter = create_sorter(
            merge_cbo_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        let mut new_word_position_docids_iter = new_word_position_docids.into_cursor()?;

        if !common_prefix_fst_words.is_empty() {
            // We fetch all the new common prefixes between the previous and new prefix fst.
            let mut buffer = Vec::new();
            let mut current_prefixes: Option<&&[String]> = None;
            let mut prefixes_cache = HashMap::new();
            while let Some((key, data)) = new_word_position_docids_iter.move_on_next()? {
                let (word, pos) = StrBEU32Codec::bytes_decode(key).ok_or(heed::Error::Decoding)?;

                current_prefixes = match current_prefixes.take() {
                    Some(prefixes) if word.starts_with(&prefixes[0]) => Some(prefixes),
                    _otherwise => {
                        write_prefixes_in_sorter(
                            &mut prefixes_cache,
                            &mut prefix_position_docids_sorter,
                        )?;
                        common_prefix_fst_words
                            .iter()
                            .find(|prefixes| word.starts_with(&prefixes[0]))
                    }
                };

                if let Some(prefixes) = current_prefixes {
                    for prefix in prefixes.iter() {
                        if word.starts_with(prefix) {
                            buffer.clear();
                            buffer.extend_from_slice(prefix.as_bytes());
                            buffer.extend_from_slice(&pos.to_be_bytes());
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

            write_prefixes_in_sorter(&mut prefixes_cache, &mut prefix_position_docids_sorter)?;
        }

        // We fetch the docids associated to the newly added word prefix fst only.
        let db = self.index.word_position_docids.remap_data_type::<ByteSlice>();
        for prefix_bytes in new_prefix_fst_words {
            let prefix = str::from_utf8(prefix_bytes.as_bytes()).map_err(|_| {
                SerializationError::Decoding { db_name: Some(WORDS_PREFIXES_FST_KEY) }
            })?;

            // iter over all lines of the DB where the key is prefixed by the current prefix.
            let iter = db
                .remap_key_type::<ByteSlice>()
                .prefix_iter(self.wtxn, prefix_bytes.as_bytes())?
                .remap_key_type::<StrBEU32Codec>();
            for result in iter {
                let ((word, pos), data) = result?;
                if word.starts_with(prefix) {
                    let key = (prefix, pos);
                    let bytes = StrBEU32Codec::bytes_encode(&key).unwrap();
                    prefix_position_docids_sorter.insert(bytes, data)?;
                }
            }
        }

        // We remove all the entries that are no more required in this word prefix position
        // docids database.
        let mut iter =
            self.index.word_prefix_position_docids.iter_mut(self.wtxn)?.lazily_decode_data();
        while let Some(((prefix, _), _)) = iter.next().transpose()? {
            if del_prefix_fst_words.contains(prefix.as_bytes()) {
                unsafe { iter.del_current()? };
            }
        }

        drop(iter);

        // We finally write all the word prefix position docids into the LMDB database.
        sorter_into_lmdb_database(
            self.wtxn,
            *self.index.word_prefix_position_docids.as_polymorph(),
            prefix_position_docids_sorter,
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
            sorter.insert(&key, data)?;
        }
    }

    Ok(())
}
