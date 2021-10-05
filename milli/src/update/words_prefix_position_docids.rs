use std::num::NonZeroU32;
use std::{cmp, str};

use fst::Streamer;
use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::BytesEncode;
use log::debug;

use crate::error::SerializationError;
use crate::heed_codec::StrBEU32Codec;
use crate::index::main_key::WORDS_PREFIXES_FST_KEY;
use crate::update::index_documents::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_lmdb_database, WriteMethod,
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
    pub fn execute(self) -> Result<()> {
        debug!("Computing and writing the word levels positions docids into LMDB on disk...");

        self.index.word_prefix_position_docids.clear(self.wtxn)?;

        let mut word_prefix_positions_docids_sorter = create_sorter(
            merge_cbo_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        // We insert the word prefix position and
        // corresponds to the word-prefix position where the prefixes appears
        // in the prefix FST previously constructed.
        let prefix_fst = self.index.words_prefixes_fst(self.wtxn)?;
        let db = self.index.word_position_docids.remap_data_type::<ByteSlice>();
        // iter over all prefixes in the prefix fst.
        let mut word_stream = prefix_fst.stream();
        while let Some(prefix_bytes) = word_stream.next() {
            let prefix = str::from_utf8(prefix_bytes).map_err(|_| {
                SerializationError::Decoding { db_name: Some(WORDS_PREFIXES_FST_KEY) }
            })?;

            // iter over all lines of the DB where the key is prefixed by the current prefix.
            let mut iter = db
                .remap_key_type::<ByteSlice>()
                .prefix_iter(self.wtxn, &prefix_bytes)?
                .remap_key_type::<StrBEU32Codec>();
            while let Some(((_word, pos), data)) = iter.next().transpose()? {
                let key = (prefix, pos);
                let bytes = StrBEU32Codec::bytes_encode(&key).unwrap();
                word_prefix_positions_docids_sorter.insert(bytes, data)?;
            }
        }

        // We finally write all the word prefix position docids into the LMDB database.
        sorter_into_lmdb_database(
            self.wtxn,
            *self.index.word_prefix_position_docids.as_polymorph(),
            word_prefix_positions_docids_sorter,
            merge_cbo_roaring_bitmaps,
            WriteMethod::Append,
        )?;

        Ok(())
    }
}
