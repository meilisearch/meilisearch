use std::str;

use fst::automaton::{Automaton, Str};
use fst::{IntoStreamer, Streamer};
use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::BytesEncode;
use log::debug;

use crate::heed_codec::StrStrU8Codec;
use crate::update::index_documents::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_lmdb_database, WriteMethod,
};
use crate::{Index, Result};

pub struct WordPrefixPairProximityDocids<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
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
        }
    }

    pub fn execute(self) -> Result<()> {
        debug!("Computing and writing the word prefix pair proximity docids into LMDB on disk...");

        self.index.word_prefix_pair_proximity_docids.clear(self.wtxn)?;

        let prefix_fst = self.index.words_prefixes_fst(self.wtxn)?;

        // Here we create a sorter akin to the previous one.
        let mut word_prefix_pair_proximity_docids_sorter = create_sorter(
            merge_cbo_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        // We insert all the word pairs corresponding to the word-prefix pairs
        // where the prefixes appears in the prefix FST previously constructed.
        let db = self.index.word_pair_proximity_docids.remap_data_type::<ByteSlice>();
        for result in db.iter(self.wtxn)? {
            let ((word1, word2, prox), data) = result?;
            let automaton = Str::new(word2).starts_with();
            let mut matching_prefixes = prefix_fst.search(automaton).into_stream();
            while let Some(prefix) = matching_prefixes.next() {
                let prefix = str::from_utf8(prefix)?;
                let pair = (word1, prefix, prox);
                let bytes = StrStrU8Codec::bytes_encode(&pair).unwrap();
                word_prefix_pair_proximity_docids_sorter.insert(bytes, data)?;
            }
        }

        drop(prefix_fst);

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
