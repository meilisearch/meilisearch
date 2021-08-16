use std::str;

use fst::Streamer;
use grenad::CompressionType;
use heed::types::ByteSlice;

use crate::update::index_documents::{
    create_sorter, merge_roaring_bitmaps, sorter_into_lmdb_database, WriteMethod,
};
use crate::{Index, Result};

pub struct WordPrefixDocids<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
}

impl<'t, 'u, 'i> WordPrefixDocids<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
    ) -> WordPrefixDocids<'t, 'u, 'i> {
        WordPrefixDocids {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_nb_chunks: None,
            max_memory: None,
        }
    }

    pub fn execute(self) -> Result<()> {
        // Clear the word prefix docids database.
        self.index.word_prefix_docids.clear(self.wtxn)?;

        let prefix_fst = self.index.words_prefixes_fst(self.wtxn)?;

        // It is forbidden to keep a mutable reference into the database
        // and write into it at the same time, therefore we write into another file.
        let mut prefix_docids_sorter = create_sorter(
            merge_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        // We iterate over all the prefixes and retrieve the corresponding docids.
        let mut prefix_stream = prefix_fst.stream();
        while let Some(bytes) = prefix_stream.next() {
            let prefix = str::from_utf8(bytes)?;
            let db = self.index.word_docids.remap_data_type::<ByteSlice>();
            for result in db.prefix_iter(self.wtxn, prefix)? {
                let (_word, data) = result?;
                prefix_docids_sorter.insert(prefix, data)?;
            }
        }

        drop(prefix_fst);

        // We finally write the word prefix docids into the LMDB database.
        sorter_into_lmdb_database(
            self.wtxn,
            *self.index.word_prefix_docids.as_polymorph(),
            prefix_docids_sorter,
            merge_roaring_bitmaps,
            WriteMethod::Append,
        )?;

        Ok(())
    }
}
