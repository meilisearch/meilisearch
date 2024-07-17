use std::collections::{HashMap, HashSet};

use grenad::CompressionType;
use heed::types::{Bytes, Str};
use heed::Database;

use super::index_documents::REDIS_CLIENT;
use crate::update::del_add::{deladd_serialize_add_side, DelAdd, KvWriterDelAdd};
use crate::update::index_documents::{
    create_sorter, merge_deladd_cbo_roaring_bitmaps,
    merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap, valid_lmdb_key,
    write_sorter_into_database, CursorClonableMmap, MergeFn,
};
use crate::{CboRoaringBitmapCodec, Result};

pub struct WordPrefixDocids<'t, 'i> {
    wtxn: &'t mut heed::RwTxn<'i>,
    word_docids: Database<Str, CboRoaringBitmapCodec>,
    word_prefix_docids: Database<Str, CboRoaringBitmapCodec>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
}

impl<'t, 'i> WordPrefixDocids<'t, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i>,
        word_docids: Database<Str, CboRoaringBitmapCodec>,
        word_prefix_docids: Database<Str, CboRoaringBitmapCodec>,
    ) -> WordPrefixDocids<'t, 'i> {
        WordPrefixDocids {
            wtxn,
            word_docids,
            word_prefix_docids,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            max_nb_chunks: None,
            max_memory: None,
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        target = "indexing::prefix",
        name = "word_prefix_docids"
    )]
    pub fn execute(
        self,
        new_word_docids: grenad::Merger<CursorClonableMmap, MergeFn>,
        new_prefix_fst_words: &[String],
        common_prefix_fst_words: &[&[String]],
        del_prefix_fst_words: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        let mut conn = REDIS_CLIENT.get_connection().unwrap();

        // It is forbidden to keep a mutable reference into the database
        // and write into it at the same time, therefore we write into another file.
        let mut prefix_docids_sorter = create_sorter(
            grenad::SortAlgorithm::Unstable,
            merge_deladd_cbo_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        if !common_prefix_fst_words.is_empty() {
            let mut current_prefixes: Option<&&[String]> = None;
            let mut prefixes_cache = HashMap::new();
            let mut new_word_docids_iter = new_word_docids.into_stream_merger_iter()?;
            while let Some((word, data)) = new_word_docids_iter.next()? {
                current_prefixes = match current_prefixes.take() {
                    Some(prefixes) if word.starts_with(prefixes[0].as_bytes()) => Some(prefixes),
                    _otherwise => {
                        write_prefixes_in_sorter(
                            &mut prefixes_cache,
                            &mut prefix_docids_sorter,
                            &mut conn,
                        )?;
                        common_prefix_fst_words
                            .iter()
                            .find(|prefixes| word.starts_with(prefixes[0].as_bytes()))
                    }
                };

                if let Some(prefixes) = current_prefixes {
                    for prefix in prefixes.iter() {
                        if word.starts_with(prefix.as_bytes()) {
                            match prefixes_cache.get_mut(prefix.as_bytes()) {
                                Some(value) => value.push(data.to_owned()),
                                None => {
                                    prefixes_cache
                                        .insert(prefix.clone().into(), vec![data.to_owned()]);
                                }
                            }
                        }
                    }
                }
            }

            write_prefixes_in_sorter(&mut prefixes_cache, &mut prefix_docids_sorter, &mut conn)?;
        }

        // We fetch the docids associated to the newly added word prefix fst only.
        let db = self.word_docids.remap_data_type::<Bytes>();
        let mut buffer = Vec::new();
        for prefix in new_prefix_fst_words {
            let prefix = std::str::from_utf8(prefix.as_bytes())?;
            for result in db.prefix_iter(self.wtxn, prefix)? {
                let (_word, data) = result?;
                buffer.clear();
                let mut writer = KvWriterDelAdd::new(&mut buffer);
                writer.insert(DelAdd::Addition, data)?;

                redis::cmd("INCR").arg(prefix.as_bytes()).query::<usize>(&mut conn).unwrap();
                prefix_docids_sorter.insert(prefix, writer.into_inner()?)?;
            }
        }

        // We remove all the entries that are no more required in this word prefix docids database.
        let mut iter = self.word_prefix_docids.iter_mut(self.wtxn)?.lazily_decode_data();
        while let Some((prefix, _)) = iter.next().transpose()? {
            if del_prefix_fst_words.contains(prefix.as_bytes()) {
                unsafe { iter.del_current()? };
            }
        }

        drop(iter);

        let database_is_empty = self.word_prefix_docids.is_empty(self.wtxn)?;

        // We finally write the word prefix docids into the LMDB database.
        write_sorter_into_database(
            prefix_docids_sorter,
            &self.word_prefix_docids,
            self.wtxn,
            database_is_empty,
            deladd_serialize_add_side,
            merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap,
        )?;

        Ok(())
    }
}

fn write_prefixes_in_sorter(
    prefixes: &mut HashMap<Vec<u8>, Vec<Vec<u8>>>,
    sorter: &mut grenad::Sorter<MergeFn>,
    conn: &mut redis::Connection,
) -> Result<()> {
    for (key, data_slices) in prefixes.drain() {
        for data in data_slices {
            if valid_lmdb_key(&key) {
                redis::cmd("INCR").arg(key.as_slice()).query::<usize>(conn).unwrap();
                sorter.insert(&key, data)?;
            }
        }
    }

    Ok(())
}
