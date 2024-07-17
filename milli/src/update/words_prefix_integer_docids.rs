use std::collections::{HashMap, HashSet};
use std::str;

use grenad::CompressionType;
use heed::types::Bytes;
use heed::{BytesDecode, BytesEncode, Database};
use tracing::debug;

use crate::error::SerializationError;
use crate::heed_codec::StrBEU16Codec;
use crate::index::main_key::WORDS_PREFIXES_FST_KEY;
use crate::update::del_add::{deladd_serialize_add_side, DelAdd, KvWriterDelAdd};
use crate::update::index_documents::{
    create_sorter, merge_deladd_cbo_roaring_bitmaps,
    merge_deladd_cbo_roaring_bitmaps_into_cbo_roaring_bitmap, valid_lmdb_key,
    write_sorter_into_database, CursorClonableMmap, MergeFn, REDIS_CLIENT,
};
use crate::{CboRoaringBitmapCodec, Result};

pub struct WordPrefixIntegerDocids<'t, 'i> {
    wtxn: &'t mut heed::RwTxn<'i>,
    prefix_database: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    word_database: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) max_nb_chunks: Option<usize>,
    pub(crate) max_memory: Option<usize>,
}

impl<'t, 'i> WordPrefixIntegerDocids<'t, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i>,
        prefix_database: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
        word_database: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    ) -> WordPrefixIntegerDocids<'t, 'i> {
        WordPrefixIntegerDocids {
            wtxn,
            prefix_database,
            word_database,
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
        name = "words_prefix_integer_docids"
    )]
    pub fn execute(
        self,
        new_word_integer_docids: grenad::Merger<CursorClonableMmap, MergeFn>,
        new_prefix_fst_words: &[String],
        common_prefix_fst_words: &[&[String]],
        del_prefix_fst_words: &HashSet<Vec<u8>>,
    ) -> Result<()> {
        debug!("Computing and writing the word levels integers docids into LMDB on disk...");

        let mut conn = REDIS_CLIENT.get_connection().unwrap();

        let mut prefix_integer_docids_sorter = create_sorter(
            grenad::SortAlgorithm::Unstable,
            merge_deladd_cbo_roaring_bitmaps,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.max_nb_chunks,
            self.max_memory,
        );

        if !common_prefix_fst_words.is_empty() {
            // We fetch all the new common prefixes between the previous and new prefix fst.
            let mut buffer = Vec::new();
            let mut current_prefixes: Option<&&[String]> = None;
            let mut prefixes_cache = HashMap::new();
            let mut new_word_integer_docids_iter =
                new_word_integer_docids.into_stream_merger_iter()?;
            while let Some((key, data)) = new_word_integer_docids_iter.next()? {
                let (word, pos) =
                    StrBEU16Codec::bytes_decode(key).map_err(heed::Error::Decoding)?;

                current_prefixes = match current_prefixes.take() {
                    Some(prefixes) if word.starts_with(&prefixes[0]) => Some(prefixes),
                    _otherwise => {
                        write_prefixes_in_sorter(
                            &mut prefixes_cache,
                            &mut prefix_integer_docids_sorter,
                            &mut conn,
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
                            buffer.push(0);
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

            write_prefixes_in_sorter(
                &mut prefixes_cache,
                &mut prefix_integer_docids_sorter,
                &mut conn,
            )?;
        }

        // We fetch the docids associated to the newly added word prefix fst only.
        let db = self.word_database.remap_data_type::<Bytes>();
        let mut buffer = Vec::new();
        for prefix_bytes in new_prefix_fst_words {
            let prefix = str::from_utf8(prefix_bytes.as_bytes()).map_err(|_| {
                SerializationError::Decoding { db_name: Some(WORDS_PREFIXES_FST_KEY) }
            })?;

            // iter over all lines of the DB where the key is prefixed by the current prefix.
            let iter = db
                .remap_key_type::<Bytes>()
                .prefix_iter(self.wtxn, prefix_bytes.as_bytes())?
                .remap_key_type::<StrBEU16Codec>();
            for result in iter {
                let ((word, pos), data) = result?;
                if word.starts_with(prefix) {
                    let key = (prefix, pos);
                    let bytes = StrBEU16Codec::bytes_encode(&key).unwrap();

                    buffer.clear();
                    let mut writer = KvWriterDelAdd::new(&mut buffer);
                    writer.insert(DelAdd::Addition, data)?;
                    redis::cmd("INCR").arg(bytes.as_ref()).query::<usize>(&mut conn).unwrap();
                    prefix_integer_docids_sorter.insert(bytes, writer.into_inner()?)?;
                }
            }
        }

        // We remove all the entries that are no more required in this word prefix integer
        // docids database.
        // We also avoid iterating over the whole `word_prefix_integer_docids` database if we know in
        // advance that the `if del_prefix_fst_words.contains(prefix.as_bytes()) {` condition below
        // will always be false (i.e. if `del_prefix_fst_words` is empty).
        if !del_prefix_fst_words.is_empty() {
            let mut iter = self.prefix_database.iter_mut(self.wtxn)?.lazily_decode_data();
            while let Some(((prefix, _), _)) = iter.next().transpose()? {
                if del_prefix_fst_words.contains(prefix.as_bytes()) {
                    unsafe { iter.del_current()? };
                }
            }
            drop(iter);
        }

        let database_is_empty = self.prefix_database.is_empty(self.wtxn)?;

        // We finally write all the word prefix integer docids into the LMDB database.
        write_sorter_into_database(
            prefix_integer_docids_sorter,
            &self.prefix_database,
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
    // TODO: Merge before insertion.
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
