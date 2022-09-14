use crate::update::index_documents::{create_writer, CursorClonableMmap};
use crate::update::prefix_word_pairs::{
    insert_into_database, write_into_lmdb_database_without_merging,
};
use crate::{CboRoaringBitmapCodec, Result, U8StrStrCodec, UncheckedU8StrStrCodec};
use grenad::CompressionType;
use heed::types::ByteSlice;
use heed::BytesDecode;
use log::debug;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};

#[logging_timer::time]
pub fn index_prefix_word_database(
    wtxn: &mut heed::RwTxn,
    word_pair_proximity_docids: heed::Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    prefix_word_pair_proximity_docids: heed::Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    max_proximity: u8,
    max_prefix_length: usize,
    new_word_pair_proximity_docids: grenad::Reader<CursorClonableMmap>,
    new_prefix_fst_words: &[String],
    common_prefix_fst_words: &[&[String]],
    del_prefix_fst_words: &HashSet<Vec<u8>>,
) -> Result<()> {
    let max_proximity = max_proximity - 1;
    debug!("Computing and writing the word prefix pair proximity docids into LMDB on disk...");

    let common_prefixes: Vec<_> = common_prefix_fst_words
        .into_iter()
        .map(|s| s.into_iter())
        .flatten()
        .map(|s| s.as_str())
        .filter(|s| s.len() <= max_prefix_length)
        .collect();

    // If the prefix trie is not empty, then we can iterate over all new
    // word pairs to look for new (word1, common_prefix, proximity) elements
    // to insert in the DB
    for proximity in 1..=max_proximity - 1 {
        for prefix in common_prefixes.iter() {
            let mut prefix_key = vec![];
            prefix_key.push(proximity);
            prefix_key.extend_from_slice(prefix.as_bytes());
            let mut cursor = new_word_pair_proximity_docids.clone().into_prefix_iter(prefix_key)?;
            // This is the core of the algorithm
            execute_on_word_pairs_and_prefixes(
                proximity,
                prefix.as_bytes(),
                // the next two arguments tell how to iterate over the new word pairs
                &mut cursor,
                |cursor| {
                    if let Some((key, value)) = cursor.next()? {
                        let (_, _, word2) = UncheckedU8StrStrCodec::bytes_decode(key)
                            .ok_or(heed::Error::Decoding)?;
                        Ok(Some((word2, value)))
                    } else {
                        Ok(None)
                    }
                },
                // and this argument tells what to do with each new key (proximity, prefix, word2) and value (roaring bitmap)
                |key, value| {
                    insert_into_database(
                        wtxn,
                        *prefix_word_pair_proximity_docids.as_polymorph(),
                        key,
                        value,
                    )
                },
            )?;
        }
    }

    // Now we do the same thing with the new prefixes and all word pairs in the DB
    let new_prefixes: Vec<_> = new_prefix_fst_words
        .into_iter()
        .map(|s| s.as_str())
        .filter(|s| s.len() <= max_prefix_length)
        .collect();

    // Since we read the DB, we can't write to it directly, so we add each new (word1, prefix, proximity)
    // element in an intermediary grenad
    let mut writer = create_writer(CompressionType::None, None, tempfile::tempfile()?);

    for proximity in 1..=max_proximity - 1 {
        for prefix in new_prefixes.iter() {
            let mut prefix_key = vec![];
            prefix_key.push(proximity);
            prefix_key.extend_from_slice(prefix.as_bytes());
            let mut db_iter = word_pair_proximity_docids
                .as_polymorph()
                .prefix_iter::<_, ByteSlice, ByteSlice>(wtxn, prefix_key.as_slice())?
                .remap_key_type::<UncheckedU8StrStrCodec>();
            execute_on_word_pairs_and_prefixes(
                proximity,
                prefix.as_bytes(),
                &mut db_iter,
                |db_iter| {
                    db_iter
                        .next()
                        .transpose()
                        .map(|x| x.map(|((_, _, word2), value)| (word2, value)))
                        .map_err(|e| e.into())
                },
                |key, value| writer.insert(key, value).map_err(|e| e.into()),
            )?;
            drop(db_iter);
        }
    }

    // and then we write the grenad into the DB
    // Since the grenad contains only new prefixes, we know in advance that none
    // of its elements already exist in the DB, thus there is no need to specify
    // how to merge conflicting elements
    write_into_lmdb_database_without_merging(
        wtxn,
        *prefix_word_pair_proximity_docids.as_polymorph(),
        writer,
    )?;

    // All of the word prefix pairs in the database that have a w2
    // that is contained in the `suppr_pw` set must be removed as well.
    if !del_prefix_fst_words.is_empty() {
        let mut iter =
            prefix_word_pair_proximity_docids.remap_data_type::<ByteSlice>().iter_mut(wtxn)?;
        while let Some(((_, prefix, _), _)) = iter.next().transpose()? {
            if del_prefix_fst_words.contains(prefix.as_bytes()) {
                // Delete this entry as the w2 prefix is no more in the words prefix fst.
                unsafe { iter.del_current()? };
            }
        }
    }

    Ok(())
}

/// This is the core of the algorithm to initialise the Word Prefix Pair Proximity Docids database.
///
/// Its main arguments are:
/// 1. a sorted prefix iterator over ((word1, word2, proximity), docids) elements
/// 2. a closure to describe how to handle the new computed (word1, prefix, proximity) elements
///
/// For more information about what this function does, read the module documentation.
fn execute_on_word_pairs_and_prefixes<I>(
    proximity: u8,
    prefix: &[u8],
    iter: &mut I,
    mut next_word2_and_docids: impl for<'a> FnMut(&'a mut I) -> Result<Option<(&'a [u8], &'a [u8])>>,
    mut insert: impl for<'a> FnMut(&'a [u8], &'a [u8]) -> Result<()>,
) -> Result<()> {
    let mut batch: BTreeMap<Vec<u8>, Vec<Cow<'static, [u8]>>> = <_>::default();

    while let Some((word2, data)) = next_word2_and_docids(iter)? {
        let entry = batch.entry(word2.to_owned()).or_default();
        entry.push(Cow::Owned(data.to_owned()));
    }

    let mut key_buffer = Vec::with_capacity(8);
    key_buffer.push(proximity);
    key_buffer.extend_from_slice(prefix);
    key_buffer.push(0);

    let mut value_buffer = Vec::with_capacity(65_536);

    for (key, values) in batch {
        key_buffer.truncate(prefix.len() + 2);
        value_buffer.clear();

        key_buffer.extend_from_slice(&key);
        let data = if values.len() > 1 {
            CboRoaringBitmapCodec::merge_into(&values, &mut value_buffer)?;
            value_buffer.as_slice()
        } else {
            &values[0]
        };
        insert(key_buffer.as_slice(), data)?;
    }
    Ok(())
}
