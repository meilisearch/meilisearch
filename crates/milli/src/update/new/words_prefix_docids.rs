use std::collections::BTreeSet;
use std::io::{BufReader, BufWriter, Read as _, Seek as _, Write};
use std::iter;
use std::num::NonZeroU32;

use hashbrown::HashMap;
use heed::types::{Bytes, DecodeIgnore};
use heed::{Database, RwTxn};
use rayon::iter::{IndexedParallelIterator as _, IntoParallelIterator, ParallelIterator as _};
use roaring::MultiOps;

use crate::heed_codec::StrBEU16Codec;
use crate::update::new::indexer::MiniString;
use crate::{CboRoaringBitmapCodec, Index, Result};

struct WordPrefixDocids {
    database: Database<Bytes, CboRoaringBitmapCodec>,
    prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
}

impl WordPrefixDocids {
    fn new(
        database: Database<Bytes, CboRoaringBitmapCodec>,
        prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
    ) -> WordPrefixDocids {
        WordPrefixDocids { database, prefix_database }
    }

    fn execute(
        self,
        wtxn: &mut heed::RwTxn,
        prefix_to_compute: &BTreeSet<MiniString>,
        prefix_to_delete: &BTreeSet<MiniString>,
    ) -> Result<()> {
        delete_prefixes(wtxn, &self.prefix_database, prefix_to_delete)?;
        self.recompute_modified_prefixes_no_frozen(wtxn, prefix_to_compute)
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing::prefix")]
    fn recompute_modified_prefixes_no_frozen(
        &self,
        wtxn: &mut RwTxn,
        prefix_to_compute: &BTreeSet<MiniString>,
    ) -> Result<()> {
        let thread_count = rayon::current_num_threads();
        let rtxns = iter::repeat_with(|| wtxn.nested_read_txn())
            .take(thread_count)
            .collect::<heed::Result<Vec<_>>>()?;

        let outputs = rtxns
            .into_par_iter()
            .enumerate()
            .map(|(thread_id, rtxn)| {
                // Represents the offsets at which prefixes computations were stored in the `values` file.
                let mut entries = Vec::new();
                let mut values = tempfile::tempfile().map(BufWriter::new)?;

                let mut tmp_buffer = Vec::new();
                for (prefix_index, prefix) in prefix_to_compute.iter().enumerate() {
                    // Is prefix for another thread?
                    if prefix_index % thread_count != thread_id {
                        continue;
                    }

                    let output = self
                        .database
                        .prefix_iter(&rtxn, prefix.as_bytes())?
                        .map(|result| result.map(|(_word, bitmap)| bitmap))
                        .union()?;

                    tmp_buffer.clear();
                    CboRoaringBitmapCodec::serialize_into_vec(&output, &mut tmp_buffer);
                    // safety: the serialized length will never exceed u32::MAX (4GiB).
                    let serialized_length = tmp_buffer.len().try_into().unwrap();
                    entries.push(PrefixEntry {
                        prefix: prefix.clone(),
                        serialized_length: NonZeroU32::new(serialized_length),
                    });
                    values.write_all(&tmp_buffer)?;
                }

                Ok((entries, values))
            })
            .collect::<Result<Vec<_>>>()?;

        // We iterate over all the collected and serialized bitmaps
        // to eventually put them in the final database.
        let mut tmp_buffer = Vec::new();
        for (index, values) in outputs {
            let mut values = values.into_inner().map_err(|e| e.into_error())?;
            values.rewind()?;
            let mut values = BufReader::new(values);
            for PrefixEntry { prefix, serialized_length } in index {
                match serialized_length {
                    Some(serialized_length) => {
                        tmp_buffer.resize(serialized_length.get() as usize, 0);
                        values.read_exact(&mut tmp_buffer)?;
                        self.prefix_database.remap_data_type::<Bytes>().put(
                            wtxn,
                            prefix.as_bytes(),
                            &tmp_buffer,
                        )?;
                    }
                    None => {
                        self.prefix_database.delete(wtxn, prefix.as_bytes())?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Represents a prefix and the lenght the bitmap takes on disk.
struct PrefixEntry {
    prefix: MiniString,
    // The size of the serialized bitmap in bytes cannot be larger than u32::MAX (4GiB) anyway.
    serialized_length: Option<NonZeroU32>,
}

struct WordPrefixIntegerDocids {
    database: Database<Bytes, CboRoaringBitmapCodec>,
    prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
}

impl WordPrefixIntegerDocids {
    fn new(
        database: Database<Bytes, CboRoaringBitmapCodec>,
        prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
    ) -> WordPrefixIntegerDocids {
        WordPrefixIntegerDocids { database, prefix_database }
    }

    fn execute(
        self,
        wtxn: &mut heed::RwTxn,
        prefix_to_compute: &BTreeSet<MiniString>,
        prefix_to_delete: &BTreeSet<MiniString>,
    ) -> Result<()> {
        delete_prefixes(wtxn, &self.prefix_database, prefix_to_delete)?;
        self.recompute_modified_prefixes_no_frozen(wtxn, prefix_to_compute)
    }

    /// Computes the same as `recompute_modified_prefixes`.
    ///
    /// ...but without aggregating the prefixes mmap pointers into a static HashMap
    /// beforehand and rather use an experimental LMDB feature to read the subset
    /// of prefixes in parallel from the uncommitted transaction.
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing::prefix")]
    fn recompute_modified_prefixes_no_frozen(
        &self,
        wtxn: &mut RwTxn,
        prefixes: &BTreeSet<MiniString>,
    ) -> Result<()> {
        let thread_count = rayon::current_num_threads();
        let rtxns = iter::repeat_with(|| wtxn.nested_read_txn())
            .take(thread_count)
            .collect::<heed::Result<Vec<_>>>()?;

        let outputs = rtxns
            .into_par_iter()
            .enumerate()
            .map(|(thread_id, rtxn)| {
                // Represents the offsets at which prefixes computations were stored in the `values` file.
                let mut entries = Vec::new();
                let mut values = tempfile::tempfile().map(BufWriter::new)?;

                let mut tmp_buffer = Vec::new();
                for (prefix_index, prefix) in prefixes.iter().enumerate() {
                    // Is prefix for another thread?
                    if prefix_index % thread_count != thread_id {
                        continue;
                    }

                    let mut bitmap_bytes_at_positions = HashMap::new();
                    for result in self
                        .database
                        .prefix_iter(&rtxn, prefix.as_bytes())?
                        .remap_types::<StrBEU16Codec, Bytes>()
                    {
                        let ((_word, pos), bitmap_bytes) = result?;
                        bitmap_bytes_at_positions
                            .entry(pos)
                            .or_insert_with(Vec::new)
                            .push(bitmap_bytes);
                    }

                    // We track positions with no corresponding bitmap bytes,
                    // these means that the prefix no longer exists in the database
                    // and must, therefore, be removed from the index.
                    for result in self
                        .prefix_database
                        .prefix_iter(&rtxn, prefix.as_bytes())?
                        .remap_types::<StrBEU16Codec, DecodeIgnore>()
                    {
                        let ((_word, pos), ()) = result?;
                        // They are represented by an empty set of bitmaps.
                        bitmap_bytes_at_positions.entry(pos).or_insert_with(Vec::new);
                    }

                    for (pos, bitmaps_bytes) in bitmap_bytes_at_positions {
                        if bitmaps_bytes.is_empty() {
                            entries.push(PrefixIntegerEntry {
                                prefix: prefix.clone(),
                                pos,
                                serialized_length: None,
                            });
                        } else {
                            let output = bitmaps_bytes
                                .into_iter()
                                .map(CboRoaringBitmapCodec::deserialize_from)
                                .union()?;
                            tmp_buffer.clear();
                            CboRoaringBitmapCodec::serialize_into_vec(&output, &mut tmp_buffer);
                            // safety: the serialized length will never exceed u32::MAX (4GiB).
                            let serialized_length = tmp_buffer.len().try_into().unwrap();
                            entries.push(PrefixIntegerEntry {
                                prefix: prefix.clone(),
                                pos,
                                serialized_length: NonZeroU32::new(serialized_length),
                            });
                            values.write_all(&tmp_buffer)?;
                        }
                    }
                }

                Ok((entries, values))
            })
            .collect::<Result<Vec<_>>>()?;

        // We iterate over all the collected and serialized bitmaps through
        // the files and entries to eventually put them in the final database.
        let mut tmp_key_buffer = Vec::new();
        let mut tmp_buffer = Vec::new();
        for (index, file) in outputs {
            let mut file = file.into_inner().map_err(|e| e.into_error())?;
            file.rewind()?;
            let mut file = BufReader::new(file);
            for PrefixIntegerEntry { prefix, pos, serialized_length } in index {
                tmp_key_buffer.clear();
                tmp_key_buffer.extend_from_slice(prefix.as_bytes());
                tmp_key_buffer.push(0);
                tmp_key_buffer.extend_from_slice(&pos.to_be_bytes());
                match serialized_length {
                    Some(serialized_length) => {
                        tmp_buffer.resize(serialized_length.get() as usize, 0);
                        file.read_exact(&mut tmp_buffer)?;
                        self.prefix_database.remap_data_type::<Bytes>().put(
                            wtxn,
                            &tmp_key_buffer,
                            &tmp_buffer,
                        )?;
                    }
                    None => {
                        self.prefix_database.delete(wtxn, &tmp_key_buffer)?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Represents a prefix, its position in the field and the length the bitmap takes on disk.
struct PrefixIntegerEntry {
    prefix: MiniString,
    pos: u16,
    serialized_length: Option<NonZeroU32>,
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing::prefix")]
fn delete_prefixes(
    wtxn: &mut RwTxn,
    prefix_database: &Database<Bytes, CboRoaringBitmapCodec>,
    prefixes: &BTreeSet<MiniString>,
) -> Result<()> {
    // We remove all the entries that are no more required in this word prefix docids database.
    for prefix in prefixes.iter() {
        let mut iter = prefix_database
            .remap_data_type::<DecodeIgnore>()
            .prefix_iter_mut(wtxn, prefix.as_bytes())?;
        while iter.next().transpose()?.is_some() {
            // safety: we do not keep a reference on database entries.
            unsafe { iter.del_current()? };
        }
    }

    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing::prefix")]
pub fn compute_word_prefix_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<MiniString>,
    prefix_to_delete: &BTreeSet<MiniString>,
) -> Result<()> {
    WordPrefixDocids::new(
        index.word_docids.remap_key_type(),
        index.word_prefix_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing::prefix")]
pub fn compute_exact_word_prefix_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<MiniString>,
    prefix_to_delete: &BTreeSet<MiniString>,
) -> Result<()> {
    WordPrefixDocids::new(
        index.exact_word_docids.remap_key_type(),
        index.exact_word_prefix_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing::prefix")]
pub fn compute_word_prefix_fid_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<MiniString>,
    prefix_to_delete: &BTreeSet<MiniString>,
) -> Result<()> {
    WordPrefixIntegerDocids::new(
        index.word_fid_docids.remap_key_type(),
        index.word_prefix_fid_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing::prefix")]
pub fn compute_word_prefix_position_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<MiniString>,
    prefix_to_delete: &BTreeSet<MiniString>,
) -> Result<()> {
    WordPrefixIntegerDocids::new(
        index.word_position_docids.remap_key_type(),
        index.word_prefix_position_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}
