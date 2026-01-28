use std::collections::BTreeSet;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::iter;

use hashbrown::HashMap;
use heed::types::{Bytes, DecodeIgnore, Str};
use heed::{Database, RwTxn};
use rayon::iter::{IndexedParallelIterator as _, IntoParallelIterator, ParallelIterator as _};
use roaring::MultiOps;
use tempfile::spooled_tempfile;

use crate::heed_codec::StrBEU16Codec;
use crate::update::GrenadParameters;
use crate::{CboRoaringBitmapCodec, Index, Prefix, Result};

struct WordPrefixDocids<'i> {
    index: &'i Index,
    database: Database<Bytes, CboRoaringBitmapCodec>,
    prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
    max_memory_by_thread: Option<usize>,
}

impl<'i> WordPrefixDocids<'i> {
    fn new(
        index: &'i Index,
        database: Database<Bytes, CboRoaringBitmapCodec>,
        prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
        grenad_parameters: &GrenadParameters,
    ) -> WordPrefixDocids<'i> {
        WordPrefixDocids {
            index,
            database,
            prefix_database,
            max_memory_by_thread: grenad_parameters.max_memory_by_thread(),
        }
    }

    fn execute(
        self,
        wtxn: &mut heed::RwTxn,
        prefix_to_compute: &BTreeSet<Prefix>,
        prefix_to_delete: &BTreeSet<Prefix>,
    ) -> Result<()> {
        delete_prefixes(wtxn, &self.prefix_database, prefix_to_delete)?;
        self.recompute_modified_prefixes_no_frozen(wtxn, prefix_to_compute)
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
    fn recompute_modified_prefixes_no_frozen(
        &self,
        wtxn: &mut RwTxn,
        prefix_to_compute: &BTreeSet<Prefix>,
    ) -> Result<()> {
        let thread_count = rayon::current_num_threads();
        let rtxns = iter::repeat_with(|| self.index.env.nested_read_txn(wtxn))
            .take(thread_count)
            .collect::<heed::Result<Vec<_>>>()?;

        let outputs = rtxns
            .into_par_iter()
            .enumerate()
            .map(|(thread_id, rtxn)| {
                // `indexes` represent offsets at which prefixes computations were stored in the `file`.
                let mut indexes = Vec::new();
                let mut file = BufWriter::new(spooled_tempfile(
                    self.max_memory_by_thread.unwrap_or(usize::MAX),
                ));

                let mut buffer = Vec::new();
                for (prefix_index, prefix) in prefix_to_compute.iter().enumerate() {
                    // Is prefix for another thread?
                    if prefix_index % thread_count != thread_id {
                        continue;
                    }

                    let output = self
                        .database
                        .prefix_iter(&rtxn, prefix.as_bytes())?
                        .remap_types::<Str, CboRoaringBitmapCodec>()
                        .map(|result| result.map(|(_word, bitmap)| bitmap))
                        .union()?;

                    buffer.clear();
                    CboRoaringBitmapCodec::serialize_into_vec(&output, &mut buffer);
                    indexes.push(PrefixEntry { prefix, serialized_length: buffer.len() });
                    file.write_all(&buffer)?;
                }

                Ok((indexes, file))
            })
            .collect::<Result<Vec<_>>>()?;

        // We iterate over all the collected and serialized bitmaps through
        // the files and entries to eventually put them in the final database.
        let mut buffer = Vec::new();
        for (index, file) in outputs {
            let mut file = file.into_inner().map_err(|e| e.into_error())?;
            file.rewind()?;
            let mut file = BufReader::new(file);
            for PrefixEntry { prefix, serialized_length } in index {
                buffer.resize(serialized_length, 0);
                file.read_exact(&mut buffer)?;
                self.prefix_database.remap_data_type::<Bytes>().put(
                    wtxn,
                    prefix.as_bytes(),
                    &buffer,
                )?;
            }
        }

        Ok(())
    }
}

/// Represents a prefix and the lenght the bitmap takes on disk.
struct PrefixEntry<'a> {
    prefix: &'a str,
    serialized_length: usize,
}

struct WordPrefixIntegerDocids<'i> {
    index: &'i Index,
    database: Database<Bytes, CboRoaringBitmapCodec>,
    prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
    max_memory_by_thread: Option<usize>,
}

impl<'i> WordPrefixIntegerDocids<'i> {
    fn new(
        index: &'i Index,
        database: Database<Bytes, CboRoaringBitmapCodec>,
        prefix_database: Database<Bytes, CboRoaringBitmapCodec>,
        grenad_parameters: &'_ GrenadParameters,
    ) -> WordPrefixIntegerDocids<'i> {
        WordPrefixIntegerDocids {
            index,
            database,
            prefix_database,
            max_memory_by_thread: grenad_parameters.max_memory_by_thread(),
        }
    }

    fn execute(
        self,
        wtxn: &mut heed::RwTxn,
        prefix_to_compute: &BTreeSet<Prefix>,
        prefix_to_delete: &BTreeSet<Prefix>,
    ) -> Result<()> {
        delete_prefixes(wtxn, &self.prefix_database, prefix_to_delete)?;
        self.recompute_modified_prefixes_no_frozen(wtxn, prefix_to_compute)
    }

    /// Computes the same as `recompute_modified_prefixes`.
    ///
    /// ...but without aggregating the prefixes mmap pointers into a static HashMap
    /// beforehand and rather use an experimental LMDB feature to read the subset
    /// of prefixes in parallel from the uncommitted transaction.
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
    fn recompute_modified_prefixes_no_frozen(
        &self,
        wtxn: &mut RwTxn,
        prefixes: &BTreeSet<Prefix>,
    ) -> Result<()> {
        let thread_count = rayon::current_num_threads();
        let rtxns = iter::repeat_with(|| self.index.env.nested_read_txn(wtxn))
            .take(thread_count)
            .collect::<heed::Result<Vec<_>>>()?;

        let outputs = rtxns
            .into_par_iter()
            .enumerate()
            .map(|(thread_id, rtxn)| {
                // `indexes` represent offsets at which prefixes computations were stored in the `file`.
                let mut indexes = Vec::new();
                let mut file = BufWriter::new(spooled_tempfile(
                    self.max_memory_by_thread.unwrap_or(usize::MAX),
                ));

                let mut buffer = Vec::new();
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
                            indexes.push(PrefixIntegerEntry {
                                prefix,
                                pos,
                                serialized_length: None,
                            });
                        } else {
                            let output = bitmaps_bytes
                                .into_iter()
                                .map(CboRoaringBitmapCodec::deserialize_from)
                                .union()?;
                            buffer.clear();
                            CboRoaringBitmapCodec::serialize_into_vec(&output, &mut buffer);
                            indexes.push(PrefixIntegerEntry {
                                prefix,
                                pos,
                                serialized_length: Some(buffer.len()),
                            });
                            file.write_all(&buffer)?;
                        }
                    }
                }

                Ok((indexes, file))
            })
            .collect::<Result<Vec<_>>>()?;

        // We iterate over all the collected and serialized bitmaps through
        // the files and entries to eventually put them in the final database.
        let mut key_buffer = Vec::new();
        let mut buffer = Vec::new();
        for (index, file) in outputs {
            let mut file = file.into_inner().map_err(|e| e.into_error())?;
            file.rewind()?;
            let mut file = BufReader::new(file);
            for PrefixIntegerEntry { prefix, pos, serialized_length } in index {
                key_buffer.clear();
                key_buffer.extend_from_slice(prefix.as_bytes());
                key_buffer.push(0);
                key_buffer.extend_from_slice(&pos.to_be_bytes());
                match serialized_length {
                    Some(serialized_length) => {
                        buffer.resize(serialized_length, 0);
                        file.read_exact(&mut buffer)?;
                        self.prefix_database.remap_data_type::<Bytes>().put(
                            wtxn,
                            &key_buffer,
                            &buffer,
                        )?;
                    }
                    None => {
                        self.prefix_database.delete(wtxn, &key_buffer)?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Represents a prefix and the length the bitmap takes on disk.
struct PrefixIntegerEntry<'a> {
    prefix: &'a str,
    pos: u16,
    serialized_length: Option<usize>,
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
fn delete_prefixes(
    wtxn: &mut RwTxn,
    prefix_database: &Database<Bytes, CboRoaringBitmapCodec>,
    prefixes: &BTreeSet<Prefix>,
) -> Result<()> {
    // We remove all the entries that are no more required in this word prefix docids database.
    for prefix in prefixes {
        let mut iter = prefix_database.prefix_iter_mut(wtxn, prefix.as_bytes())?;
        while iter.next().transpose()?.is_some() {
            // safety: we do not keep a reference on database entries.
            unsafe { iter.del_current()? };
        }
    }

    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
pub fn compute_word_prefix_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<Prefix>,
    prefix_to_delete: &BTreeSet<Prefix>,
    grenad_parameters: &GrenadParameters,
) -> Result<()> {
    WordPrefixDocids::new(
        index,
        index.word_docids.remap_key_type(),
        index.word_prefix_docids.remap_key_type(),
        grenad_parameters,
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
pub fn compute_exact_word_prefix_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<Prefix>,
    prefix_to_delete: &BTreeSet<Prefix>,
    grenad_parameters: &GrenadParameters,
) -> Result<()> {
    WordPrefixDocids::new(
        index,
        index.exact_word_docids.remap_key_type(),
        index.exact_word_prefix_docids.remap_key_type(),
        grenad_parameters,
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
pub fn compute_word_prefix_fid_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<Prefix>,
    prefix_to_delete: &BTreeSet<Prefix>,
    grenad_parameters: &GrenadParameters,
) -> Result<()> {
    WordPrefixIntegerDocids::new(
        index,
        index.word_fid_docids.remap_key_type(),
        index.word_prefix_fid_docids.remap_key_type(),
        grenad_parameters,
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
pub fn compute_word_prefix_position_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &BTreeSet<Prefix>,
    prefix_to_delete: &BTreeSet<Prefix>,
    grenad_parameters: &GrenadParameters,
) -> Result<()> {
    WordPrefixIntegerDocids::new(
        index,
        index.word_position_docids.remap_key_type(),
        index.word_prefix_position_docids.remap_key_type(),
        grenad_parameters,
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}
