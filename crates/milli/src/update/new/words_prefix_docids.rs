use std::collections::BTreeSet;
use std::io::{self, ErrorKind, Write};
use std::iter;

use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use hashbrown::HashMap;
use heed::types::{Bytes, DecodeIgnore};
use heed::{Database, RwTxn};
use rayon::iter::{IndexedParallelIterator as _, IntoParallelIterator, ParallelIterator as _};
use roaring::{MultiOps, RoaringBitmap};

use super::offloader::{Decoder, Encoder, Offloader};
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
                let mut entries =
                    tempfile::tempfile().map(Offloader::<_, OutPrefixEntryCodec>::new)?;

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
                    entries.push(InPrefixEntry { prefix: prefix.as_ref(), bitmap: output })?;
                }

                entries.finish().map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;

        // We iterate over all the collected and serialized bitmaps through
        // the files and entries to eventually put them in the final database.
        for mut entries in outputs {
            while let Some(OutPrefixEntry { key, value }) = entries.next_entry()? {
                // TODO why doesn't it deletes?
                self.prefix_database.remap_data_type::<Bytes>().put_reserved(
                    wtxn,
                    key,
                    value.len(),
                    |space| space.write_all(value),
                )?;
            }
        }

        Ok(())
    }
}

/// Represents a prefix, its position in the field and the length the bitmap takes on disk.
pub struct InPrefixEntry<'a> {
    pub prefix: &'a str,
    pub bitmap: RoaringBitmap,
}

impl Encoder for InPrefixEntry<'_> {
    fn encode<W: io::Write>(self, tmp_buffer: &mut Vec<u8>, writer: &mut W) -> io::Result<()> {
        let InPrefixEntry { prefix, bitmap } = self;

        // prefix length and prefix
        let prefix_length: u8 =
            prefix.len().try_into().map_err(|_| io::Error::other("prefix length too long"))?;
        writer.write_u8(prefix_length)?;
        writer.write_all(prefix.as_bytes())?;

        // bitmap length and bitmap
        let serialized_bytes = {
            tmp_buffer.clear();
            CboRoaringBitmapCodec::serialize_into_vec(&bitmap, tmp_buffer);
            &tmp_buffer[..]
        };
        let serialized_bitmap_length: u32 = serialized_bytes
            .len()
            .try_into()
            .map_err(|_| io::Error::other("serialized bitmap length too long"))?;
        writer.write_u32::<NativeEndian>(serialized_bitmap_length)?;
        writer.write_all(serialized_bytes)?;

        Ok(())
    }
}

pub struct OutPrefixEntryCodec;

/// Represents the key and value of a prefix integer entry.
pub struct OutPrefixEntry<'b> {
    pub key: &'b [u8],
    pub value: &'b [u8],
}

impl<'b> Decoder<'b> for OutPrefixEntryCodec {
    type Decoded = OutPrefixEntry<'b>;

    fn decode<R: io::Read>(
        first_tmp_buffer: &'b mut Vec<u8>,
        second_tmp_buffer: &'b mut Vec<u8>,
        reader: &mut R,
    ) -> io::Result<Option<Self::Decoded>> {
        // prefix length and prefix
        let prefix_length = match reader.read_u8() {
            Ok(prefix_length) => prefix_length,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };
        first_tmp_buffer.resize(prefix_length as usize, 0);
        reader.read_exact(first_tmp_buffer)?;

        // bitmap length and bitmap (bytes)
        let bitmap_length = reader.read_u32::<NativeEndian>()?;
        second_tmp_buffer.resize(bitmap_length as usize, 0);
        reader.read_exact(second_tmp_buffer)?;

        Ok(Some(Self::Decoded {
            key: first_tmp_buffer.as_slice(),
            value: second_tmp_buffer.as_slice(),
        }))
    }
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
                let mut entries =
                    tempfile::tempfile().map(Offloader::<_, OutPrefixIntegerEntryCodec>::new)?;

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
                        let bitmap = if bitmaps_bytes.is_empty() {
                            None
                        } else {
                            let output = bitmaps_bytes
                                .into_iter()
                                .map(CboRoaringBitmapCodec::deserialize_from)
                                .union()?;
                            Some(output)
                        };
                        entries.push(InPrefixIntegerEntry {
                            prefix: prefix.as_str(),
                            pos,
                            bitmap,
                        })?;
                    }
                }

                entries.finish().map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;

        // We iterate over all the collected and serialized bitmaps through
        // the files and entries to eventually put them in the final database.
        for mut entries in outputs {
            while let Some(OutPrefixIntegerEntry { key, value }) = entries.next_entry()? {
                match value {
                    Some(bytes) => {
                        self.prefix_database.remap_data_type::<Bytes>().put_reserved(
                            wtxn,
                            key,
                            bytes.len(),
                            |space| space.write_all(bytes),
                        )?;
                    }
                    None => {
                        self.prefix_database.delete(wtxn, key)?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Represents a prefix, its position in the field and the length the bitmap takes on disk.
pub struct InPrefixIntegerEntry<'a> {
    pub prefix: &'a str,
    pub pos: u16,
    pub bitmap: Option<RoaringBitmap>,
}

impl Encoder for InPrefixIntegerEntry<'_> {
    fn encode<W: io::Write>(self, tmp_buffer: &mut Vec<u8>, writer: &mut W) -> io::Result<()> {
        let InPrefixIntegerEntry { prefix, pos, bitmap } = self;

        // prefix length and prefix
        let prefix_length =
            prefix.len().try_into().map_err(|_| io::Error::other("prefix length too long"))?;
        writer.write_u8(prefix_length)?;
        writer.write_all(prefix.as_bytes())?;

        // pos
        writer.write_u16::<NativeEndian>(pos)?;

        // bitmap length and bitmap
        let serialized_bytes = match bitmap {
            Some(bitmap) => {
                tmp_buffer.clear();
                CboRoaringBitmapCodec::serialize_into_vec(&bitmap, tmp_buffer);
                &tmp_buffer[..]
            }
            None => &[][..],
        };
        let serialized_bitmap_length = serialized_bytes
            .len()
            .try_into()
            .map_err(|_| io::Error::other("serialized bitmap length too long"))?;
        writer.write_u32::<NativeEndian>(serialized_bitmap_length)?;
        writer.write_all(serialized_bytes)?;

        Ok(())
    }
}

pub struct OutPrefixIntegerEntryCodec;

/// Represents the key and value of a prefix integer entry.
pub struct OutPrefixIntegerEntry<'b> {
    pub key: &'b [u8],
    pub value: Option<&'b [u8]>,
}

impl<'b> Decoder<'b> for OutPrefixIntegerEntryCodec {
    type Decoded = OutPrefixIntegerEntry<'b>;

    fn decode<R: io::Read>(
        first_tmp_buffer: &'b mut Vec<u8>,
        second_tmp_buffer: &'b mut Vec<u8>,
        reader: &mut R,
    ) -> io::Result<Option<Self::Decoded>> {
        // prefix length and prefix
        let prefix_length = match reader.read_u8() {
            Ok(prefix_length) => prefix_length,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };
        first_tmp_buffer.resize(prefix_length as usize, 0);
        reader.read_exact(first_tmp_buffer)?;
        first_tmp_buffer.push(0);

        // pos
        let pos = reader.read_u16::<NativeEndian>()?;
        first_tmp_buffer.extend_from_slice(&pos.to_be_bytes());

        // bitmap length and bitmap (bytes)
        let bitmap_length = reader.read_u32::<NativeEndian>()?;
        let bitmap = if bitmap_length == 0 {
            None
        } else {
            second_tmp_buffer.resize(bitmap_length as usize, 0);
            reader.read_exact(second_tmp_buffer)?;
            Some(second_tmp_buffer.as_slice())
        };

        Ok(Some(Self::Decoded { key: first_tmp_buffer.as_slice(), value: bitmap }))
    }
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
