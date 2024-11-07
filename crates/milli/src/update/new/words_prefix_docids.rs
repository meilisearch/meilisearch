use std::cell::RefCell;
use std::collections::HashSet;
use std::io::{BufReader, BufWriter, Read, Seek, Write};

use hashbrown::HashMap;
use heed::types::Bytes;
use heed::{BytesDecode, Database, RoTxn, RwTxn};
use rayon::iter::{IntoParallelIterator, ParallelIterator as _};
use roaring::MultiOps;
use tempfile::tempfile;
use thread_local::ThreadLocal;

use super::ref_cell_ext::RefCellExt as _;
use crate::heed_codec::StrBEU16Codec;
use crate::{CboRoaringBitmapCodec, Index, Prefix, Result};

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
        prefix_to_compute: &HashSet<Prefix>,
        prefix_to_delete: &HashSet<Prefix>,
    ) -> Result<()> {
        delete_prefixes(wtxn, &self.prefix_database, prefix_to_delete)?;
        self.recompute_modified_prefixes(wtxn, prefix_to_compute)
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
    fn recompute_modified_prefixes(
        &self,
        wtxn: &mut RwTxn,
        prefixes: &HashSet<Prefix>,
    ) -> Result<()> {
        // We fetch the docids associated to the newly added word prefix fst only.
        // And collect the CboRoaringBitmaps pointers in an HashMap.
        let frozen = FrozenPrefixBitmaps::from_prefixes(self.database, wtxn, prefixes)?;

        // We access this HashMap in parallel to compute the *union* of all
        // of them and *serialize* them into files. There is one file by CPU.
        let local_entries = ThreadLocal::with_capacity(rayon::current_num_threads());
        prefixes.into_par_iter().map(AsRef::as_ref).try_for_each(|prefix| {
            let refcell = local_entries.get_or_try(|| {
                tempfile().map(BufWriter::new).map(|f| RefCell::new((Vec::new(), f, Vec::new())))
            })?;

            let mut refmut = refcell.borrow_mut_or_yield();
            let (ref mut index, ref mut file, ref mut buffer) = *refmut;

            let output = frozen
                .bitmaps(prefix)
                .unwrap()
                .iter()
                .map(|bytes| CboRoaringBitmapCodec::deserialize_from(bytes))
                .union()?;

            buffer.clear();
            CboRoaringBitmapCodec::serialize_into(&output, buffer);
            index.push(PrefixEntry { prefix, serialized_length: buffer.len() });
            file.write_all(buffer)
        })?;

        drop(frozen);

        // We iterate over all the collected and serialized bitmaps through
        // the files and entries to eventually put them in the final database.
        for refcell in local_entries {
            let (index, file, mut buffer) = refcell.into_inner();
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

/// Stores prefixes along with all the pointers to the associated
/// CBoRoaringBitmaps.
///
/// They are collected synchronously and stored into an HashMap. The
/// Synchronous process is doing a small amount of work by just storing
/// pointers. It can then be accessed in parallel to get the associated
/// bitmaps pointers.
struct FrozenPrefixBitmaps<'a, 'rtxn> {
    prefixes_bitmaps: HashMap<&'a str, Vec<&'rtxn [u8]>>,
}

impl<'a, 'rtxn> FrozenPrefixBitmaps<'a, 'rtxn> {
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
    pub fn from_prefixes(
        database: Database<Bytes, CboRoaringBitmapCodec>,
        rtxn: &'rtxn RoTxn,
        prefixes: &'a HashSet<Prefix>,
    ) -> heed::Result<Self> {
        let database = database.remap_data_type::<Bytes>();

        let mut prefixes_bitmaps = HashMap::new();
        for prefix in prefixes {
            let mut bitmap_bytes = Vec::new();
            for result in database.prefix_iter(rtxn, prefix.as_bytes())? {
                let (_word, bytes) = result?;
                bitmap_bytes.push(bytes);
            }
            assert!(prefixes_bitmaps.insert(prefix.as_str(), bitmap_bytes).is_none());
        }

        Ok(Self { prefixes_bitmaps })
    }

    pub fn bitmaps(&self, key: &str) -> Option<&[&'rtxn [u8]]> {
        self.prefixes_bitmaps.get(key).map(AsRef::as_ref)
    }
}

unsafe impl<'a, 'rtxn> Sync for FrozenPrefixBitmaps<'a, 'rtxn> {}

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
        prefix_to_compute: &HashSet<Prefix>,
        prefix_to_delete: &HashSet<Prefix>,
    ) -> Result<()> {
        delete_prefixes(wtxn, &self.prefix_database, prefix_to_delete)?;
        self.recompute_modified_prefixes(wtxn, prefix_to_compute)
    }

    #[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
    fn recompute_modified_prefixes(
        &self,
        wtxn: &mut RwTxn,
        prefixes: &HashSet<Prefix>,
    ) -> Result<()> {
        // We fetch the docids associated to the newly added word prefix fst only.
        // We use a HashMap to store the docids associated to each position, may be RAM consuming.
        let mut integer_docids = HashMap::new();
        let mut key_buffer = Vec::new();
        for prefix in prefixes {
            let prefix = prefix.as_bytes();
            for result in self.database.prefix_iter(wtxn, prefix)? {
                let (key, data) = result?;
                let (_word, pos) =
                    StrBEU16Codec::bytes_decode(key).map_err(heed::Error::Decoding)?;

                match integer_docids.get_mut(&pos) {
                    Some(docids) => {
                        *docids |= &data;
                    }
                    None => {
                        integer_docids.insert(pos, data);
                    }
                }
            }

            for (pos, docids) in integer_docids.iter_mut() {
                if !docids.is_empty() {
                    key_buffer.clear();
                    key_buffer.extend_from_slice(prefix);
                    key_buffer.push(0);
                    key_buffer.extend_from_slice(&pos.to_be_bytes());
                    self.prefix_database.put(wtxn, &key_buffer, docids)?;
                }
                docids.clear();
            }
        }

        Ok(())
    }
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
fn delete_prefixes(
    wtxn: &mut RwTxn,
    prefix_database: &Database<Bytes, CboRoaringBitmapCodec>,
    prefixes: &HashSet<Prefix>,
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
    prefix_to_compute: &HashSet<Prefix>,
    prefix_to_delete: &HashSet<Prefix>,
) -> Result<()> {
    WordPrefixDocids::new(
        index.word_docids.remap_key_type(),
        index.word_prefix_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
pub fn compute_exact_word_prefix_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &HashSet<Prefix>,
    prefix_to_delete: &HashSet<Prefix>,
) -> Result<()> {
    WordPrefixDocids::new(
        index.exact_word_docids.remap_key_type(),
        index.exact_word_prefix_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
pub fn compute_word_prefix_fid_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &HashSet<Prefix>,
    prefix_to_delete: &HashSet<Prefix>,
) -> Result<()> {
    WordPrefixIntegerDocids::new(
        index.word_fid_docids.remap_key_type(),
        index.word_prefix_fid_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
pub fn compute_word_prefix_position_docids(
    wtxn: &mut RwTxn,
    index: &Index,
    prefix_to_compute: &HashSet<Prefix>,
    prefix_to_delete: &HashSet<Prefix>,
) -> Result<()> {
    WordPrefixIntegerDocids::new(
        index.word_position_docids.remap_key_type(),
        index.word_prefix_position_docids.remap_key_type(),
    )
    .execute(wtxn, prefix_to_compute, prefix_to_delete)
}
