use std::collections::HashSet;

use hashbrown::HashMap;
use heed::{types::Bytes, RwTxn};
use heed::{BytesDecode, Database};
use roaring::RoaringBitmap;

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
        let mut docids = RoaringBitmap::new();
        for prefix in prefixes {
            docids.clear();
            let prefix = prefix.as_bytes();
            for result in self.database.prefix_iter(wtxn, prefix)? {
                let (_word, data) = result?;
                docids |= &data;
            }

            self.prefix_database.put(wtxn, prefix, &docids)?;
        }

        Ok(())
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
                    self.prefix_database.put(wtxn, &key_buffer, &docids)?;
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
        let prefix = prefix.as_bytes();
        if !prefix_database.delete(wtxn, prefix)? {
            unreachable!("We tried to delete an unknown key")
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
