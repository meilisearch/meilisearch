use std::fs::File;
use std::num::{NonZeroU8, NonZeroUsize};
use std::{cmp, mem};

use chrono::Utc;
use grenad::{CompressionType, FileFuse, Reader, Writer};
use heed::types::{ByteSlice, DecodeIgnore};
use heed::{BytesEncode, Error};
use log::debug;
use roaring::RoaringBitmap;

use crate::error::InternalError;
use crate::heed_codec::facet::{
    FacetLevelValueF64Codec, FacetLevelValueU32Codec, FacetStringLevelZeroCodec,
    FacetStringZeroBoundsValueCodec,
};
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::update::index_documents::{
    create_writer, write_into_lmdb_database, writer_into_reader, WriteMethod,
};
use crate::{FieldId, Index, Result};

pub struct Facets<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
    _update_id: u64,
}

impl<'t, 'u, 'i> Facets<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
        update_id: u64,
    ) -> Facets<'t, 'u, 'i> {
        Facets {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            chunk_fusing_shrink_size: None,
            level_group_size: NonZeroUsize::new(4).unwrap(),
            min_level_size: NonZeroUsize::new(5).unwrap(),
            _update_id: update_id,
        }
    }

    pub fn level_group_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.level_group_size = NonZeroUsize::new(cmp::max(value.get(), 2)).unwrap();
        self
    }

    pub fn min_level_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.min_level_size = value;
        self
    }

    pub fn execute(self) -> Result<()> {
        self.index.set_updated_at(self.wtxn, &Utc::now())?;
        // We get the faceted fields to be able to create the facet levels.
        let faceted_fields = self.index.faceted_fields_ids(self.wtxn)?;

        debug!("Computing and writing the facet values levels docids into LMDB on disk...");

        for field_id in faceted_fields {
            // Clear the facet string levels.
            clear_field_string_levels(
                self.wtxn,
                self.index.facet_id_string_docids.remap_types::<ByteSlice, DecodeIgnore>(),
                field_id,
            )?;

            // Compute and store the faceted strings documents ids.
            let string_documents_ids = compute_faceted_documents_ids(
                self.wtxn,
                self.index.facet_id_string_docids.remap_key_type::<ByteSlice>(),
                field_id,
            )?;

            let facet_string_levels = compute_facet_string_levels(
                self.wtxn,
                self.index.facet_id_string_docids,
                self.chunk_compression_type,
                self.chunk_compression_level,
                self.chunk_fusing_shrink_size,
                self.level_group_size,
                self.min_level_size,
                field_id,
            )?;

            // Clear the facet number levels.
            clear_field_number_levels(self.wtxn, self.index.facet_id_f64_docids, field_id)?;

            // Compute and store the faceted numbers documents ids.
            let number_documents_ids = compute_faceted_documents_ids(
                self.wtxn,
                self.index.facet_id_f64_docids.remap_key_type::<ByteSlice>(),
                field_id,
            )?;

            let facet_number_levels = compute_facet_number_levels(
                self.wtxn,
                self.index.facet_id_f64_docids,
                self.chunk_compression_type,
                self.chunk_compression_level,
                self.chunk_fusing_shrink_size,
                self.level_group_size,
                self.min_level_size,
                field_id,
            )?;

            self.index.put_string_faceted_documents_ids(
                self.wtxn,
                field_id,
                &string_documents_ids,
            )?;
            self.index.put_number_faceted_documents_ids(
                self.wtxn,
                field_id,
                &number_documents_ids,
            )?;

            write_into_lmdb_database(
                self.wtxn,
                *self.index.facet_id_f64_docids.as_polymorph(),
                facet_number_levels,
                |_, _| Err(InternalError::IndexingMergingKeys { process: "facet number levels" }),
                WriteMethod::GetMergePut,
            )?;

            write_into_lmdb_database(
                self.wtxn,
                *self.index.facet_id_string_docids.as_polymorph(),
                facet_string_levels,
                |_, _| Err(InternalError::IndexingMergingKeys { process: "facet string levels" }),
                WriteMethod::GetMergePut,
            )?;
        }

        Ok(())
    }
}

fn clear_field_number_levels<'t>(
    wtxn: &'t mut heed::RwTxn,
    db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
    field_id: FieldId,
) -> heed::Result<()> {
    let left = (field_id, 1, f64::MIN, f64::MIN);
    let right = (field_id, u8::MAX, f64::MAX, f64::MAX);
    let range = left..=right;
    db.delete_range(wtxn, &range).map(drop)
}

fn compute_facet_number_levels<'t>(
    rtxn: &'t heed::RoTxn,
    db: heed::Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    shrink_size: Option<u64>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
    field_id: FieldId,
) -> Result<Reader<FileFuse>> {
    let first_level_size = db
        .remap_key_type::<ByteSlice>()
        .prefix_iter(rtxn, &field_id.to_be_bytes())?
        .remap_types::<DecodeIgnore, DecodeIgnore>()
        .fold(Ok(0usize), |count, result| result.and(count).map(|c| c + 1))?;

    // It is forbidden to keep a cursor and write in a database at the same time with LMDB
    // therefore we write the facet levels entries into a grenad file before transfering them.
    let mut writer = tempfile::tempfile()
        .and_then(|file| create_writer(compression_type, compression_level, file))?;

    let level_0_range = {
        let left = (field_id, 0, f64::MIN, f64::MIN);
        let right = (field_id, 0, f64::MAX, f64::MAX);
        left..=right
    };

    // Groups sizes are always a power of the original level_group_size and therefore a group
    // always maps groups of the previous level and never splits previous levels groups in half.
    let group_size_iter = (1u8..)
        .map(|l| (l, level_group_size.get().pow(l as u32)))
        .take_while(|(_, s)| first_level_size / *s >= min_level_size.get());

    for (level, group_size) in group_size_iter {
        let mut left = 0.0;
        let mut right = 0.0;
        let mut group_docids = RoaringBitmap::new();

        for (i, result) in db.range(rtxn, &level_0_range)?.enumerate() {
            let ((_field_id, _level, value, _right), docids) = result?;

            if i == 0 {
                left = value;
            } else if i % group_size == 0 {
                // we found the first bound of the next group, we must store the left
                // and right bounds associated with the docids.
                write_number_entry(&mut writer, field_id, level, left, right, &group_docids)?;

                // We save the left bound for the new group and also reset the docids.
                group_docids = RoaringBitmap::new();
                left = value;
            }

            // The right bound is always the bound we run through.
            group_docids |= docids;
            right = value;
        }

        if !group_docids.is_empty() {
            write_number_entry(&mut writer, field_id, level, left, right, &group_docids)?;
        }
    }

    writer_into_reader(writer, shrink_size)
}

fn write_number_entry(
    writer: &mut Writer<File>,
    field_id: FieldId,
    level: u8,
    left: f64,
    right: f64,
    ids: &RoaringBitmap,
) -> Result<()> {
    let key = (field_id, level, left, right);
    let key = FacetLevelValueF64Codec::bytes_encode(&key).ok_or(Error::Encoding)?;
    let data = CboRoaringBitmapCodec::bytes_encode(&ids).ok_or(Error::Encoding)?;
    writer.insert(&key, &data)?;
    Ok(())
}

fn compute_faceted_documents_ids(
    rtxn: &heed::RoTxn,
    db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
    field_id: FieldId,
) -> Result<RoaringBitmap> {
    let mut documents_ids = RoaringBitmap::new();

    for result in db.prefix_iter(rtxn, &field_id.to_be_bytes())? {
        let (_key, docids) = result?;
        documents_ids |= docids;
    }

    Ok(documents_ids)
}

fn clear_field_string_levels<'t>(
    wtxn: &'t mut heed::RwTxn,
    db: heed::Database<ByteSlice, DecodeIgnore>,
    field_id: FieldId,
) -> heed::Result<()> {
    let left = (field_id, NonZeroU8::new(1).unwrap(), u32::MIN, u32::MIN);
    let right = (field_id, NonZeroU8::new(u8::MAX).unwrap(), u32::MAX, u32::MAX);
    let range = left..=right;
    db.remap_key_type::<FacetLevelValueU32Codec>().delete_range(wtxn, &range).map(drop)
}

fn compute_facet_string_levels<'t>(
    rtxn: &'t heed::RoTxn,
    db: heed::Database<FacetStringLevelZeroCodec, CboRoaringBitmapCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    shrink_size: Option<u64>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
    field_id: FieldId,
) -> Result<Reader<FileFuse>> {
    let first_level_size = db
        .remap_key_type::<ByteSlice>()
        .prefix_iter(rtxn, &field_id.to_be_bytes())?
        .remap_types::<DecodeIgnore, DecodeIgnore>()
        .fold(Ok(0usize), |count, result| result.and(count).map(|c| c + 1))?;

    // It is forbidden to keep a cursor and write in a database at the same time with LMDB
    // therefore we write the facet levels entries into a grenad file before transfering them.
    let mut writer = tempfile::tempfile()
        .and_then(|file| create_writer(compression_type, compression_level, file))?;

    // Groups sizes are always a power of the original level_group_size and therefore a group
    // always maps groups of the previous level and never splits previous levels groups in half.
    let group_size_iter = (1u8..)
        .map(|l| (l, level_group_size.get().pow(l as u32)))
        .take_while(|(_, s)| first_level_size / *s >= min_level_size.get());

    for (level, group_size) in group_size_iter {
        let level = NonZeroU8::new(level).unwrap();
        let mut left = (0, "");
        let mut right = (0, "");
        let mut group_docids = RoaringBitmap::new();

        // Because we know the size of the level 0 we can use a range iterator that starts
        // at the first value of the level and goes to the last by simply counting.
        for (i, result) in db.range(rtxn, &((field_id, "")..))?.take(first_level_size).enumerate() {
            let ((_field_id, value), docids) = result?;

            if i == 0 {
                left = (i as u32, value);
            } else if i % group_size == 0 {
                // we found the first bound of the next group, we must store the left
                // and right bounds associated with the docids. We also reset the docids.
                let docids = mem::take(&mut group_docids);
                write_string_entry(&mut writer, field_id, level, left, right, docids)?;

                // We save the left bound for the new group.
                left = (i as u32, value);
            }

            // The right bound is always the bound we run through.
            group_docids |= docids;
            right = (i as u32, value);
        }

        if !group_docids.is_empty() {
            let docids = mem::take(&mut group_docids);
            write_string_entry(&mut writer, field_id, level, left, right, docids)?;
        }
    }

    writer_into_reader(writer, shrink_size)
}

fn write_string_entry(
    writer: &mut Writer<File>,
    field_id: FieldId,
    level: NonZeroU8,
    (left_id, left_value): (u32, &str),
    (right_id, right_value): (u32, &str),
    docids: RoaringBitmap,
) -> Result<()> {
    let key = (field_id, level, left_id, right_id);
    let key = FacetLevelValueU32Codec::bytes_encode(&key).ok_or(Error::Encoding)?;
    let data = match level.get() {
        1 => (Some((left_value, right_value)), docids),
        _ => (None, docids),
    };
    let data = FacetStringZeroBoundsValueCodec::<CboRoaringBitmapCodec>::bytes_encode(&data)
        .ok_or(Error::Encoding)?;
    writer.insert(&key, &data)?;
    Ok(())
}
