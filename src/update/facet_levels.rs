use std::fs::File;
use std::num::NonZeroUsize;

use grenad::{CompressionType, Reader, Writer, FileFuse};
use heed::types::{ByteSlice, DecodeIgnore};
use heed::{BytesEncode, Error};
use itertools::Itertools;
use log::debug;
use num_traits::{Bounded, Zero};
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::CboRoaringBitmapCodec;
use crate::heed_codec::facet::{FacetLevelValueI64Codec, FacetLevelValueF64Codec};
use crate::Index;
use crate::update::index_documents::WriteMethod;
use crate::update::index_documents::{create_writer, writer_into_reader, write_into_lmdb_database};

#[derive(Debug, Copy, Clone)]
pub enum EasingName {
    Expo,
    Quart,
    Circ,
    Linear,
}

pub struct FacetLevels<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    number_of_levels: NonZeroUsize,
    last_level_size: NonZeroUsize,
    easing_function: EasingName,
}

impl<'t, 'u, 'i> FacetLevels<'t, 'u, 'i> {
    pub fn new(wtxn: &'t mut heed::RwTxn<'i, 'u>, index: &'i Index) -> FacetLevels<'t, 'u, 'i> {
        FacetLevels {
            wtxn,
            index,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: None,
            chunk_fusing_shrink_size: None,
            number_of_levels: NonZeroUsize::new(5).unwrap(),
            last_level_size: NonZeroUsize::new(5).unwrap(),
            easing_function: EasingName::Expo,
        }
    }

    pub fn number_of_levels(&mut self, value: NonZeroUsize) -> &mut Self {
        self.number_of_levels = value;
        self
    }

    pub fn last_level_size(&mut self, value: NonZeroUsize) -> &mut Self {
        self.last_level_size = value;
        self
    }

    pub fn easing_function(&mut self, value: EasingName) -> &mut Self {
        self.easing_function = value;
        self
    }

    pub fn execute(self) -> anyhow::Result<()> {
        // We get the faceted fields to be able to create the facet levels.
        let faceted_fields = self.index.faceted_fields(self.wtxn)?;

        debug!("Computing and writing the facet values levels docids into LMDB on disk...");
        for (field_id, facet_type) in faceted_fields {
            let content = match facet_type {
                FacetType::Integer => {
                    clear_field_levels::<i64, FacetLevelValueI64Codec>(
                        self.wtxn,
                        self.index.facet_field_id_value_docids,
                        field_id,
                    )?;

                    compute_facet_levels::<i64, FacetLevelValueI64Codec>(
                        self.wtxn,
                        self.index.facet_field_id_value_docids,
                        self.chunk_compression_type,
                        self.chunk_compression_level,
                        self.chunk_fusing_shrink_size,
                        self.last_level_size,
                        self.number_of_levels,
                        self.easing_function,
                        field_id,
                    )?
                },
                FacetType::Float => {
                    clear_field_levels::<f64, FacetLevelValueF64Codec>(
                        self.wtxn,
                        self.index.facet_field_id_value_docids,
                        field_id,
                    )?;

                    compute_facet_levels::<f64, FacetLevelValueF64Codec>(
                        self.wtxn,
                        self.index.facet_field_id_value_docids,
                        self.chunk_compression_type,
                        self.chunk_compression_level,
                        self.chunk_fusing_shrink_size,
                        self.last_level_size,
                        self.number_of_levels,
                        self.easing_function,
                        field_id,
                    )?
                },
                FacetType::String => continue,
            };

            write_into_lmdb_database(
                self.wtxn,
                *self.index.facet_field_id_value_docids.as_polymorph(),
                content,
                |_, _| anyhow::bail!("invalid facet level merging"),
                WriteMethod::GetMergePut,
            )?;
        }

        Ok(())
    }
}

fn clear_field_levels<'t, T: 't, KC>(
    wtxn: &'t mut heed::RwTxn,
    db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
    field_id: u8,
) -> heed::Result<()>
where
    T: Copy + Bounded,
    KC: heed::BytesDecode<'t, DItem = (u8, u8, T, T)>,
    KC: for<'x> heed::BytesEncode<'x, EItem = (u8, u8, T, T)>,
{
    let left = (field_id, 1, T::min_value(), T::min_value());
    let right = (field_id, u8::MAX, T::max_value(), T::max_value());
    let range = left..=right;
    db.remap_key_type::<KC>()
        .delete_range(wtxn, &range)
        .map(drop)
}

fn compute_facet_levels<'t, T: 't, KC>(
    rtxn: &'t heed::RoTxn,
    db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    shrink_size: Option<u64>,
    last_level_size: NonZeroUsize,
    number_of_levels: NonZeroUsize,
    easing_function: EasingName,
    field_id: u8,
) -> anyhow::Result<Reader<FileFuse>>
where
    T: Copy + PartialEq + PartialOrd + Bounded + Zero,
    KC: heed::BytesDecode<'t, DItem = (u8, u8, T, T)>,
    KC: for<'x> heed::BytesEncode<'x, EItem = (u8, u8, T, T)>,
{
    let first_level_size = db.prefix_iter(rtxn, &[field_id])?
        .remap_types::<DecodeIgnore, DecodeIgnore>()
        .fold(Ok(0usize), |count, result| result.and(count).map(|c| c + 1))?;

    // It is forbidden to keep a cursor and write in a database at the same time with LMDB
    // therefore we write the facet levels entries into a grenad file before transfering them.
    let mut writer = tempfile::tempfile().and_then(|file| {
        create_writer(compression_type, compression_level, file)
    })?;

    let level_0_range = {
        let left = (field_id, 0, T::min_value(), T::min_value());
        let right = (field_id, 0, T::max_value(), T::max_value());
        left..=right
    };

    let level_sizes_iter =
        levels_iterator(first_level_size, last_level_size.get(), number_of_levels.get(), easing_function)
            .map(|size| (first_level_size as f64 / size as f64).ceil() as usize)
            .unique()
            .enumerate()
            .skip(1);

    // TODO we must not create levels with identical group sizes.
    for (level, level_entry_sizes) in level_sizes_iter {
        let mut left = T::zero();
        let mut right = T::zero();
        let mut group_docids = RoaringBitmap::new();

        let db = db.remap_key_type::<KC>();
        for (i, result) in db.range(rtxn, &level_0_range)?.enumerate() {
            let ((_field_id, _level, value, _right), docids) = result?;

            if i == 0 {
                left = value;
            } else if i % level_entry_sizes == 0 {
                // we found the first bound of the next group, we must store the left
                // and right bounds associated with the docids.
                write_entry::<T, KC>(&mut writer, field_id, level as u8, left, right, &group_docids)?;

                // We save the left bound for the new group and also reset the docids.
                group_docids = RoaringBitmap::new();
                left = value;
            }

            // The right bound is always the bound we run through.
            group_docids.union_with(&docids);
            right = value;
        }

        if !group_docids.is_empty() {
            write_entry::<T, KC>(&mut writer, field_id, level as u8, left, right, &group_docids)?;
        }
    }

    writer_into_reader(writer, shrink_size)
}

fn write_entry<T, KC>(
    writer: &mut Writer<File>,
    field_id: u8,
    level: u8,
    left: T,
    right: T,
    ids: &RoaringBitmap,
) -> anyhow::Result<()>
where
    KC: for<'x> heed::BytesEncode<'x, EItem = (u8, u8, T, T)>,
{
    let key = (field_id, level, left, right);
    let key = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
    let data = CboRoaringBitmapCodec::bytes_encode(&ids).ok_or(Error::Encoding)?;
    writer.insert(&key, &data)?;
    Ok(())
}

fn levels_iterator(
    first_level_size: usize, // biggest level
    last_level_size: usize, // smallest level
    number_of_levels: usize,
    easing_function: EasingName,
) -> impl Iterator<Item=usize>
{
    let easing_function = match easing_function {
        EasingName::Expo => ease_out_expo,
        EasingName::Quart => ease_out_quart,
        EasingName::Circ => ease_out_circ,
        EasingName::Linear => ease_out_linear,
    };

    let b = last_level_size as f64;
    let end = first_level_size as f64;
    let c = end - b;
    let d = number_of_levels;
    (0..=d).map(move |t| ((end + b) - easing_function(t as f64, b, c, d as f64)) as usize)
}

// Go look at the function definitions here:
// https://docs.rs/easer/0.2.1/easer/index.html
// https://easings.net/#easeOutExpo
fn ease_out_expo(t: f64, b: f64, c: f64, d: f64) -> f64 {
    if t == d {
        b + c
    } else {
        c * (-2.0_f64.powf(-10.0 * t / d) + 1.0) + b
    }
}

// https://easings.net/#easeOutCirc
fn ease_out_circ(t: f64, b: f64, c: f64, d: f64) -> f64 {
    let t = t / d - 1.0;
    c * (1.0 - t * t).sqrt() + b
}

// https://easings.net/#easeOutQuart
fn ease_out_quart(t: f64, b: f64, c: f64, d: f64) -> f64 {
    let t = t / d - 1.0;
    -c * ((t * t * t * t) - 1.0) + b
}

fn ease_out_linear(t: f64, b: f64, c: f64, d: f64) -> f64 {
    c * t / d + b
}
