use std::fs::File;

use grenad::{CompressionType, Reader, Writer, FileFuse};
use heed::types::{ByteSlice, DecodeIgnore};
use heed::{BytesEncode, Error};
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::{facet::FacetLevelValueI64Codec, CboRoaringBitmapCodec};
use crate::update::index_documents::{create_writer, writer_into_reader};

pub fn clear_field_levels(
    wtxn: &mut heed::RwTxn,
    db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
    field_id: u8,
) -> heed::Result<()>
{
    let range = (field_id, 1, i64::MIN, i64::MIN)..=(field_id, u8::MAX, i64::MAX, i64::MAX);
    db.remap_key_type::<FacetLevelValueI64Codec>()
        .delete_range(wtxn, &range)
        .map(drop)
}

pub fn compute_facet_levels(
    rtxn: &heed::RoTxn,
    db: heed::Database<ByteSlice, CboRoaringBitmapCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    shrink_size: Option<u64>,
    field_id: u8,
    facet_type: FacetType,
) -> anyhow::Result<Reader<FileFuse>>
{
    let last_level_size = 5;
    let number_of_levels = 5;
    let first_level_size = db.prefix_iter(rtxn, &[field_id])?
        .remap_types::<DecodeIgnore, DecodeIgnore>()
        .fold(Ok(0u64), |count, result| result.and(count).map(|c| c + 1))?;

    // It is forbidden to keep a cursor and write in a database at the same time with LMDB
    // therefore we write the facet levels entries into a grenad file before transfering them.
    let mut writer = tempfile::tempfile().and_then(|file| {
        create_writer(compression_type, compression_level, file)
    })?;

    let level_0_range = (field_id, 0, i64::MIN, i64::MIN)..=(field_id, 0, i64::MAX, i64::MAX);
    let level_sizes_iter = levels_iterator(first_level_size, last_level_size, number_of_levels)
        .enumerate()
        .skip(1);

    // TODO we must not create levels with identical group sizes.
    for (level, size) in level_sizes_iter {
        let level_entry_sizes = (first_level_size as f64 / size as f64).ceil() as usize;
        let mut left = 0;
        let mut right = 0;
        let mut group_docids = RoaringBitmap::new();

        let db = db.remap_key_type::<FacetLevelValueI64Codec>();
        for (i, result) in db.range(rtxn, &level_0_range)?.enumerate() {
            let ((_field_id, _level, value, _right), docids) = result?;

            if i == 0 {
                left = value;
            } else if i % level_entry_sizes == 0 {
                // we found the first bound of the next group, we must store the left
                // and right bounds associated with the docids.
                write_entry(&mut writer, field_id, level as u8, left, right, &group_docids)?;

                // We save the left bound for the new group and also reset the docids.
                group_docids = RoaringBitmap::new();
                left = value;
            }

            // The right bound is always the bound we run through.
            group_docids.union_with(&docids);
            right = value;
        }

        if !group_docids.is_empty() {
            write_entry(&mut writer, field_id, level as u8, left, right, &group_docids)?;
        }
    }

    writer_into_reader(writer, shrink_size)
}

fn write_entry(
    writer: &mut Writer<File>,
    field_id: u8,
    level: u8,
    left: i64,
    right: i64,
    ids: &RoaringBitmap,
) -> anyhow::Result<()>
{
    let key = (field_id, level, left, right);
    let key = FacetLevelValueI64Codec::bytes_encode(&key).ok_or(Error::Encoding)?;
    let data = CboRoaringBitmapCodec::bytes_encode(&ids).ok_or(Error::Encoding)?;
    writer.insert(&key, &data)?;
    Ok(())
}

fn levels_iterator(
    first_level_size: u64, // biggest level
    last_level_size: u64, // smallest level
    number_of_levels: u64,
) -> impl Iterator<Item=u64>
{
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

    let b = last_level_size as f64;
    let end = first_level_size as f64;
    let c = end - b;
    let d = number_of_levels;
    (0..=d).map(move |t| ((end + b) - ease_out_expo(t as f64, b, c, d as f64)) as u64)
}
