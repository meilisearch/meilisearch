use std::fs::File;
use std::io::BufReader;
use std::{iter, mem};

use grenad::CompressionType;
use heed::types::{Bytes, LazyDecode};
use heed::{Database, RwTxn};
use rayon::prelude::*;
use roaring::MultiOps;
use tempfile::tempfile;

use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::heed_codec::BytesRefCodec;
use crate::update::facet::{FACET_GROUP_SIZE, FACET_MIN_LEVEL_SIZE};
use crate::update::{create_writer, writer_into_reader};
use crate::{CboRoaringBitmapCodec, FieldId, Index};

/// Generate the facet level based on the level 0.
///
/// The function will generate all the group levels from
/// the group 1 to the level n until the number of group
/// is smaller than the minimum required size.
pub fn generate_facet_levels(
    index: &Index,
    wtxn: &mut RwTxn,
    field_id: FieldId,
    facet_type: FacetType,
) -> crate::Result<()> {
    let db = match facet_type {
        FacetType::String => index
            .facet_id_string_docids
            .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>()
            .lazily_decode_data(),
        FacetType::Number => index
            .facet_id_f64_docids
            .remap_key_type::<FacetGroupKeyCodec<BytesRefCodec>>()
            .lazily_decode_data(),
    };

    clear_levels(db, wtxn, field_id)?;

    let mut base_level: u8 = 0;
    // That's a do-while loop
    while {
        let mut level_size = 0;
        let level = base_level.checked_add(1).unwrap();
        for reader in compute_level(index, wtxn, db, field_id, base_level)? {
            let mut cursor = reader.into_cursor()?;
            while let Some((left_bound, facet_group_value)) = cursor.move_on_next()? {
                level_size += 1;
                let key = FacetGroupKey { field_id, level, left_bound };
                debug_assert!(
                    db.get(wtxn, &key).transpose().is_none(),
                    "entry must not be there and must have already been deleted: {key:?}"
                );
                db.remap_data_type::<Bytes>().put(wtxn, &key, facet_group_value)?;
            }
        }

        base_level = level;

        // If the next level will have the minimum required groups, continue.
        (level_size / FACET_GROUP_SIZE as usize) >= FACET_MIN_LEVEL_SIZE as usize
    } {}

    Ok(())
}

/// Compute the groups of facets from the provided base level
/// and write the content into different grenad files.
fn compute_level(
    index: &Index,
    wtxn: &heed::RwTxn,
    db: Database<FacetGroupKeyCodec<BytesRefCodec>, LazyDecode<FacetGroupValueCodec>>,
    field_id: FieldId,
    base_level: u8,
) -> Result<Vec<grenad::Reader<BufReader<File>>>, crate::Error> {
    let thread_count = rayon::current_num_threads();
    let rtxns = iter::repeat_with(|| index.env.nested_read_txn(wtxn))
        .take(thread_count)
        .collect::<heed::Result<Vec<_>>>()?;

    let range = {
        // Based on the first possible value for the base level up to
        // the first possible value for the next level *excluded*.
        let left = FacetGroupKey::<&[u8]> { field_id, level: base_level, left_bound: &[] };
        let right = FacetGroupKey::<&[u8]> {
            field_id,
            level: base_level.checked_add(1).unwrap(),
            left_bound: &[],
        };
        left..right
    };

    rtxns
        .into_par_iter()
        .enumerate()
        .map(|(thread_id, rtxn)| {
            let mut writer = tempfile().map(|f| create_writer(CompressionType::None, None, f))?;

            let mut left_bound = None;
            let mut group_docids = Vec::new();
            let mut ser_buffer = Vec::new();
            for (i, result) in db.range(&rtxn, &range)?.enumerate() {
                let (key, lazy_value) = result?;

                let start_of_group = i % FACET_GROUP_SIZE as usize == 0;
                let group_index = i / FACET_GROUP_SIZE as usize;
                let group_for_thread = group_index % thread_count == thread_id;

                if group_for_thread {
                    if start_of_group {
                        if let Some(left_bound) = left_bound.take() {
                            // We store the bitmaps in a Vec this way we can use
                            // the MultiOps operations that tends to be more efficient
                            // for unions. The Vec is empty after the operation.
                            //
                            // We also don't forget to store the group size corresponding
                            // to the number of entries merged in this group.
                            ser_buffer.clear();
                            let group_len: u8 = group_docids.len().try_into().unwrap();
                            ser_buffer.push(group_len);
                            let group_docids = mem::take(&mut group_docids);
                            let docids = group_docids.into_iter().union();
                            CboRoaringBitmapCodec::serialize_into_vec(&docids, &mut ser_buffer);
                            writer.insert(left_bound, &ser_buffer)?;
                        }
                        left_bound = Some(key.left_bound);
                    }

                    // Lazily decode the bitmaps we are interested in.
                    let value = lazy_value.decode().map_err(heed::Error::Decoding)?;
                    group_docids.push(value.bitmap);
                }
            }

            if let Some(left_bound) = left_bound.take() {
                ser_buffer.clear();
                // We don't forget to store the group size corresponding
                // to the number of entries merged in this group.
                let group_len: u8 = group_docids.len().try_into().unwrap();
                ser_buffer.push(group_len);
                let group_docids = group_docids.into_iter().union();
                CboRoaringBitmapCodec::serialize_into_vec(&group_docids, &mut ser_buffer);
                writer.insert(left_bound, &ser_buffer)?;
            }

            writer_into_reader(writer)
        })
        .collect()
}

/// Clears all the levels and only keeps the level 0 of the specified field id.
fn clear_levels(
    db: Database<FacetGroupKeyCodec<BytesRefCodec>, LazyDecode<FacetGroupValueCodec>>,
    wtxn: &mut RwTxn<'_>,
    field_id: FieldId,
) -> heed::Result<()> {
    let left = FacetGroupKey::<&[u8]> { field_id, level: 1, left_bound: &[] };
    let right = FacetGroupKey::<&[u8]> { field_id, level: u8::MAX, left_bound: &[] };
    let range = left..=right;
    db.delete_range(wtxn, &range).map(drop)
}
