use std::cmp;
use std::fs::File;
use std::num::NonZeroUsize;

use grenad::{CompressionType, Reader, Writer, FileFuse};
use heed::types::{ByteSlice, DecodeIgnore};
use heed::{BytesEncode, Error};
use log::debug;
use roaring::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::{StrLevelPositionCodec, CboRoaringBitmapCodec};
use crate::Index;
use crate::update::index_documents::WriteMethod;
use crate::update::index_documents::{create_writer, writer_into_reader, write_into_lmdb_database};

pub struct WordsLevelPositions<'t, 'u, 'i> {
    wtxn: &'t mut heed::RwTxn<'i, 'u>,
    index: &'i Index,
    pub(crate) chunk_compression_type: CompressionType,
    pub(crate) chunk_compression_level: Option<u32>,
    pub(crate) chunk_fusing_shrink_size: Option<u64>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
    _update_id: u64,
}

impl<'t, 'u, 'i> WordsLevelPositions<'t, 'u, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i, 'u>,
        index: &'i Index,
        update_id: u64,
    ) -> WordsLevelPositions<'t, 'u, 'i>
    {
        WordsLevelPositions {
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

    pub fn execute(self) -> anyhow::Result<()> {
        debug!("Computing and writing the word levels positions docids into LMDB on disk...");

        clear_non_zero_levels_positions(self.wtxn, self.index.word_level_position_docids)?;

        let entries = compute_positions_levels(
            self.wtxn,
            self.index.word_level_position_docids,
            self.chunk_compression_type,
            self.chunk_compression_level,
            self.chunk_fusing_shrink_size,
            self.level_group_size,
            self.min_level_size,
        )?;

        write_into_lmdb_database(
            self.wtxn,
            *self.index.facet_field_id_value_docids.as_polymorph(),
            entries,
            |_, _| anyhow::bail!("invalid facet level merging"),
            WriteMethod::GetMergePut,
        )?;

        Ok(())
    }
}

fn clear_non_zero_levels_positions(
    wtxn: &mut heed::RwTxn,
    db: heed::Database<StrLevelPositionCodec, CboRoaringBitmapCodec>,
) -> heed::Result<()>
{
    let mut iter = db.iter_mut(wtxn)?.lazily_decode_data();
    while let Some(result) = iter.next() {
        let ((_, level, _, _), _) = result?;
        if level != 0 {
            iter.del_current()?;
        }
    }
    Ok(())
}

/// Generates all the words positions levels (including the level zero).
fn compute_positions_levels(
    rtxn: &heed::RoTxn,
    db: heed::Database<StrLevelPositionCodec, CboRoaringBitmapCodec>,
    compression_type: CompressionType,
    compression_level: Option<u32>,
    shrink_size: Option<u64>,
    level_group_size: NonZeroUsize,
    min_level_size: NonZeroUsize,
) -> anyhow::Result<Reader<FileFuse>>
{
    // let first_level_size = db.prefix_iter(rtxn, &[field_id])?
    //     .remap_types::<DecodeIgnore, DecodeIgnore>()
    //     .fold(Ok(0usize), |count, result| result.and(count).map(|c| c + 1))?;

    // // It is forbidden to keep a cursor and write in a database at the same time with LMDB
    // // therefore we write the facet levels entries into a grenad file before transfering them.
    // let mut writer = tempfile::tempfile().and_then(|file| {
    //     create_writer(compression_type, compression_level, file)
    // })?;

    // let level_0_range = {
    //     let left = (field_id, 0, T::min_value(), T::min_value());
    //     let right = (field_id, 0, T::max_value(), T::max_value());
    //     left..=right
    // };

    // // Groups sizes are always a power of the original level_group_size and therefore a group
    // // always maps groups of the previous level and never splits previous levels groups in half.
    // let group_size_iter = (1u8..)
    //     .map(|l| (l, level_group_size.get().pow(l as u32)))
    //     .take_while(|(_, s)| first_level_size / *s >= min_level_size.get());

    // for (level, group_size) in group_size_iter {
    //     let mut left = T::zero();
    //     let mut right = T::zero();
    //     let mut group_docids = RoaringBitmap::new();

    //     let db = db.remap_key_type::<KC>();
    //     for (i, result) in db.range(rtxn, &level_0_range)?.enumerate() {
    //         let ((_field_id, _level, value, _right), docids) = result?;

    //         if i == 0 {
    //             left = value;
    //         } else if i % group_size == 0 {
    //             // we found the first bound of the next group, we must store the left
    //             // and right bounds associated with the docids.
    //             write_entry::<T, KC>(&mut writer, field_id, level, left, right, &group_docids)?;

    //             // We save the left bound for the new group and also reset the docids.
    //             group_docids = RoaringBitmap::new();
    //             left = value;
    //         }

    //         // The right bound is always the bound we run through.
    //         group_docids.union_with(&docids);
    //         right = value;
    //     }

    //     if !group_docids.is_empty() {
    //         write_entry::<T, KC>(&mut writer, field_id, level, left, right, &group_docids)?;
    //     }
    // }

    // writer_into_reader(writer, shrink_size)

    todo!()
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
