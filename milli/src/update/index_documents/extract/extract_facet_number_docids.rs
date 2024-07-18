use std::fs::File;
use std::io::{self, BufReader};
use std::num::NonZeroUsize;

use heed::{BytesDecode, BytesEncode};

use super::helpers::{
    create_sorter, merge_deladd_cbo_roaring_bitmaps, sorter_into_reader, GrenadParameters,
};
use crate::heed_codec::facet::{
    FacetGroupKey, FacetGroupKeyCodec, FieldDocIdFacetF64Codec, OrderedF64Codec,
};
use crate::update::del_add::{DelAdd, KvReaderDelAdd};
use crate::update::index_documents::cache::SorterCacheDelAddCboRoaringBitmap;
use crate::update::settings::InnerIndexSettingsDiff;
use crate::update::MergeFn;
use crate::Result;

/// Extracts the facet number and the documents ids where this facet number appear.
///
/// Returns a grenad reader with the list of extracted facet numbers and
/// documents ids from the given chunk of docid facet number positions.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_facet_number_docids<R: io::Read + io::Seek>(
    fid_docid_facet_number: grenad::Reader<R>,
    indexer: GrenadParameters,
    _settings_diff: &InnerIndexSettingsDiff,
) -> Result<grenad::Reader<BufReader<File>>> {
    let max_memory = indexer.max_memory_by_thread();

    let facet_number_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Unstable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );
    let mut cached_facet_number_docids_sorter =
        SorterCacheDelAddCboRoaringBitmap::<20, MergeFn>::new(
            NonZeroUsize::new(20).unwrap(),
            facet_number_docids_sorter,
            b"fnd",
            super::REDIS_CLIENT.get_connection().unwrap(),
        );

    let mut cursor = fid_docid_facet_number.into_cursor()?;
    while let Some((key_bytes, deladd_obkv_bytes)) = cursor.move_on_next()? {
        let (field_id, document_id, number) =
            FieldDocIdFacetF64Codec::bytes_decode(key_bytes).unwrap();

        let key = FacetGroupKey { field_id, level: 0, left_bound: number };
        let key_bytes = FacetGroupKeyCodec::<OrderedF64Codec>::bytes_encode(&key).unwrap();
        for (deladd_key, _) in KvReaderDelAdd::new(deladd_obkv_bytes).iter() {
            match deladd_key {
                DelAdd::Deletion => {
                    cached_facet_number_docids_sorter.insert_del_u32(&key_bytes, document_id)?
                }
                DelAdd::Addition => {
                    cached_facet_number_docids_sorter.insert_add_u32(&key_bytes, document_id)?
                }
            }
        }
    }

    sorter_into_reader(cached_facet_number_docids_sorter.into_sorter()?, indexer)
}
