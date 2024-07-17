use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;
use std::iter::FromIterator;
use std::num::NonZeroUsize;
use std::{io, str};

use charabia::normalizer::{Normalize, NormalizerOption};
use heed::types::SerdeJson;
use heed::BytesEncode;

use super::helpers::{create_sorter, sorter_into_reader, try_split_array_at, GrenadParameters};
use super::REDIS_CLIENT;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec};
use crate::heed_codec::{BEU16StrCodec, StrRefCodec};
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::cache::SorterCacheDelAddCboRoaringBitmap;
use crate::update::index_documents::helpers::{
    merge_deladd_btreeset_string, merge_deladd_cbo_roaring_bitmaps,
};
use crate::update::settings::InnerIndexSettingsDiff;
use crate::update::MergeFn;
use crate::{FieldId, Result, MAX_FACET_VALUE_LENGTH};

/// Extracts the facet string and the documents ids where this facet string appear.
///
/// Returns a grenad reader with the list of extracted facet strings and
/// documents ids from the given chunk of docid facet string positions.
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_facet_string_docids<R: io::Read + io::Seek>(
    docid_fid_facet_string: grenad::Reader<R>,
    indexer: GrenadParameters,
    _settings_diff: &InnerIndexSettingsDiff,
) -> Result<(grenad::Reader<BufReader<File>>, grenad::Reader<BufReader<File>>)> {
    let mut conn = REDIS_CLIENT.get_connection().unwrap();
    let max_memory = indexer.max_memory_by_thread();
    let options = NormalizerOption { lossy: true, ..Default::default() };

    let facet_string_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
    );
    let mut cached_facet_string_docids_sorter =
        SorterCacheDelAddCboRoaringBitmap::<20, MergeFn>::new(
            NonZeroUsize::new(200).unwrap(),
            facet_string_docids_sorter,
            REDIS_CLIENT.get_connection().unwrap(),
        );

    let mut normalized_facet_string_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        merge_deladd_btreeset_string,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory.map(|m| m / 2),
    );

    let mut buffer = Vec::new();
    let mut cursor = docid_fid_facet_string.into_cursor()?;
    while let Some((key, deladd_original_value_bytes)) = cursor.move_on_next()? {
        let deladd_reader = KvReaderDelAdd::new(deladd_original_value_bytes);

        // nothing to do if we delete and re-add the value.
        if deladd_reader.get(DelAdd::Deletion).is_some()
            && deladd_reader.get(DelAdd::Addition).is_some()
        {
            continue;
        }

        let (field_id_bytes, bytes) = try_split_array_at(key).unwrap();
        let field_id = FieldId::from_be_bytes(field_id_bytes);

        let (document_id_bytes, normalized_value_bytes) =
            try_split_array_at::<_, 4>(bytes).unwrap();
        let document_id = u32::from_be_bytes(document_id_bytes);

        let normalized_value = str::from_utf8(normalized_value_bytes)?;

        // Facet search normalization
        {
            let mut hyper_normalized_value = normalized_value.normalize(&options);
            let normalized_truncated_facet: String;
            if hyper_normalized_value.len() > MAX_FACET_VALUE_LENGTH {
                normalized_truncated_facet = hyper_normalized_value
                    .char_indices()
                    .take_while(|(idx, _)| *idx < MAX_FACET_VALUE_LENGTH)
                    .map(|(_, c)| c)
                    .collect();
                hyper_normalized_value = normalized_truncated_facet.into();
            }
            let set = BTreeSet::from_iter(std::iter::once(normalized_value));

            buffer.clear();
            let mut obkv = KvWriterDelAdd::new(&mut buffer);
            for (deladd_key, _) in deladd_reader.iter() {
                let val = SerdeJson::bytes_encode(&set).map_err(heed::Error::Encoding)?;
                obkv.insert(deladd_key, val)?;
            }
            obkv.finish()?;

            let key = (field_id, hyper_normalized_value.as_ref());
            let key_bytes = BEU16StrCodec::bytes_encode(&key).map_err(heed::Error::Encoding)?;
            redis::cmd("INCR").arg(key_bytes.as_ref()).query::<usize>(&mut conn).unwrap();
            normalized_facet_string_docids_sorter.insert(key_bytes, &buffer)?;
        }

        let key = FacetGroupKey { field_id, level: 0, left_bound: normalized_value };
        let key_bytes = FacetGroupKeyCodec::<StrRefCodec>::bytes_encode(&key).unwrap();
        for (deladd_key, _) in deladd_reader.iter() {
            match deladd_key {
                DelAdd::Deletion => {
                    cached_facet_string_docids_sorter.insert_del_u32(&key_bytes, document_id)?
                }
                DelAdd::Addition => {
                    cached_facet_string_docids_sorter.insert_add_u32(&key_bytes, document_id)?
                }
            }
        }
    }

    let normalized = sorter_into_reader(normalized_facet_string_docids_sorter, indexer)?;
    sorter_into_reader(cached_facet_string_docids_sorter.into_sorter()?, indexer)
        .map(|s| (s, normalized))
}
