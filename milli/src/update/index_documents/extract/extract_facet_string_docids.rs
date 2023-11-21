use std::fs::File;
use std::io::BufReader;
use std::{io, str};

use heed::BytesEncode;

use super::helpers::{create_sorter, sorter_into_reader, try_split_array_at, GrenadParameters};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec};
use crate::heed_codec::StrRefCodec;
use crate::update::del_add::{KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::helpers::merge_deladd_cbo_roaring_bitmaps;
use crate::{FieldId, Result};

/// Extracts the facet string and the documents ids where this facet string appear.
///
/// Returns a grenad reader with the list of extracted facet strings and
/// documents ids from the given chunk of docid facet string positions.
#[logging_timer::time]
pub fn extract_facet_string_docids<R: io::Read + io::Seek>(
    docid_fid_facet_string: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<BufReader<File>>> {
    puffin::profile_function!();

    let max_memory = indexer.max_memory_by_thread();

    let mut facet_string_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        merge_deladd_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut buffer = Vec::new();
    let mut cursor = docid_fid_facet_string.into_cursor()?;
    while let Some((key, deladd_original_value_bytes)) = cursor.move_on_next()? {
        let (field_id_bytes, bytes) = try_split_array_at(key).unwrap();
        let field_id = FieldId::from_be_bytes(field_id_bytes);

        let (document_id_bytes, normalized_value_bytes) =
            try_split_array_at::<_, 4>(bytes).unwrap();
        let document_id = u32::from_be_bytes(document_id_bytes);

        let normalized_value = str::from_utf8(normalized_value_bytes)?;
        let key = FacetGroupKey { field_id, level: 0, left_bound: normalized_value };
        let key_bytes = FacetGroupKeyCodec::<StrRefCodec>::bytes_encode(&key).unwrap();

        buffer.clear();
        let mut obkv = KvWriterDelAdd::new(&mut buffer);
        for (deladd_key, _) in KvReaderDelAdd::new(deladd_original_value_bytes).iter() {
            obkv.insert(deladd_key, document_id.to_ne_bytes())?;
        }
        obkv.finish()?;
        facet_string_docids_sorter.insert(&key_bytes, &buffer)?;
    }

    sorter_into_reader(facet_string_docids_sorter, indexer)
}
