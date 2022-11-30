use std::fs::File;
use std::io;

use heed::BytesEncode;

use super::helpers::{create_sorter, sorter_into_reader, try_split_array_at, GrenadParameters};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec};
use crate::heed_codec::StrRefCodec;
use crate::update::index_documents::merge_cbo_roaring_bitmaps;
use crate::{FieldId, Result, MAX_FACET_VALUE_LENGTH};

/// Extracts the facet string and the documents ids where this facet string appear.
///
/// Returns a grenad reader with the list of extracted facet strings and
/// documents ids from the given chunk of docid facet string positions.
#[logging_timer::time]
pub fn extract_facet_string_docids<R: io::Read + io::Seek>(
    docid_fid_facet_string: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut facet_string_docids_sorter = create_sorter(
        grenad::SortAlgorithm::Stable,
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut cursor = docid_fid_facet_string.into_cursor()?;
    while let Some((key, _original_value_bytes)) = cursor.move_on_next()? {
        let (field_id_bytes, bytes) = try_split_array_at(key).unwrap();
        let field_id = FieldId::from_be_bytes(field_id_bytes);

        let (document_id_bytes, normalized_value_bytes) =
            try_split_array_at::<_, 4>(bytes).unwrap();
        let document_id = u32::from_be_bytes(document_id_bytes);

        let mut normalised_value = std::str::from_utf8(normalized_value_bytes)?;

        let normalised_truncated_value: String;
        if normalised_value.len() > MAX_FACET_VALUE_LENGTH {
            normalised_truncated_value = normalised_value
                .char_indices()
                .take_while(|(idx, _)| idx + 4 < MAX_FACET_VALUE_LENGTH)
                .map(|(_, c)| c)
                .collect();
            normalised_value = normalised_truncated_value.as_str();
        }
        let key = FacetGroupKey { field_id, level: 0, left_bound: normalised_value };
        let key_bytes = FacetGroupKeyCodec::<StrRefCodec>::bytes_encode(&key).unwrap();
        // document id is encoded in native-endian because of the CBO roaring bitmap codec
        facet_string_docids_sorter.insert(&key_bytes, document_id.to_ne_bytes())?;
    }

    sorter_into_reader(facet_string_docids_sorter, indexer)
}
