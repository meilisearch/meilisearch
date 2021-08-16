use std::fs::File;
use std::iter::FromIterator;
use std::{io, str};

use roaring::RoaringBitmap;

use super::helpers::{
    create_sorter, keep_first_prefix_value_merge_roaring_bitmaps, sorter_into_reader,
    try_split_array_at, GrenadParameters,
};
use crate::heed_codec::facet::{encode_prefix_string, FacetStringLevelZeroCodec};
use crate::{FieldId, Result};

/// Extracts the facet string and the documents ids where this facet string appear.
///
/// Returns a grenad reader with the list of extracted facet strings and
/// documents ids from the given chunk of docid facet string positions.
pub fn extract_facet_string_docids<R: io::Read>(
    mut docid_fid_facet_string: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut facet_string_docids_sorter = create_sorter(
        keep_first_prefix_value_merge_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut key_buffer = Vec::new();
    let mut value_buffer = Vec::new();
    while let Some((key, original_value_bytes)) = docid_fid_facet_string.next()? {
        let (field_id_bytes, bytes) = try_split_array_at(key).unwrap();
        let field_id = FieldId::from_be_bytes(field_id_bytes);
        let (document_id_bytes, normalized_value_bytes) = try_split_array_at(bytes).unwrap();
        let document_id = u32::from_be_bytes(document_id_bytes);
        let original_value = str::from_utf8(original_value_bytes)?;

        key_buffer.clear();
        FacetStringLevelZeroCodec::serialize_into(
            field_id,
            str::from_utf8(normalized_value_bytes)?,
            &mut key_buffer,
        );

        value_buffer.clear();
        encode_prefix_string(original_value, &mut value_buffer)?;
        let bitmap = RoaringBitmap::from_iter(Some(document_id));
        bitmap.serialize_into(&mut value_buffer)?;

        facet_string_docids_sorter.insert(&key_buffer, &value_buffer)?;
    }

    sorter_into_reader(facet_string_docids_sorter, indexer)
}
