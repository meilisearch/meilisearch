use std::fs::File;
use std::io;

use heed::{BytesDecode, BytesEncode};

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_reader, GrenadParameters,
};
use crate::heed_codec::facet::{FacetLevelValueF64Codec, FieldDocIdFacetF64Codec};
use crate::Result;

/// Extracts the facet number and the documents ids where this facet number appear.
///
/// Returns a grenad reader with the list of extracted facet numbers and
/// documents ids from the given chunk of docid facet number positions.
pub fn extract_facet_number_docids<R: io::Read>(
    mut docid_fid_facet_number: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut facet_number_docids_sorter = create_sorter(
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    while let Some((key_bytes, _)) = docid_fid_facet_number.next()? {
        let (field_id, document_id, number) =
            FieldDocIdFacetF64Codec::bytes_decode(key_bytes).unwrap();

        let key = (field_id, 0, number, number);
        let key_bytes = FacetLevelValueF64Codec::bytes_encode(&key).unwrap();

        facet_number_docids_sorter.insert(key_bytes, document_id.to_ne_bytes())?;
    }

    sorter_into_reader(facet_number_docids_sorter, indexer)
}
