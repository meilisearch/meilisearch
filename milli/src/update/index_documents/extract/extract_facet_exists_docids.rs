use std::fs::File;
use std::io;

use heed::{BytesDecode, BytesEncode};

use super::helpers::{
    create_sorter, merge_cbo_roaring_bitmaps, sorter_into_reader, GrenadParameters,
};
use crate::heed_codec::facet::{FieldIdCodec, FieldIdDocIdCodec};
use crate::Result;

/// Extracts the documents ids where this field appears.
///
/// Returns a grenad reader whose key is the field id encoded
/// with `FieldIdCodec` and the value is a document_id (u32)
/// encoded as native-endian bytes.
#[logging_timer::time]
pub fn extract_facet_exists_docids<R: io::Read + io::Seek>(
    docid_fid_facet_number: grenad::Reader<R>,
    indexer: GrenadParameters,
) -> Result<grenad::Reader<File>> {
    let max_memory = indexer.max_memory_by_thread();

    let mut facet_exists_docids_sorter = create_sorter(
        merge_cbo_roaring_bitmaps,
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        indexer.max_nb_chunks,
        max_memory,
    );

    let mut cursor = docid_fid_facet_number.into_cursor()?;
    while let Some((key_bytes, _)) = cursor.move_on_next()? {
        let (field_id, document_id) = FieldIdDocIdCodec::bytes_decode(key_bytes).unwrap();
        let key_bytes = FieldIdCodec::bytes_encode(&field_id).unwrap();
        facet_exists_docids_sorter.insert(key_bytes, document_id.to_ne_bytes())?;
    }

    sorter_into_reader(facet_exists_docids_sorter, indexer)
}
