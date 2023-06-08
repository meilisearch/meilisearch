use std::fs::File;
use std::io;

use bytemuck::cast_slice;
use serde_json::from_slice;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::{FieldId, InternalError, Result};

/// Extracts the embedding vector contained in each document under the `_vector` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the Vec<f32>
#[logging_timer::time]
pub fn extract_vector_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    vector_fid: FieldId,
) -> Result<grenad::Reader<File>> {
    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::new(value);

        // first we get the _vector field
        if let Some(vector) = obkv.get(vector_fid) {
            // try to extract the vector
            let vector: Vec<f32> = from_slice(vector).map_err(InternalError::SerdeJson).unwrap();
            let bytes = cast_slice(&vector);
            writer.insert(docid_bytes, bytes)?;
        }
        // else => the _vector object was `null`, there is nothing to do
    }

    writer_into_reader(writer)
}
