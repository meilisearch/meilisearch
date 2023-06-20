use std::convert::TryFrom;
use std::fs::File;
use std::io;

use bytemuck::cast_slice;
use serde_json::from_slice;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::{FieldId, InternalError, Result, VectorOrArrayOfVectors};

/// Extracts the embedding vector contained in each document under the `_vectors` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the Vec<f32>
#[logging_timer::time]
pub fn extract_vector_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    vectors_fid: FieldId,
) -> Result<grenad::Reader<File>> {
    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::new(value);

        // first we retrieve the _vectors field
        if let Some(vectors) = obkv.get(vectors_fid) {
            // extract the vectors
            // TODO return a user error before unwrapping
            let vectors = from_slice(vectors)
                .map_err(InternalError::SerdeJson)
                .map(VectorOrArrayOfVectors::into_array_of_vectors)
                .unwrap();

            for (i, vector) in vectors.into_iter().enumerate() {
                match u16::try_from(i) {
                    Ok(i) => {
                        let mut key = docid_bytes.to_vec();
                        key.extend_from_slice(&i.to_ne_bytes());
                        let bytes = cast_slice(&vector);
                        writer.insert(key, bytes)?;
                    }
                    Err(_) => continue,
                }
            }
        }
        // else => the `_vectors` object was `null`, there is nothing to do
    }

    writer_into_reader(writer)
}
