use std::convert::TryFrom;
use std::fs::File;
use std::io;

use bytemuck::cast_slice;
use serde_json::{from_slice, Value};

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::error::UserError;
use crate::{FieldId, InternalError, Result, VectorOrArrayOfVectors};

/// Extracts the embedding vector contained in each document under the `_vectors` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the Vec<f32>
#[logging_timer::time]
pub fn extract_vector_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    primary_key_id: FieldId,
    vectors_fid: FieldId,
) -> Result<grenad::Reader<File>> {
    puffin::profile_function!();

    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::new(value);

        // since we only needs the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let document_id = || -> Value {
            let document_id = obkv.get(primary_key_id).unwrap();
            serde_json::from_slice(document_id).unwrap()
        };

        // first we retrieve the _vectors field
        if let Some(vectors) = obkv.get(vectors_fid) {
            // extract the vectors
            let vectors = match from_slice(vectors) {
                Ok(vectors) => VectorOrArrayOfVectors::into_array_of_vectors(vectors),
                Err(_) => {
                    return Err(UserError::InvalidVectorsType {
                        document_id: document_id(),
                        value: from_slice(vectors).map_err(InternalError::SerdeJson)?,
                    }
                    .into())
                }
            };

            for (i, vector) in vectors.into_iter().enumerate().take(u16::MAX as usize) {
                let index = u16::try_from(i).unwrap();
                let mut key = docid_bytes.to_vec();
                key.extend_from_slice(&index.to_be_bytes());
                let bytes = cast_slice(&vector);
                writer.insert(key, bytes)?;
            }
        }
        // else => the `_vectors` object was `null`, there is nothing to do
    }

    writer_into_reader(writer)
}
