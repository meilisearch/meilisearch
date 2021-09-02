use std::fs::File;
use std::io;

use concat_arrays::concat_arrays;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::{FieldId, InternalError, Result, UserError};

/// Extracts the geographical coordinates contained in each document under the `_geo` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the (latitude, longitude)
pub fn extract_geo_points<R: io::Read>(
    mut obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    primary_key_id: FieldId,
    geo_field_id: FieldId,
) -> Result<grenad::Reader<File>> {
    let mut writer = tempfile::tempfile().and_then(|file| {
        create_writer(indexer.chunk_compression_type, indexer.chunk_compression_level, file)
    })?;

    while let Some((docid_bytes, value)) = obkv_documents.next()? {
        let obkv = obkv::KvReader::new(value);
        let point = match obkv.get(geo_field_id) {
            Some(point) => point,
            None => continue,
        };
        let point: Value = serde_json::from_slice(point).map_err(InternalError::SerdeJson)?;

        if let Some((lat, lng)) = point["lat"].as_f64().zip(point["lng"].as_f64()) {
            // this will create an array of 16 bytes (two 8 bytes floats)
            let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
            writer.insert(docid_bytes, bytes)?;
        } else {
            let primary_key = obkv.get(primary_key_id).unwrap(); // TODO: TAMO: is this valid?
            let primary_key =
                serde_json::from_slice(primary_key).map_err(InternalError::SerdeJson)?;
            Err(UserError::InvalidGeoField { document_id: primary_key, object: point })?
        }
    }

    Ok(writer_into_reader(writer)?)
}
