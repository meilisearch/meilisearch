use std::fs::File;
use std::io;

use concat_arrays::concat_arrays;
use log::warn;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::{FieldId, InternalError, Result};

/// Extracts the geographical coordinates contained in each document under the `_geo` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the (latitude, longitude)
pub fn extract_geo_points<R: io::Read>(
    mut obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    geo_field_id: Option<FieldId>, // faire un grenad vide
) -> Result<grenad::Reader<File>> {
    let mut writer = tempfile::tempfile().and_then(|file| {
        create_writer(indexer.chunk_compression_type, indexer.chunk_compression_level, file)
    })?;

    // we never encountered any documents with a `_geo` field. We can skip entirely this step
    if geo_field_id.is_none() {
        return Ok(writer_into_reader(writer)?);
    }
    let geo_field_id = geo_field_id.unwrap();

    while let Some((docid_bytes, value)) = obkv_documents.next()? {
        let obkv = obkv::KvReader::new(value);
        let point = obkv.get(geo_field_id).unwrap(); // TODO: TAMO where should we handle this error?
        let point: Value = serde_json::from_slice(point).map_err(InternalError::SerdeJson)?;

        if let Some((lat, lng)) = point["lat"].as_f64().zip(point["lng"].as_f64()) {
            // this will create an array of 16 bytes (two 8 bytes floats)
            let bytes: [u8; 16] = concat_arrays![lat.to_le_bytes(), lng.to_le_bytes()];
            writer.insert(docid_bytes, bytes)?;
        } else {
            // TAMO: improve the warn
            warn!("Malformed `_geo` field");
            continue;
        }
    }

    Ok(writer_into_reader(writer)?)
}
