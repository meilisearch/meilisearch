use std::fs::File;
use std::io;

use concat_arrays::concat_arrays;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::{FieldId, InternalError, Result, UserError};

/// Extracts the geographical coordinates contained in each document under the `_geo` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the (latitude, longitude)
pub fn extract_geo_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    primary_key_id: FieldId,
    (lat_fid, lng_fid): (FieldId, FieldId),
) -> Result<grenad::Reader<File>> {
    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::new(value);
        let (lat, lng) = obkv.get(lat_fid).zip(obkv.get(lng_fid)).ok_or_else(|| {
            let primary_key = obkv.get(primary_key_id).unwrap();
            let primary_key = serde_json::from_slice(primary_key).unwrap();
            UserError::InvalidGeoField { document_id: primary_key }
        })?;
        let (lat, lng): (f64, f64) = (
            serde_json::from_slice(lat).map_err(InternalError::SerdeJson)?,
            serde_json::from_slice(lng).map_err(InternalError::SerdeJson)?,
        );

        let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
        writer.insert(docid_bytes, bytes)?;
    }

    Ok(writer_into_reader(writer)?)
}
