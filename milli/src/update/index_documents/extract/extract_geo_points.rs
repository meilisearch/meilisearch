use std::fs::File;
use std::io;
use std::result::Result as StdResult;

use concat_arrays::concat_arrays;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::error::GeoError;
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
        // since we only needs the primary key when we throw an error we create this getter to
        // lazily get it when needed
        let primary_key = || -> Value {
            let primary_key = obkv.get(primary_key_id).unwrap();
            serde_json::from_slice(primary_key).unwrap()
        };

        // first we get the two fields
        let lat = obkv.get(lat_fid).ok_or_else(|| -> UserError {
            GeoError::MissingLatitude { document_id: primary_key() }.into()
        })?;
        let lng = obkv.get(lng_fid).ok_or_else(|| -> UserError {
            GeoError::MissingLongitude { document_id: primary_key() }.into()
        })?;

        // then we extract the values
        let lat = extract_value(serde_json::from_slice(lat).map_err(InternalError::SerdeJson)?)
            .map_err(|lat| -> UserError {
                GeoError::BadLatitude { document_id: primary_key(), value: lat }.into()
            })?;

        let lng = extract_value(serde_json::from_slice(lng).map_err(InternalError::SerdeJson)?)
            .map_err(|lng| -> UserError {
                GeoError::BadLongitude { document_id: primary_key(), value: lng }.into()
            })?;

        let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
        writer.insert(docid_bytes, bytes)?;
    }

    Ok(writer_into_reader(writer)?)
}

fn extract_value(value: Value) -> StdResult<f64, Value> {
    match value {
        Value::Number(ref n) => n.as_f64().ok_or(value),
        Value::String(ref s) => s.parse::<f64>().map_err(|_| value),
        value => Err(value),
    }
}
