use std::fs::File;
use std::io::{self, BufReader};

use concat_arrays::concat_arrays;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::error::GeoError;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::extract_finite_float_from_value;
use crate::update::settings::{InnerIndexSettings, InnerIndexSettingsDiff};
use crate::{FieldId, InternalError, Result};

/// Extracts the geographical coordinates contained in each document under the `_geo` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the (latitude, longitude)
#[tracing::instrument(level = "trace", skip_all, target = "indexing::extract")]
pub fn extract_geo_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    primary_key_id: FieldId,
    settings_diff: &InnerIndexSettingsDiff,
) -> Result<grenad::Reader<BufReader<File>>> {
    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::from_slice(value);
        // since we only need the primary key when we throw an error
        // we create this getter to lazily get it when needed
        let document_id = || -> Value {
            let reader = KvReaderDelAdd::from_slice(obkv.get(primary_key_id).unwrap());
            let document_id =
                reader.get(DelAdd::Deletion).or(reader.get(DelAdd::Addition)).unwrap();
            serde_json::from_slice(document_id).unwrap()
        };

        // extract old version
        let del_lat_lng = extract_lat_lng(obkv, &settings_diff.old, DelAdd::Deletion, document_id)?;
        // extract new version
        let add_lat_lng = extract_lat_lng(obkv, &settings_diff.new, DelAdd::Addition, document_id)?;

        if del_lat_lng != add_lat_lng {
            let mut obkv = KvWriterDelAdd::memory();
            if let Some([lat, lng]) = del_lat_lng {
                #[allow(clippy::drop_non_drop)]
                let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
                obkv.insert(DelAdd::Deletion, bytes)?;
            }
            if let Some([lat, lng]) = add_lat_lng {
                #[allow(clippy::drop_non_drop)]
                let bytes: [u8; 16] = concat_arrays![lat.to_ne_bytes(), lng.to_ne_bytes()];
                obkv.insert(DelAdd::Addition, bytes)?;
            }
            let bytes = obkv.into_inner()?;
            writer.insert(docid_bytes, bytes)?;
        }
    }

    writer_into_reader(writer)
}

/// Extract the finite floats lat and lng from two bytes slices.
fn extract_lat_lng(
    document: &obkv::KvReader<FieldId>,
    settings: &InnerIndexSettings,
    deladd: DelAdd,
    document_id: impl Fn() -> Value,
) -> Result<Option<[f64; 2]>> {
    match settings.geo_fields_ids {
        Some((lat_fid, lng_fid)) => {
            let lat =
                document.get(lat_fid).map(KvReaderDelAdd::from_slice).and_then(|r| r.get(deladd));
            let lng =
                document.get(lng_fid).map(KvReaderDelAdd::from_slice).and_then(|r| r.get(deladd));
            let (lat, lng) = match (lat, lng) {
                (Some(lat), Some(lng)) => (lat, lng),
                (Some(_), None) => {
                    return Err(GeoError::MissingLatitude { document_id: document_id() }.into())
                }
                (None, Some(_)) => {
                    return Err(GeoError::MissingLongitude { document_id: document_id() }.into())
                }
                (None, None) => return Ok(None),
            };
            let lat = extract_finite_float_from_value(
                serde_json::from_slice(lat).map_err(InternalError::SerdeJson)?,
            )
            .map_err(|lat| GeoError::BadLatitude { document_id: document_id(), value: lat })?;

            let lng = extract_finite_float_from_value(
                serde_json::from_slice(lng).map_err(InternalError::SerdeJson)?,
            )
            .map_err(|lng| GeoError::BadLongitude { document_id: document_id(), value: lng })?;
            Ok(Some([lat, lng]))
        }
        None => Ok(None),
    }
}
