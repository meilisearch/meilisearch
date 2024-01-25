use std::fs::File;
use std::io::{self, BufReader};

use concat_arrays::concat_arrays;
use serde_json::Value;

use super::helpers::{create_writer, writer_into_reader, GrenadParameters};
use crate::error::GeoError;
use crate::update::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};
use crate::update::index_documents::extract_finite_float_from_value;
use crate::{FieldId, InternalError, Result};

/// Extracts the geographical coordinates contained in each document under the `_geo` field.
///
/// Returns the generated grenad reader containing the docid as key associated to the (latitude, longitude)
#[logging_timer::time]
pub fn extract_geo_points<R: io::Read + io::Seek>(
    obkv_documents: grenad::Reader<R>,
    indexer: GrenadParameters,
    primary_key_id: FieldId,
    (lat_fid, lng_fid): (FieldId, FieldId),
) -> Result<grenad::Reader<BufReader<File>>> {
    puffin::profile_function!();

    let mut writer = create_writer(
        indexer.chunk_compression_type,
        indexer.chunk_compression_level,
        tempfile::tempfile()?,
    );

    let mut cursor = obkv_documents.into_cursor()?;
    while let Some((docid_bytes, value)) = cursor.move_on_next()? {
        let obkv = obkv::KvReader::new(value);
        // since we only need the primary key when we throw an error
        // we create this getter to lazily get it when needed
        let document_id = || -> Value {
            let document_id = obkv.get(primary_key_id).unwrap();
            serde_json::from_slice(document_id).unwrap()
        };

        // first we get the two fields
        match (obkv.get(lat_fid), obkv.get(lng_fid)) {
            (Some(lat), Some(lng)) => {
                let deladd_lat_obkv = KvReaderDelAdd::new(lat);
                let deladd_lng_obkv = KvReaderDelAdd::new(lng);

                // then we extract the values
                let del_lat_lng = deladd_lat_obkv
                    .get(DelAdd::Deletion)
                    .zip(deladd_lng_obkv.get(DelAdd::Deletion))
                    .map(|(lat, lng)| extract_lat_lng(lat, lng, document_id))
                    .transpose()?;
                let add_lat_lng = deladd_lat_obkv
                    .get(DelAdd::Addition)
                    .zip(deladd_lng_obkv.get(DelAdd::Addition))
                    .map(|(lat, lng)| extract_lat_lng(lat, lng, document_id))
                    .transpose()?;

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
            (None, Some(_)) => {
                return Err(GeoError::MissingLatitude { document_id: document_id() }.into())
            }
            (Some(_), None) => {
                return Err(GeoError::MissingLongitude { document_id: document_id() }.into())
            }
            (None, None) => (),
        }
    }

    writer_into_reader(writer)
}

/// Extract the finite floats lat and lng from two bytes slices.
fn extract_lat_lng(lat: &[u8], lng: &[u8], document_id: impl Fn() -> Value) -> Result<[f64; 2]> {
    let lat = extract_finite_float_from_value(
        serde_json::from_slice(lat).map_err(InternalError::SerdeJson)?,
    )
    .map_err(|lat| GeoError::BadLatitude { document_id: document_id(), value: lat })?;

    let lng = extract_finite_float_from_value(
        serde_json::from_slice(lng).map_err(InternalError::SerdeJson)?,
    )
    .map_err(|lng| GeoError::BadLongitude { document_id: document_id(), value: lng })?;

    Ok([lat, lng])
}
