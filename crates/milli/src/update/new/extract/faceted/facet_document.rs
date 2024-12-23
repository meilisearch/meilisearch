use serde_json::Value;

use crate::constants::RESERVED_GEO_FIELD_NAME;
use crate::update::new::document::Document;
use crate::update::new::extract::geo::extract_geo_coordinates;
use crate::update::new::extract::perm_json_p;
use crate::{FieldId, GlobalFieldsIdsMap, InternalError, Result, UserError};

pub fn extract_document_facets<'doc>(
    attributes_to_extract: &[&str],
    document: impl Document<'doc>,
    external_document_id: &str,
    field_id_map: &mut GlobalFieldsIdsMap,
    facet_fn: &mut impl FnMut(FieldId, perm_json_p::Depth, &Value) -> Result<()>,
) -> Result<()> {
    for res in document.iter_top_level_fields() {
        let (field_name, value) = res?;

        let mut tokenize_field =
            |name: &str, depth: perm_json_p::Depth, value: &Value| match field_id_map
                .id_or_insert(name)
            {
                Some(field_id) => facet_fn(field_id, depth, value),
                None => Err(UserError::AttributeLimitReached.into()),
            };

        // if the current field is searchable or contains a searchable attribute
        let selection = perm_json_p::select_field(field_name, Some(attributes_to_extract), &[]);
        if selection != perm_json_p::Selection::Skip {
            // parse json.
            match serde_json::value::to_value(value).map_err(InternalError::SerdeJson)? {
                Value::Object(object) => {
                    perm_json_p::seek_leaf_values_in_object(
                        &object,
                        Some(attributes_to_extract),
                        &[], // skip no attributes
                        field_name,
                        perm_json_p::Depth::OnBaseKey,
                        &mut tokenize_field,
                    )?;

                    if selection == perm_json_p::Selection::Select {
                        tokenize_field(
                            field_name,
                            perm_json_p::Depth::OnBaseKey,
                            &Value::Object(object),
                        )?;
                    }
                }
                Value::Array(array) => {
                    perm_json_p::seek_leaf_values_in_array(
                        &array,
                        Some(attributes_to_extract),
                        &[], // skip no attributes
                        field_name,
                        perm_json_p::Depth::OnBaseKey,
                        &mut tokenize_field,
                    )?;

                    if selection == perm_json_p::Selection::Select {
                        tokenize_field(
                            field_name,
                            perm_json_p::Depth::OnBaseKey,
                            &Value::Array(array),
                        )?;
                    }
                }
                value => tokenize_field(field_name, perm_json_p::Depth::OnBaseKey, &value)?,
            }
        }
    }

    if attributes_to_extract.contains(&RESERVED_GEO_FIELD_NAME) {
        if let Some(geo_value) = document.geo_field()? {
            if let Some([lat, lng]) = extract_geo_coordinates(external_document_id, geo_value)? {
                let (lat_fid, lng_fid) = field_id_map
                    .id_or_insert("_geo.lat")
                    .zip(field_id_map.id_or_insert("_geo.lng"))
                    .ok_or(UserError::AttributeLimitReached)?;

                facet_fn(lat_fid, perm_json_p::Depth::OnBaseKey, &lat.into())?;
                facet_fn(lng_fid, perm_json_p::Depth::OnBaseKey, &lng.into())?;
            }
        }
    }

    Ok(())
}
