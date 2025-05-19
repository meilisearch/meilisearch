use std::collections::HashSet;

use serde_json::Value;

use crate::attribute_patterns::PatternMatch;
use crate::fields_ids_map::metadata::Metadata;
use crate::update::new::document::Document;
use crate::update::new::extract::geo::extract_geo_coordinates;
use crate::update::new::extract::perm_json_p;
use crate::{
    FieldId, FilterableAttributesRule, GlobalFieldsIdsMap, InternalError, Result, UserError,
};

use crate::filterable_attributes_rules::match_faceted_field;

#[allow(clippy::too_many_arguments)]
pub fn extract_document_facets<'doc>(
    document: impl Document<'doc>,
    external_document_id: &str,
    field_id_map: &mut GlobalFieldsIdsMap,
    filterable_attributes: &[FilterableAttributesRule],
    sortable_fields: &HashSet<String>,
    asc_desc_fields: &HashSet<String>,
    distinct_field: &Option<String>,
    is_geo_enabled: bool,
    facet_fn: &mut impl FnMut(FieldId, Metadata, perm_json_p::Depth, &Value) -> Result<()>,
) -> Result<()> {
    // return the match result for the given field name.
    let match_field = |field_name: &str| -> PatternMatch {
        match_faceted_field(
            field_name,
            filterable_attributes,
            sortable_fields,
            asc_desc_fields,
            distinct_field,
        )
    };
    let no_of_existing_fields = field_id_map.len();

    // extract the field if it is faceted (facet searchable, filterable, sortable)
    let mut extract_field = |name: &str, depth: perm_json_p::Depth, value: &Value| -> Result<()> {
        match field_id_map.id_with_metadata_or_insert(name) {
            Some((field_id, meta)) => {
                facet_fn(field_id, meta, depth, value)?;

                Ok(())
            }
            None => Err(UserError::AttributeLimitReached {
                document_id: None,
                new_field_count: 1,
                number_of_existing_field: no_of_existing_fields
            }.into()),
        }
    };

    for res in document.iter_top_level_fields() {
        let (field_name, value) = res?;
        let selection = match_field(field_name);

        // extract the field if it matches a pattern and if it is faceted (facet searchable, filterable, sortable)
        let mut match_and_extract = |name: &str, depth: perm_json_p::Depth, value: &Value| {
            let selection = match_field(name);
            if selection == PatternMatch::Match {
                extract_field(name, depth, value)?;
            }

            Ok(selection)
        };

        if selection != PatternMatch::NoMatch {
            // parse json.
            match serde_json::value::to_value(value).map_err(InternalError::SerdeJson)? {
                Value::Object(object) => {
                    perm_json_p::seek_leaf_values_in_object(
                        &object,
                        field_name,
                        perm_json_p::Depth::OnBaseKey,
                        &mut match_and_extract,
                    )?;

                    if selection == PatternMatch::Match {
                        extract_field(
                            field_name,
                            perm_json_p::Depth::OnBaseKey,
                            &Value::Object(object),
                        )?;
                    }
                }
                Value::Array(array) => {
                    perm_json_p::seek_leaf_values_in_array(
                        &array,
                        field_name,
                        perm_json_p::Depth::OnBaseKey,
                        &mut match_and_extract,
                    )?;

                    if selection == PatternMatch::Match {
                        extract_field(
                            field_name,
                            perm_json_p::Depth::OnBaseKey,
                            &Value::Array(array),
                        )?;
                    }
                }
                value => extract_field(field_name, perm_json_p::Depth::OnBaseKey, &value)?,
            }
        }
    }

    if is_geo_enabled {
        if let Some(geo_value) = document.geo_field()? {
            if let Some([lat, lng]) = extract_geo_coordinates(external_document_id, geo_value)? {
                let ((lat_fid, lat_meta), (lng_fid, lng_meta)) = field_id_map
                    .id_with_metadata_or_insert("_geo.lat")
                    .zip(field_id_map.id_with_metadata_or_insert("_geo.lng"))
                    .ok_or(UserError::AttributeLimitReached {
                        document_id: Some(external_document_id.to_string()),
                        new_field_count: 2,
                        number_of_existing_field: field_id_map.len()
                    })?;

                facet_fn(lat_fid, lat_meta, perm_json_p::Depth::OnBaseKey, &lat.into())?;
                facet_fn(lng_fid, lng_meta, perm_json_p::Depth::OnBaseKey, &lng.into())?;
            }
        }
    }

    Ok(())
}
