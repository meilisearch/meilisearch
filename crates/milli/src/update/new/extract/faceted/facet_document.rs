use serde_json::value::RawValue;
use serde_json::Value;

use super::extract_facets::DelAddFacetValue;
use crate::update::new::document::{DeltaDocument, DeltaValue, Document};
use crate::update::new::extract::geo::extract_geo_coordinates;
use crate::update::new::extract::{perm_json_p, BalancedCaches};
use crate::{FieldId, FieldsIdsMap, GlobalFieldsIdsMap, InternalError, Result, UserError};

pub fn extract_document_facets<'doc>(
    attributes_to_extract: &[&str],
    document: impl Document<'doc>,
    external_document_id: &str,
    field_id_map: &mut GlobalFieldsIdsMap,
    facet_fn: &mut impl FnMut(FieldId, perm_json_p::Depth, &Value) -> Result<()>,
) -> Result<()> {
    for res in document.iter_top_level_fields() {
        let (field_name, value) = res?;

        extract_document_facet(attributes_to_extract, field_id_map, facet_fn, field_name, value)?;
    }

    if attributes_to_extract.contains(&"_geo") {
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

fn extract_document_facet(
    attributes_to_extract: &[&str],
    field_id_map: &mut GlobalFieldsIdsMap<'_>,
    facet_fn: &mut impl FnMut(u16, perm_json_p::Depth, &Value) -> std::result::Result<(), crate::Error>,
    field_name: &str,
    value: &serde_json::value::RawValue,
) -> Result<()> {
    let mut tokenize_field = |name: &str, depth: perm_json_p::Depth, value: &Value| {
        match field_id_map.id_or_insert(name) {
            Some(field_id) => facet_fn(field_id, depth, value),
            None => Err(UserError::AttributeLimitReached.into()),
        }
    };
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
    };
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn extract_merged_document_facets<'doc, 'del_add_facet_value, 'cache>(
    attributes_to_extract: &[&str],
    document: DeltaDocument<'doc, 'doc, 'doc, FieldsIdsMap>,
    external_document_id: &str,
    del_add_facet_value: &mut DelAddFacetValue<'del_add_facet_value>,
    cached_sorter: &mut BalancedCaches<'cache>,
    field_id_map: &mut GlobalFieldsIdsMap,
    facet_fn_del: &mut impl FnMut(
        FieldId,
        perm_json_p::Depth,
        &Value,
        &mut DelAddFacetValue<'del_add_facet_value>,
        &mut BalancedCaches<'cache>,
    ) -> Result<()>,
    facet_fn_add: &mut impl FnMut(
        FieldId,
        perm_json_p::Depth,
        &Value,
        &mut DelAddFacetValue<'del_add_facet_value>,
        &mut BalancedCaches<'cache>,
    ) -> Result<()>,
) -> Result<()> {
    for res in document.delta_top_level_fields() {
        let (field_name, value) = res?;
        match value {
            DeltaValue::Deleted(value) => {
                extract_document_facet(
                    attributes_to_extract,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_del(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                    field_name,
                    value,
                )?;
            }
            DeltaValue::Inserted(value) => {
                extract_document_facet(
                    attributes_to_extract,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_add(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                    field_name,
                    value,
                )?;
            }
            DeltaValue::Modified(current, updated) => {
                extract_document_facet(
                    attributes_to_extract,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_del(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                    field_name,
                    current,
                )?;
                extract_document_facet(
                    attributes_to_extract,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_add(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                    field_name,
                    updated,
                )?;
            }
            DeltaValue::Unchanged(_) => {}
        }
    }

    if attributes_to_extract.contains(&"_geo") {
        match document.delta_geo_field()? {
            Some(DeltaValue::Deleted(deleted)) => {
                extract_geo_facet(
                    external_document_id,
                    deleted,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_del(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                )?;
            }
            Some(DeltaValue::Inserted(inserted)) => {
                extract_geo_facet(
                    external_document_id,
                    inserted,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_add(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                )?;
            }
            Some(DeltaValue::Modified(current, updated)) => {
                extract_geo_facet(
                    external_document_id,
                    current,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_del(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                )?;
                extract_geo_facet(
                    external_document_id,
                    updated,
                    field_id_map,
                    &mut |fid, depth, value| {
                        facet_fn_add(fid, depth, value, del_add_facet_value, cached_sorter)
                    },
                )?;
            }
            None | Some(DeltaValue::Unchanged(_)) => {}
        }
    }

    Ok(())
}

fn extract_geo_facet(
    external_document_id: &str,
    geo_value: &RawValue,
    field_id_map: &mut GlobalFieldsIdsMap<'_>,
    facet_fn: &mut impl FnMut(FieldId, perm_json_p::Depth, &Value) -> Result<()>,
) -> Result<()> {
    if let Some([lat, lng]) = extract_geo_coordinates(external_document_id, geo_value)? {
        let (lat_fid, lng_fid) = field_id_map
            .id_or_insert("_geo.lat")
            .zip(field_id_map.id_or_insert("_geo.lng"))
            .ok_or(UserError::AttributeLimitReached)?;

        facet_fn(lat_fid, perm_json_p::Depth::OnBaseKey, &lat.into())?;
        facet_fn(lng_fid, perm_json_p::Depth::OnBaseKey, &lng.into())?;
    };
    Ok(())
}
