use serde_json::Value;

use crate::update::new::document::Document;
use crate::update::new::extract::perm_json_p;
use crate::{FieldId, GlobalFieldsIdsMap, InternalError, Result, UserError};

pub fn extract_document_facets<'doc>(
    attributes_to_extract: &[&str],
    document: impl Document<'doc>,
    field_id_map: &mut GlobalFieldsIdsMap,
    facet_fn: &mut impl FnMut(FieldId, &Value) -> Result<()>,
) -> Result<()> {
    let geo = document.geo_field().transpose().map(|res|  res.map(|rval| ("_geo", rval)));
    for res in document.iter_top_level_fields().chain(geo) {
        let (field_name, value) = res?;

        let mut tokenize_field = |name: &str, value: &Value| match field_id_map.id_or_insert(name) {
            Some(field_id) => facet_fn(field_id, value),
            None => Err(UserError::AttributeLimitReached.into()),
        };

        // if the current field is searchable or contains a searchable attribute
        if perm_json_p::select_field(field_name, Some(attributes_to_extract), &[]) {
            // parse json.
            match serde_json::value::to_value(value).map_err(InternalError::SerdeJson)? {
                Value::Object(object) => perm_json_p::seek_leaf_values_in_object(
                    &object,
                    Some(attributes_to_extract),
                    &[], // skip no attributes
                    field_name,
                    &mut tokenize_field,
                )?,
                Value::Array(array) => perm_json_p::seek_leaf_values_in_array(
                    &array,
                    Some(attributes_to_extract),
                    &[], // skip no attributes
                    field_name,
                    &mut tokenize_field,
                )?,
                value => tokenize_field(field_name, &value)?,
            }
        }
    }

    Ok(())
}
