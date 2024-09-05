use serde_json::Value;

use crate::update::new::extract::perm_json_p;
use crate::update::new::KvReaderFieldId;
use crate::{FieldId, GlobalFieldsIdsMap, InternalError, Result, UserError};

pub fn extract_document_facets(
    attributes_to_extract: &[&str],
    obkv: &KvReaderFieldId,
    field_id_map: &mut GlobalFieldsIdsMap,
    facet_fn: &mut impl FnMut(FieldId, &Value) -> Result<()>,
) -> Result<()> {
    let mut field_name = String::new();
    for (field_id, field_bytes) in obkv {
        let Some(field_name) = field_id_map.name(field_id).map(|s| {
            field_name.clear();
            field_name.push_str(s);
            &field_name
        }) else {
            unreachable!("field id not found in field id map");
        };

        let mut tokenize_field = |name: &str, value: &Value| match field_id_map.id_or_insert(name) {
            Some(field_id) => facet_fn(field_id, value),
            None => Err(UserError::AttributeLimitReached.into()),
        };

        // if the current field is searchable or contains a searchable attribute
        if perm_json_p::select_field(field_name, Some(attributes_to_extract), &[]) {
            // parse json.
            match serde_json::from_slice(field_bytes).map_err(InternalError::SerdeJson)? {
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
