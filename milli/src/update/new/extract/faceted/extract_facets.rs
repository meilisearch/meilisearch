use std::collections::HashSet;

use heed::RoTxn;
use serde_json::Value;

use super::FacetedExtractor;
use crate::facet::value_encoding::f64_into_bytes;
use crate::{normalize_facet, FieldId, Index, Result, MAX_FACET_VALUE_LENGTH};

pub struct FieldIdFacetNumberDocidsExtractor;

impl FacetedExtractor for FieldIdFacetNumberDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        let number = value.as_number()?;
        let n = number.as_f64()?;
        let ordered = f64_into_bytes(n)?;

        // fid - level - orderedf64 - orignalf64
        output.extend_from_slice(&field_id.to_be_bytes());
        output.push(1); // level 0
        output.extend_from_slice(&ordered);
        output.extend_from_slice(&n.to_be_bytes());

        Some(&*output)
    }
}

pub struct FieldIdFacetStringDocidsExtractor;

impl FacetedExtractor for FieldIdFacetStringDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        let string = value.as_str()?;
        let normalize = normalize_facet(string);
        let truncated = truncate_str(&normalize);

        // fid - level - normalized string
        output.extend_from_slice(&field_id.to_be_bytes());
        output.push(1); // level 0
        output.extend_from_slice(truncated.as_bytes());

        Some(&*output)
    }
}

/// Truncates a string to the biggest valid LMDB key size.
fn truncate_str(s: &str) -> &str {
    let index = s
        .char_indices()
        .map(|(idx, _)| idx)
        .chain(std::iter::once(s.len()))
        .take_while(|idx| idx <= &MAX_FACET_VALUE_LENGTH)
        .last();

    &s[..index.unwrap_or(0)]
}

pub struct FieldIdFacetIsNullDocidsExtractor;

impl FacetedExtractor for FieldIdFacetIsNullDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        if value.is_null() {
            output.extend_from_slice(&field_id.to_be_bytes());
            Some(&*output)
        } else {
            None
        }
    }
}

pub struct FieldIdFacetExistsDocidsExtractor;

impl FacetedExtractor for FieldIdFacetExistsDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        _value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        output.extend_from_slice(&field_id.to_be_bytes());
        Some(&*output)
    }
}

pub struct FieldIdFacetIsEmptyDocidsExtractor;

impl FacetedExtractor for FieldIdFacetIsEmptyDocidsExtractor {
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }

    fn build_key<'b>(
        field_id: FieldId,
        value: &Value,
        output: &'b mut Vec<u8>,
    ) -> Option<&'b [u8]> {
        let is_empty = match value {
            Value::Null | Value::Bool(_) | Value::Number(_) => false,
            Value::String(s) => s.is_empty(),
            Value::Array(a) => a.is_empty(),
            Value::Object(o) => o.is_empty(),
        };

        if is_empty {
            output.extend_from_slice(&field_id.to_be_bytes());
            Some(&*output)
        } else {
            None
        }
    }
}
