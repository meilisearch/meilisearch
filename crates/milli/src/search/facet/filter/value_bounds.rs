use std::ops::Bound;

use filter_parser::{Condition, TokenLike as _};
use roaring::RoaringBitmap;

use crate::error::Error;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};

pub enum ValueBounds {
    Range { normalized: (Bound<String>, Bound<String>), number: Option<(Bound<f64>, Bound<f64>)> },
    FieldIsEmpty,
    FieldIsNull,
    FieldExists,
    Equal { normalized: String, number: Option<f64> },
    NotEqual { normalized: String, number: Option<f64> },
    Contains { normalized: String },
    StartsWith { normalized: String },
}

impl ValueBounds {
    pub fn new(operator: &Condition) -> ValueBounds {
        use std::ops::Bound::*;
        match operator {
            Condition::GreaterThan(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Excluded(number), Included(f64::MAX)));
                let left_normalized_value = crate::normalize_facet(val.fragment());
                let str_bounds = (Excluded(left_normalized_value), Unbounded);
                ValueBounds::Range { normalized: str_bounds, number: number_bounds }
            }
            Condition::GreaterThanOrEqual(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Included(number), Included(f64::MAX)));
                let left_normalized_value = crate::normalize_facet(val.fragment());
                let str_bounds = (Included(left_normalized_value), Unbounded);
                ValueBounds::Range { normalized: str_bounds, number: number_bounds }
            }
            Condition::LowerThan(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Included(f64::MIN), Excluded(number)));
                let left_normalized_value = crate::normalize_facet(val.fragment());
                let str_bounds = (Unbounded, Excluded(left_normalized_value));
                ValueBounds::Range { normalized: str_bounds, number: number_bounds }
            }
            Condition::LowerThanOrEqual(val) => {
                let number = val.parse_finite_float().ok();
                let number_bounds = number.map(|number| (Included(f64::MIN), Included(number)));
                let left_normalized_value = crate::normalize_facet(val.fragment());
                let str_bounds = (Unbounded, Included(left_normalized_value));
                ValueBounds::Range { normalized: str_bounds, number: number_bounds }
            }
            Condition::Between { from, to } => {
                let from_number = from.parse_finite_float().ok();
                let to_number = to.parse_finite_float().ok();

                let number_bounds =
                    from_number.zip(to_number).map(|(from, to)| (Included(from), Included(to)));
                let left_normalized_value = crate::normalize_facet(from.fragment());
                let right_normalized_value = crate::normalize_facet(to.fragment());
                let str_bounds =
                    (Included(left_normalized_value), Included(right_normalized_value));
                ValueBounds::Range { normalized: str_bounds, number: number_bounds }
            }
            Condition::Null => ValueBounds::FieldIsNull,
            Condition::Empty => ValueBounds::FieldIsEmpty,
            Condition::Exists => ValueBounds::FieldExists,
            Condition::Equal(val) => {
                let normalized = crate::normalize_facet(val.fragment());
                let number = val.parse_finite_float().ok();
                ValueBounds::Equal { normalized, number }
            }
            Condition::NotEqual(val) => {
                let normalized = crate::normalize_facet(val.fragment());
                let number = val.parse_finite_float().ok();
                ValueBounds::NotEqual { normalized, number }
            }
            Condition::Contains { keyword: _, word } => {
                let normalized = crate::normalize_facet(word.fragment());
                ValueBounds::Contains { normalized }
            }
            Condition::StartsWith { keyword: _, word } => {
                let normalized = crate::normalize_facet(word.fragment());
                ValueBounds::StartsWith { normalized }
            }
        }
    }
}

pub fn to_str_bounds(bounds: &(Bound<String>, Bound<String>)) -> (Bound<&str>, Bound<&str>) {
    (bounds.0.as_ref().map(|s| s.as_str()), bounds.1.as_ref().map(|s| s.as_str()))
}

pub fn evaluate_equal(
    rtxn: &heed::RoTxn<'_>,
    field_id: u16,
    numbers_db: heed::Database<
        FacetGroupKeyCodec<crate::heed_codec::facet::OrderedF64Codec>,
        FacetGroupValueCodec,
    >,
    strings_db: heed::Database<
        FacetGroupKeyCodec<crate::heed_codec::StrRefCodec>,
        FacetGroupValueCodec,
    >,
    normalized: String,
    number: Option<f64>,
) -> Result<RoaringBitmap, Error> {
    let string_docids = strings_db
        .get(rtxn, &FacetGroupKey { field_id, level: 0, left_bound: &normalized })?
        .map(|v| v.bitmap)
        .unwrap_or_default();
    let number_docids = match number {
        Some(n) => numbers_db
            .get(rtxn, &FacetGroupKey { field_id, level: 0, left_bound: n })?
            .map(|v| v.bitmap)
            .unwrap_or_default(),
        None => RoaringBitmap::new(),
    };
    Ok(string_docids | number_docids)
}
