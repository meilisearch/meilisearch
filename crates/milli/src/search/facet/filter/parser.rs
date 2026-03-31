use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::{Debug, Display};
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use either::Either;
use filter_parser::IndexFilterCondition;
pub use filter_parser::{Condition, Error as FPError, FilterCondition, Token};
use heed::types::LazyDecode;
use heed::BytesEncode;
use memchr::memmem::Finder;
use roaring::{MultiOps, RoaringBitmap};
use serde_json::Value;

use super::facet_range_search;
use crate::constants::{
    RESERVED_GEOJSON_FIELD_NAME, RESERVED_GEO_FIELD_NAME, RESERVED_VECTORS_FIELD_NAME,
};
use crate::error::{Error, UserError};
use crate::filterable_attributes_rules::{filtered_matching_patterns, matching_features};
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec, FacetGroupValueCodec};
use crate::index::db_name::FACET_ID_STRING_DOCIDS;
use crate::search::facet::facet_range_search::find_docids_of_facet_within_bounds;
use crate::search::facet::filter::FilterError;
use crate::{
    distance_between_two_points, lat_lng_to_xyz, FieldId, FieldsIdsMap,
    FilterableAttributesFeatures, FilterableAttributesRule, Index, InternalError, Result,
    SerializationError,
};

use crate::{search::facet::filter::MAX_FILTER_DEPTH, Filter, SHARD_FIELD};

impl<'a> Filter<'a> {
    pub fn from_json(facets: &'a Value) -> Result<Option<Self>> {
        match facets {
            Value::String(expr) => {
                let condition = Self::from_str(expr)?;
                Ok(condition)
            }
            Value::Array(arr) => Self::parse_filter_array(arr),
            v => Err(Error::UserError(UserError::InvalidFilterExpression(
                &["String", "Array"],
                v.clone(),
            ))),
        }
    }

    fn parse_filter_array(arr: &'a [Value]) -> Result<Option<Self>> {
        let mut ands = Vec::new();
        for value in arr {
            match value {
                Value::String(s) => ands.push(Either::Right(s.as_str())),
                Value::Array(arr) => {
                    let mut ors = Vec::new();
                    for value in arr {
                        match value {
                            Value::String(s) => ors.push(s.as_str()),
                            v => {
                                return Err(Error::UserError(UserError::InvalidFilterExpression(
                                    &["String"],
                                    v.clone(),
                                )))
                            }
                        }
                    }
                    ands.push(Either::Left(ors));
                }
                v => {
                    return Err(Error::UserError(UserError::InvalidFilterExpression(
                        &["String", "[String]"],
                        v.clone(),
                    )))
                }
            }
        }

        Self::from_array(ands)
    }

    pub fn from_array<I, J>(array: I) -> Result<Option<Self>>
    where
        I: IntoIterator<Item = Either<J, &'a str>>,
        J: IntoIterator<Item = &'a str>,
    {
        let mut ands = vec![];

        for either in array {
            match either {
                Either::Left(array) => {
                    let mut ors = vec![];
                    for rule in array {
                        if let Some(filter) = Self::from_str(rule)? {
                            ors.push(filter.condition);
                        }
                    }

                    match ors.len() {
                        0 => (),
                        1 => ands.push(ors.pop().unwrap()),
                        _ => ands.push(FilterCondition::Or(ors)),
                    }
                }
                Either::Right(rule) => {
                    if let Some(filter) = Self::from_str(rule)? {
                        ands.push(filter.condition);
                    }
                }
            }
        }
        let and = if ands.is_empty() {
            return Ok(None);
        } else if ands.len() == 1 {
            ands.pop().unwrap()
        } else {
            FilterCondition::And(ands)
        };

        if let Some(token) = and.token_at_depth(MAX_FILTER_DEPTH) {
            return Err(token.as_external_error(FilterError::TooDeep).into());
        }

        Ok(Some(Self { condition: and }))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expression: &'a str) -> Result<Option<Self>> {
        let condition = match FilterCondition::parse(expression) {
            Ok(Some(fc)) => Ok(fc),
            Ok(None) => return Ok(None),
            Err(e) => Err(Error::UserError(UserError::InvalidFilter(e.to_string()))),
        }?;

        if let Some(token) = condition.token_at_depth(MAX_FILTER_DEPTH) {
            return Err(token.as_external_error(FilterError::TooDeep).into());
        }

        Ok(Some(Self { condition }))
    }

    pub fn use_contains_operator(&self) -> Option<&Token<'_>> {
        self.condition.use_contains_operator()
    }

    pub fn use_vector_filter(&self) -> Option<&Token<'_>> {
        self.condition.use_vector_filter()
    }

    pub fn use_shard_filter(&self) -> Option<&Token<'_>> {
        self.condition.use_field(SHARD_FIELD)
    }

    pub fn use_foreign_filter(&self) -> Option<&Token<'_>> {
        self.condition.use_foreign_operator()
    }
}
