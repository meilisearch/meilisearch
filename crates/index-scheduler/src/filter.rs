use either::Either;
use meilisearch_types::error::Code;
use meilisearch_types::milli::{self, Filter, FilterCondition, IndexFilter, IndexFilterCondition};
use serde_json::Value;

use crate::{Error, Result, RoFeatures};

/// Convert a vector of filters into a vector of index filters without evaluating the foreign filters
///
/// This function will not open any foreign index but will panic if a foreign filter is encountered.
pub fn filters_into_index_filters_unchecked(
    filters: Vec<Option<Filter>>,
) -> milli::Result<Vec<Option<IndexFilter>>> {
    filters
        .into_iter()
        .map(|filter| {
            let Some(filter) = filter else { return Ok(None) };
            condition_to_index_condition(filter.condition, &mut |_| unreachable!())
                .map(|condition| Some(IndexFilter { condition }))
        })
        .collect::<milli::Result<_>>()
}

pub fn condition_to_index_condition<F>(
    filter: FilterCondition,
    foreign_filter: &mut F,
) -> milli::Result<IndexFilterCondition>
where
    F: FnMut(FilterCondition) -> milli::Result<IndexFilterCondition>,
{
    match filter {
        FilterCondition::Not(filter) => condition_to_index_condition(*filter, foreign_filter)
            .map(Box::new)
            .map(IndexFilterCondition::Not),
        FilterCondition::Condition { fid, op } => Ok(IndexFilterCondition::Condition { fid, op }),
        FilterCondition::In { fid, els } => Ok(IndexFilterCondition::In { fid, els }),
        FilterCondition::Or(filters) => filters
            .into_iter()
            .map(|filter| condition_to_index_condition(filter, foreign_filter))
            .collect::<milli::Result<_>>()
            .map(IndexFilterCondition::Or),

        FilterCondition::And(filters) => filters
            .into_iter()
            .map(|filter| condition_to_index_condition(filter, foreign_filter))
            .collect::<milli::Result<_>>()
            .map(IndexFilterCondition::And),

        FilterCondition::VectorExists { fid, embedder, filter } => {
            Ok(IndexFilterCondition::VectorExists { fid, embedder, filter })
        }
        FilterCondition::GeoLowerThan { point, radius, resolution } => {
            Ok(IndexFilterCondition::GeoLowerThan { point, radius, resolution })
        }
        FilterCondition::GeoBoundingBox { top_right_point, bottom_left_point } => {
            Ok(IndexFilterCondition::GeoBoundingBox { top_right_point, bottom_left_point })
        }
        FilterCondition::GeoPolygon { points } => Ok(IndexFilterCondition::GeoPolygon { points }),
        FilterCondition::Foreign { .. } => foreign_filter(filter),
    }
}

pub fn parse_filter(
    facets: &Value,
    filter_parsing_error_code: Code,
    features: RoFeatures,
    index_uid: Option<&str>,
) -> Result<Option<Filter>> {
    let filter = match facets {
        Value::String(expr) => Filter::from_str(expr).map_err(|e| {
            Error::Milli { error: e, index_uid: index_uid.map(String::from) }
                .with_custom_error_code(filter_parsing_error_code)
        }),
        Value::Array(arr) => parse_filter_array(arr, filter_parsing_error_code, index_uid),
        v => Err(invalid_filter_syntax_error(
            &["String", "Array"],
            v,
            filter_parsing_error_code,
            index_uid,
        )),
    }?;

    check_filter_experimental_features(filter, features, index_uid)
}

fn check_filter_experimental_features(
    filter: Option<Filter>,
    features: RoFeatures,
    index_uid: Option<&str>,
) -> Result<Option<Filter>> {
    if let Some(ref filter) = filter {
        // If the contains operator is used while the contains filter feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_contains_operator().zip(features.check_contains_filter().err())
        {
            return Err(Error::Milli {
                error: token.to_external_error(error).into(),
                index_uid: index_uid.map(String::from),
            }
            .with_custom_error_code(Code::FeatureNotEnabled));
        }

        // If a foreign filter is used while the foreign keys feature is not enabled, errors out
        if let Some((token, error)) = filter
            .use_foreign_filter()
            .zip(features.check_foreign_keys_setting("using a foreign filter").err())
        {
            return Err(Error::Milli {
                error: token.to_external_error(error).into(),
                index_uid: index_uid.map(String::from),
            }
            .with_custom_error_code(Code::FeatureNotEnabled));
        }

        // If a shard filter is used while the network feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_shard_filter().zip(features.check_network("using a shard filter").err())
        {
            return Err(Error::Milli {
                error: token.to_external_error(error).into(),
                index_uid: index_uid.map(String::from),
            }
            .with_custom_error_code(Code::FeatureNotEnabled));
        }

        // If a vector filter is used while the multi modal feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_vector_filter().zip(features.check_multimodal("using a vector filter").err())
        {
            return Err(Error::Milli {
                error: token.to_external_error(error).into(),
                index_uid: index_uid.map(String::from),
            }
            .with_custom_error_code(Code::FeatureNotEnabled));
        }
    }

    Ok(filter)
}

fn parse_filter_array(
    arr: &[Value],
    code: Code,
    index_uid: Option<&str>,
) -> Result<Option<Filter>> {
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
                            return Err(invalid_filter_syntax_error(
                                &["String"],
                                v,
                                code,
                                index_uid,
                            ));
                        }
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => {
                return Err(invalid_filter_syntax_error(
                    &["String", "[String]"],
                    v,
                    code,
                    index_uid,
                ));
            }
        }
    }

    Filter::from_array(ands)
        .map_err(|e| Error::Milli { error: e, index_uid: None }.with_custom_error_code(code))
}

fn invalid_filter_syntax_error(
    expected: &[&str],
    found: &Value,
    code: Code,
    index_uid: Option<&str>,
) -> Error {
    let error = milli::Error::UserError(milli::UserError::InvalidFilter(format!(
        "Invalid syntax for the filter parameter: `expected {}, found: {}`.",
        expected.join(", "),
        found
    )));

    Error::Milli { error, index_uid: index_uid.map(String::from) }.with_custom_error_code(code)
}

/// Parse an index filter from a JSON value
///
/// This function will:
/// - Check the experimental features
/// - Parse the filter
/// - if a foreign filter is encountered, return an error "Unsupported foreign filter"
pub fn parse_local_index_filter(
    filter: &Value,
    index_uid: Option<&str>,
    features: RoFeatures,
    code: Code,
) -> Result<Option<IndexFilter>> {
    let Some(Filter { condition }) = parse_filter(filter, code, features, index_uid)? else {
        return Ok(None);
    };
    let condition = condition_to_index_condition(condition, &mut |filter| {
        let FilterCondition::Foreign { fid, op: _ } = filter else { unreachable!() };
        let error = milli::Error::UserError(milli::UserError::InvalidFilter(
            "Filter condition `_foreign` is not supported for this endpoint.".to_string(),
        ));
        Err(fid.to_external_error(error).into())
    })
    .map_err(|e| {
        Error::Milli { error: e, index_uid: index_uid.map(String::from) }
            .with_custom_error_code(code)
    })?;

    Ok(Some(IndexFilter { condition }))
}
