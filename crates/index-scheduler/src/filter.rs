use either::Either;
use meilisearch_types::error::Code;
use meilisearch_types::milli::{self, Filter, IndexFilter};
use serde_json::Value;

use crate::{Error, Result, RoFeatures};

pub fn parse_filter(
    facets: &Value,
    filter_parsing_error_code: Code,
    features: RoFeatures,
) -> Result<Option<Filter>> {
    let filter = match facets {
        Value::String(expr) => Filter::from_str(expr).map_err(|e| {
            Error::Milli { error: e, index_uid: None }
                .with_custom_error_code(filter_parsing_error_code)
        }),
        Value::Array(arr) => parse_filter_array(arr, filter_parsing_error_code),
        v => Err(invalid_filter_syntax_error(&["String", "Array"], v, filter_parsing_error_code)),
    }?;

    check_filter_experimental_features(filter, features)
}

fn check_filter_experimental_features(
    filter: Option<Filter>,
    features: RoFeatures,
) -> Result<Option<Filter>> {
    if let Some(ref filter) = filter {
        // If the contains operator is used while the contains filter feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_contains_operator().zip(features.check_contains_filter().err())
        {
            return Err(Error::Milli {
                error: token.to_external_error(error).into(),
                index_uid: None,
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
                index_uid: None,
            }
            .with_custom_error_code(Code::FeatureNotEnabled));
        }

        // If a shard filter is used while the network feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_shard_filter().zip(features.check_network("using a shard filter").err())
        {
            return Err(Error::Milli {
                error: token.to_external_error(error).into(),
                index_uid: None,
            }
            .with_custom_error_code(Code::FeatureNotEnabled));
        }

        // If a vector filter is used while the multi modal feature is not enabled, errors out
        if let Some((token, error)) =
            filter.use_vector_filter().zip(features.check_multimodal("using a vector filter").err())
        {
            return Err(Error::Milli {
                error: token.to_external_error(error).into(),
                index_uid: None,
            }
            .with_custom_error_code(Code::FeatureNotEnabled));
        }
    }

    Ok(filter)
}

fn parse_filter_array(arr: &[Value], code: Code) -> Result<Option<Filter>> {
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
                            return Err(invalid_filter_syntax_error(&["String"], v, code));
                        }
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => {
                return Err(invalid_filter_syntax_error(&["String", "[String]"], v, code));
            }
        }
    }

    Filter::from_array(ands)
        .map_err(|e| Error::Milli { error: e, index_uid: None }.with_custom_error_code(code))
}

fn invalid_filter_syntax_error(expected: &[&str], found: &Value, code: Code) -> Error {
    let error = milli::Error::UserError(milli::UserError::InvalidFilter(format!(
        "Invalid syntax for the filter parameter: `expected {}, found: {}`.",
        expected.join(", "),
        found
    )));

    Error::Milli { error, index_uid: None }.with_custom_error_code(code)
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
    let Some(filter) = parse_filter(filter, code, features)? else {
        return Ok(None);
    };

    Ok(Some(IndexFilter::from_filter_without_foreign(filter).map_err(|(fid, _)| {
        let error = fid
            .to_external_error("Filter condition `_foreign` is not supported for this endpoint.")
            .into();
        Error::Milli { error, index_uid: index_uid.map(String::from) }.with_custom_error_code(code)
    })?))
}
