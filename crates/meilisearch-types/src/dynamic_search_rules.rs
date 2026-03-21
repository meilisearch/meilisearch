use std::collections::BTreeMap;
use std::convert::Infallible;

use deserr::{DeserializeError, Deserr, ErrorKind, ValuePointerRef};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use utoipa::ToSchema;

use crate::deserr::DeserrJsonError;
use crate::error::ParseOffsetDateTimeError;

pub type DynamicSearchRules = BTreeMap<String, DynamicSearchRule>;

#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase)]
#[serde(rename_all = "camelCase")]
pub struct DynamicSearchRule {
    /// Unique identifier of the dynamic search rule.
    pub uid: String,
    /// Human-readable description of the dynamic search rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    pub description: Option<String>,
    /// Priority of the dynamic search rule. Lower values take precedence over higher ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    pub priority: Option<u64>,
    /// Whether the dynamic search rule is active.
    #[serde(default)]
    #[deserr(default)]
    pub active: bool,
    /// Conditions that must match before the dynamic search rule applies.
    #[serde(default)]
    #[deserr(default)]
    pub conditions: Vec<Condition>,
    /// Actions to apply when the dynamic search rule matches.
    pub actions: Vec<RuleAction>,
}

#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, Eq, ToSchema)]
#[deserr(error = DeserrJsonError, tag = "scope", rename_all = camelCase, validate = validate_condition -> DeserrJsonError)]
#[serde(tag = "scope", rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum Condition {
    #[deserr(rename_all = camelCase)]
    #[serde(rename_all = "camelCase")]
    Query {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[deserr(default)]
        is_empty: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[deserr(default)]
        contains: Option<String>,
    },

    #[deserr(rename_all = camelCase)]
    #[serde(rename_all = "camelCase")]
    Time {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "time::serde::rfc3339::option"
        )]
        #[deserr(default, try_from(Option<String>) = parse_optional_rfc3339_datetime -> ParseOffsetDateTimeError)]
        start: Option<OffsetDateTime>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "time::serde::rfc3339::option"
        )]
        #[deserr(default, try_from(Option<String>) = parse_optional_rfc3339_datetime -> ParseOffsetDateTimeError)]
        end: Option<OffsetDateTime>,
    },
}

fn validate_condition<E: DeserializeError>(
    condition: Condition,
    location: ValuePointerRef,
) -> Result<Condition, E> {
    match &condition {
        Condition::Query { is_empty, contains } => {
            if is_empty.is_some() && contains.is_some() {
                return Err(deserr::take_cf_content(E::error::<Infallible>(
                    None,
                    ErrorKind::Unexpected {
                        msg: "either `isEmpty` or `contains` can be used, not all at once"
                            .to_string(),
                    },
                    location,
                )));
            }

            if is_empty.is_none() && contains.is_none() {
                return Err(deserr::take_cf_content(E::error::<Infallible>(
                    None,
                    ErrorKind::Unexpected {
                        msg: "at least `isEmpty` or `contains` must be used".to_string(),
                    },
                    location,
                )));
            }
        }

        Condition::Time { start, end } => {
            if let Some((start, end)) = start.as_ref().zip(end.as_ref()) {
                if start > end {
                    return Err(deserr::take_cf_content(E::error::<Infallible>(
                        None,
                        ErrorKind::Unexpected {
                            msg: format!(
                                "`end` (`{end}`) should be later than `start` (`{start}`)"
                            ),
                        },
                        location,
                    )));
                }
            }
        }
    }

    Ok(condition)
}

#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct RuleAction {
    /// Target document selector for this action.
    pub selector: Selector,
    // Use Object here because utoipa's tagged-enum schema generation combines
    // allOf with additionalProperties: false in a way that Spectral rejects.
    #[schema(value_type = Object)]
    /// Action payload to apply to the selected document.
    pub action: DynamicSearchRuleAction,
}

#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, Eq, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct Selector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    pub index_uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, Eq, ToSchema)]
#[deserr(error = DeserrJsonError, tag = "type", rename_all = camelCase, deny_unknown_fields)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub enum DynamicSearchRuleAction {
    Pin { position: u32 },
}

fn parse_optional_rfc3339_datetime(
    value: Option<String>,
) -> Result<Option<OffsetDateTime>, ParseOffsetDateTimeError> {
    let Some(value) = value else { return Ok(None) };
    OffsetDateTime::parse(&value, &Rfc3339).map(Some).map_err(|_| ParseOffsetDateTimeError(value))
}
