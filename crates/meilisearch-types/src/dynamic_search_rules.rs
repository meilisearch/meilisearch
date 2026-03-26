use std::collections::BTreeMap;
use std::convert::Infallible;

use deserr::{DeserializeError, Deserr, ErrorKind, ValuePointerRef};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use utoipa::ToSchema;

use crate::deserr::DeserrJsonError;
use crate::error::ParseOffsetDateTimeError;
use crate::index_uid::IndexUid;

pub type RuleUid = IndexUid;

pub type DynamicSearchRules = BTreeMap<RuleUid, DynamicSearchRule>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct DynamicSearchRule {
    /// Unique identifier of the dynamic search rule.
    pub uid: RuleUid,
    /// Human-readable description of the dynamic search rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Precedence of the dynamic search rule. Lower numeric values take precedence over higher
    /// ones. If omitted, the rule is treated as having the lowest precedence. This precedence is
    /// used to resolve conflicts between matching rules:
    /// - If the same document is selected by multiple rules, the smallest `priority` number wins
    /// - If different documents are pinned to the same position, they are ordered by ascending `priority`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u64>,
    /// Whether the dynamic search rule is active.
    #[serde(default = "default_dynamic_search_rule_active")]
    pub active: bool,
    /// Conditions that must match before the dynamic search rule applies.
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Actions to apply when the dynamic search rule matches.
    pub actions: Vec<RuleAction>,
}

const fn default_dynamic_search_rule_active() -> bool {
    true
}

#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, Eq, ToSchema)]
#[deserr(tag = "scope", rename_all = camelCase, validate = validate_condition -> DeserrJsonError)]
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

// We manually check the exclusivity of `is_empty` and `contains` because Deserr does not support
// untagged enums
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
#[deserr(
    rename_all = camelCase,
    deny_unknown_fields,
    where_predicate = __Deserr_E: deserr::MergeWithError<crate::index_uid::IndexUidFormatError>
)]
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
#[deserr(
    rename_all = camelCase,
    deny_unknown_fields,
    where_predicate = __Deserr_E: deserr::MergeWithError<crate::index_uid::IndexUidFormatError>
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct Selector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    pub index_uid: Option<IndexUid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[deserr(default)]
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, Eq, ToSchema)]
#[deserr(tag = "type", rename_all = camelCase, deny_unknown_fields)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub enum DynamicSearchRuleAction {
    Pin { position: u32 },
}

fn parse_optional_rfc3339_datetime(
    value: Option<String>,
) -> Result<Option<OffsetDateTime>, ParseOffsetDateTimeError> {
    value
        .map(|value| {
            OffsetDateTime::parse(&value, &Rfc3339).map_err(|_| ParseOffsetDateTimeError(value))
        })
        .transpose()
}
