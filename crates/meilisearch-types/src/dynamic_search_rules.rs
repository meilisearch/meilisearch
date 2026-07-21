use std::collections::BTreeMap;
use std::convert::Infallible;

use deserr::{DeserializeError, Deserr, ErrorKind, ValuePointerRef};
use milli::update::new::document::Document;
use milli::update::Setting;
use milli::FaultSource;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use utoipa::ToSchema;

use crate::deserr::DeserrJsonError;
use crate::error::deserr_codes::{
    InvalidDynamicSearchRuleActions, InvalidDynamicSearchRuleActive,
    InvalidDynamicSearchRuleConditions, InvalidDynamicSearchRuleDescription,
    InvalidDynamicSearchRulePriority,
};
use crate::error::ParseOffsetDateTimeError;
use crate::index_uid::IndexUid;

pub type RuleUid = IndexUid;

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
    pub precedence: Option<u64>,
    /// Whether the dynamic search rule is active.
    #[serde(default = "default_dynamic_search_rule_active")]
    pub active: bool,
    /// Conditions that must match before the dynamic search rule applies.
    #[serde(default)]
    pub conditions: Conditions,
    /// Actions to apply when the dynamic search rule matches.
    pub actions: Vec<RuleAction>,
}

impl DynamicSearchRule {
    pub fn new(uid: RuleUid) -> Self {
        Self {
            uid,
            description: None,
            precedence: None,
            active: true,
            conditions: Default::default(),
            actions: vec![],
        }
    }

    pub fn try_from_meili_doc<'a>(
        doc: impl Document<'a>,
        fault_source: FaultSource,
    ) -> Result<Self, milli::Error> {
        use milli::dynamic_search_rules::fields as dsr_fields;

        let to_milli_error = |err| match fault_source {
            FaultSource::User => milli::Error::UserError(milli::UserError::SerdeJson(err)),
            FaultSource::Runtime | FaultSource::Bug | FaultSource::Undecided => {
                milli::Error::InternalError(milli::InternalError::SerdeJson(err))
            }
        };

        let uid = serde_json::from_str(
            doc.top_level_field(dsr_fields::UID)?
                .ok_or_else(|| match fault_source {
                    FaultSource::User => {
                        milli::Error::UserError(milli::UserError::MissingDocumentId {
                            primary_key: dsr_fields::UID.to_string(),
                            document: Default::default(),
                        })
                    }
                    FaultSource::Runtime | FaultSource::Bug | FaultSource::Undecided => {
                        milli::Error::InternalError(milli::InternalError::DatabaseMissingEntry {
                            db_name: "dsr index",
                            key: None,
                        })
                    }
                })?
                .get(),
        )
        .map_err(to_milli_error)?;
        let description = match doc.top_level_field(dsr_fields::DESCRIPTION)? {
            // we deserialize the description as an Option rather than hardcoding Some here,
            // because the description could be an explicit `null`
            Some(description) => serde_json::from_str(description.get()).map_err(to_milli_error)?,
            None => None,
        };

        let precedence = match doc.top_level_field(dsr_fields::PRECEDENCE)? {
            Some(precedence) => serde_json::from_str(precedence.get()).map_err(to_milli_error)?,
            None => None,
        };

        let active = match doc.top_level_field(dsr_fields::ACTIVE)? {
            Some(active) => serde_json::from_str(active.get()).map_err(to_milli_error)?,
            // `active` defaults to true!
            None => true,
        };

        let conditions = match doc.top_level_field(dsr_fields::CONDITIONS)? {
            Some(conditions) => serde_json::from_str(conditions.get()).map_err(to_milli_error)?,
            None => Default::default(),
        };

        let actions = match doc.top_level_field(dsr_fields::ACTIONS)? {
            Some(actions) => serde_json::from_str(actions.get()).map_err(to_milli_error)?,
            None => Default::default(),
        };

        Ok(Self { uid, description, precedence, active, conditions, actions })
    }

    pub fn into_uid_update(self) -> (RuleUid, DynamicSearchRuleUpdateRequest) {
        let Self { uid, description, precedence, active, conditions, actions } = self;
        (
            uid,
            DynamicSearchRuleUpdateRequest {
                description: Setting::some_or_not_set(description),
                precedence: Setting::some_or_not_set(precedence),
                active: Setting::Set(active),
                conditions: Setting::Set(conditions),
                actions: Setting::Set(actions),
            },
        )
    }

    pub fn apply_update(&mut self, update: DynamicSearchRuleUpdateRequest) {
        let Self { uid: _, description, precedence, active, conditions, actions } = self;

        let DynamicSearchRuleUpdateRequest {
            description: new_description,
            precedence: new_precedence,
            active: new_active,
            conditions: new_conditions,
            actions: new_actions,
        } = update;

        *description = match new_description {
            Setting::Set(new_description) => Some(new_description),
            Setting::Reset => None,
            Setting::NotSet => description.take(),
        };
        *precedence = match new_precedence {
            Setting::Set(new_precedence) => Some(new_precedence),
            Setting::Reset => None,
            Setting::NotSet => precedence.take(),
        };

        *active = match new_active {
            Setting::Set(new_active) => new_active,
            Setting::Reset => true,
            Setting::NotSet => *active,
        };

        match new_conditions {
            Setting::Set(Conditions { time: new_time, query: new_query, filter: new_filter }) => {
                let Conditions { time, query, filter } = conditions;
                *time = new_time;
                *query = new_query;
                *filter = new_filter;
            }
            Setting::Reset => *conditions = Conditions::default(),
            Setting::NotSet => (),
        }

        *actions = match new_actions {
            Setting::Set(new_actions) => new_actions,
            Setting::Reset => vec![],
            Setting::NotSet => std::mem::take(actions),
        };
    }

    pub fn facet_count(&self) -> usize {
        let Some(filter) = self.conditions.filter.as_ref() else {
            return 0;
        };

        let mut count = 0;
        for value in filter.values.values() {
            count_value(value, &mut count);
        }
        count
    }
}

fn count_value(value: &serde_json::Value, count: &mut usize) {
    match value {
        serde_json::Value::Null => {}
        serde_json::Value::String(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::Bool(_) => {
            *count += 1;
        }
        serde_json::Value::Array(values) => {
            for value in values {
                count_value(value, count)
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                count_value(value, count);
            }
        }
    }
}

const fn default_dynamic_search_rule_active() -> bool {
    true
}

#[routes::request(db, setting)]
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicSearchRuleUpdateRequest {
    /// Human-readable description of the dynamic search rule.
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleDescription>, schema_type = Option<String>, skip_serializing_if = "Setting::is_not_set")]
    pub description: Setting<String>,
    /// Precedence of the dynamic search rule. Lower numeric values take precedence over higher
    /// ones. If omitted, the rule is treated as having the lowest precedence. This precedence is
    /// used to resolve conflicts between matching rules:
    /// - If the same document is selected by multiple rules, the smallest `priority` number wins
    /// - If different documents are pinned to the same position, they are ordered by ascending `priority`
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRulePriority>, schema_type = Option<u64>, skip_serializing_if = "Setting::is_not_set")]
    pub precedence: Setting<u64>,
    /// Whether the dynamic search rule is active.
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleActive>, schema_type = Option<bool>, skip_serializing_if = "Setting::is_not_set")]
    pub active: Setting<bool>,
    /// Conditions that must match before the dynamic search rule applies.
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleConditions>, schema_type = Option<Conditions>, skip_serializing_if = "Setting::is_not_set")]
    pub conditions: Setting<Conditions>,
    /// Actions to apply when the dynamic search rule matches.
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleActions>, schema_type = Option<Vec<RuleAction>>, skip_serializing_if = "Setting::is_not_set")]
    pub actions: Setting<Vec<RuleAction>>,
}

#[routes::request(db, validate = validate_condition -> DeserrJsonError, override_error = DeserrJsonError<InvalidDynamicSearchRuleConditions>)]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Conditions {
    /// Time range where the rule is active
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<TimeCondition>,
    /// Conditions on the search query that determines whether the rule is active
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryCondition>,
    /// Conditions on the values matching the filter of the search query that determines whether the rule is active
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterCondition>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, ToSchema, Deserr)]
#[deserr(rename_all = camelCase)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct TimeCondition {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    #[deserr(default, try_from(Option<String>) = parse_optional_rfc3339_datetime -> ParseOffsetDateTimeError)]
    /// Start of the time range where this rule can be considered active.
    ///
    /// Specify as a RFC3339 datetime.
    pub start: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    /// End of the time range where this rule can be considered active.
    ///
    /// Specify as a RFC3339 datetime.
    #[deserr(default, try_from(Option<String>) = parse_optional_rfc3339_datetime -> ParseOffsetDateTimeError)]
    pub end: Option<OffsetDateTime>,
}

impl routes::RequestBody for TimeCondition {}

#[routes::request(db, override_error = DeserrJsonError<InvalidDynamicSearchRuleConditions>)]
#[derive(Debug, Clone, PartialEq)]
pub struct QueryCondition {
    /// If present and non-null, specifies either:
    ///
    /// - That this rule can only be active when the search query is empty
    /// - That this rule can only be active when the search query is non-empty (contains at least one word)
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub is_empty: Option<bool>,

    /// If present and non-null, specifies that the rule can only be active if all the specified words are
    /// present in the search query.
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub words: Option<String>,
}

#[routes::request(db, override_error = DeserrJsonError<InvalidDynamicSearchRuleConditions>)]
#[derive(Debug, Clone, PartialEq)]
pub struct FilterCondition {
    #[request(default)]
    pub values: BTreeMap<String, serde_json::Value>,
}

// We manually check the exclusivity of `is_empty` and `contains` because Deserr does not support
// untagged enums
fn validate_condition<E: DeserializeError>(
    conditions: Conditions,
    location: ValuePointerRef,
) -> Result<Conditions, E> {
    // 1. check is_empty and words
    if let Some(query) = &conditions.query {
        if query.is_empty == Some(true) && query.words.is_some() {
            return Err(deserr::take_cf_content(E::error::<Infallible>(
                None,
                ErrorKind::Unexpected {
                    msg: "either `isEmpty` or `words` can be used, not both at once".to_string(),
                },
                location.push_key("query"),
            )));
        }
    }

    // 2. check that start is before end
    if let Some(time) = &conditions.time {
        if let Some((start, end)) = time.start.as_ref().zip(time.end.as_ref()) {
            if start > end {
                return Err(deserr::take_cf_content(E::error::<Infallible>(
                    None,
                    ErrorKind::Unexpected {
                        msg: format!("`end` (`{end}`) should be later than `start` (`{start}`)"),
                    },
                    location.push_key("time"),
                )));
            }
        }
    }

    Ok(conditions)
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

// manual impl: no support for schema_type = Object and tag in DynamicSearchRuleAction
impl routes::RequestBody for RuleAction {}

#[routes::request(db, where_predicate = __Deserr_E: deserr::MergeWithError<crate::index_uid::IndexUidFormatError>, no_error)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<IndexUid>,
    #[request(required)]
    pub id: String,
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
