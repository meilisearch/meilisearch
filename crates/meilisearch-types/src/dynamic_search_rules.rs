use std::collections::BTreeMap;

use deserr::Deserr;
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
#[deserr(error = DeserrJsonError, tag = "scope", rename_all = camelCase)]
#[serde(tag = "scope", rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub enum Condition {
    #[deserr(rename_all = camelCase)]
    #[serde(rename_all = "camelCase")]
    Query { is_empty: bool },
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

#[cfg(test)]
mod tests {
    use super::*;
    use deserr::Deserr;
    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};
    use std::fmt::Debug;
    use time::macros::datetime;

    fn round_trip<T>(expected: &T)
    where
        T: Serialize + DeserializeOwned + Deserr<DeserrJsonError> + PartialEq + Debug,
    {
        let serialized: Value = serde_json::to_value(expected).unwrap();
        let deserialized: T = serde_json::from_value(serialized).unwrap();
        assert_eq!(&deserialized, expected);

        let serialized: Value = serde_json::to_value(expected).unwrap();
        let deserialized: T = deserr::deserialize(serialized).unwrap();
        assert_eq!(&deserialized, expected);
    }

    #[test]
    fn full_rule_round_trip() {
        let expected = DynamicSearchRule {
            uid: "black-friday-2025".to_string(),
            description: Some("Black Friday 2025 Merchandising rules".to_string()),
            priority: Some(1),
            active: true,
            conditions: vec![
                Condition::Query { is_empty: true },
                Condition::Time {
                    start: Some(datetime!(2025-11-28 0:00:00 UTC)),
                    end: Some(datetime!(2025-11-28 23:59:59 UTC)),
                },
            ],
            actions: vec![
                RuleAction {
                    selector: Selector {
                        index_uid: Some("products".to_string()),
                        id: Some("123".to_string()),
                    },
                    action: DynamicSearchRuleAction::Pin { position: 3 },
                },
                RuleAction {
                    selector: Selector {
                        index_uid: Some("products".to_string()),
                        id: Some("456".to_string()),
                    },
                    action: DynamicSearchRuleAction::Pin { position: 0 },
                },
                RuleAction {
                    selector: Selector { index_uid: None, id: Some("789".to_string()) },
                    action: DynamicSearchRuleAction::Pin { position: 8 },
                },
                RuleAction {
                    selector: Selector { index_uid: None, id: Some("999".to_string()) },
                    action: DynamicSearchRuleAction::Pin { position: 12 },
                },
            ],
        };

        round_trip(&expected);
        insta::assert_json_snapshot!(expected);
    }

    #[test]
    fn minimal_rule_round_trip() {
        let expected = DynamicSearchRule {
            uid: "simple-rule".to_string(),
            description: None,
            priority: None,
            active: false,
            conditions: vec![],
            actions: vec![RuleAction {
                selector: Selector { index_uid: None, id: Some("42".to_string()) },
                action: DynamicSearchRuleAction::Pin { position: 1 },
            }],
        };

        round_trip(&expected);
        insta::assert_json_snapshot!(expected);
    }

    #[test]
    fn all_actions_round_trip() {
        let action = DynamicSearchRuleAction::Pin { position: 5 };
        round_trip(&action);
        insta::assert_json_snapshot!("pin", action);
    }

    #[test]
    fn all_conditions_round_trip() {
        let condition = Condition::Query { is_empty: true };
        round_trip(&condition);
        insta::assert_json_snapshot!("query", condition);

        let condition = Condition::Time {
            start: Some(datetime!(2025-01-01 0:00:00 UTC)),
            end: Some(datetime!(2025-12-31 23:59:59 UTC)),
        };
        round_trip(&condition);
        insta::assert_json_snapshot!("time_both", condition);

        let condition =
            Condition::Time { start: Some(datetime!(2025-01-01 0:00:00 UTC)), end: None };
        round_trip(&condition);
        insta::assert_json_snapshot!("time_start", condition);

        let condition =
            Condition::Time { start: None, end: Some(datetime!(2025-12-31 23:59:59 UTC)) };
        round_trip(&condition);
        insta::assert_json_snapshot!("time_end", condition);
    }

    #[test]
    fn defaults_on_deserialization() {
        let json = json!({
            "uid": "defaults-test",
            "actions": [
                {
                    "selector": {},
                    "action": { "type": "pin", "position": 0 }
                }
            ]
        });

        let rule: DynamicSearchRule = serde_json::from_value(json).unwrap();
        assert_eq!(rule.description, None);
        assert_eq!(rule.priority, None);
        assert!(!rule.active);
        assert!(rule.conditions.is_empty());
    }

    #[test]
    fn skip_serializing_none_fields() {
        let rule = DynamicSearchRule {
            uid: "no-optionals".to_string(),
            description: None,
            priority: None,
            active: false,
            conditions: vec![],
            actions: vec![RuleAction {
                selector: Selector { index_uid: None, id: None },
                action: DynamicSearchRuleAction::Pin { position: 0 },
            }],
        };

        let serialized = serde_json::to_value(&rule).unwrap();
        let obj = serialized.as_object().unwrap();
        assert!(!obj.contains_key("version"));
        assert!(!obj.contains_key("description"));
        assert!(!obj.contains_key("priority"));

        let selector = obj["actions"][0]["selector"].as_object().unwrap();
        assert!(!selector.contains_key("indexUid"));
        assert!(!selector.contains_key("id"));
    }
}
