use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub type DynamicSearchRules = BTreeMap<String, DynamicSearchRule>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DynamicSearchRule {
    pub uid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u64>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    pub actions: Vec<RuleAction>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "scope", rename_all = "camelCase")]
pub enum Condition {
    Query(QueryCondition),
    Time(TimeCondition),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueryCondition {
    pub is_empty: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TimeCondition {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub start: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub end: Option<OffsetDateTime>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RuleAction {
    pub selector: Selector,
    pub action: Action,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Selector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Action {
    Pin(PinArgs),
    Boost(BoostArgs),
    Bury(BuryArgs),
    Hide(HideArgs),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PinArgs {
    pub position: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BoostArgs {
    pub score: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BuryArgs {
    pub score: f64,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HideArgs {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};
    use std::fmt::Debug;
    use time::macros::datetime;

    fn round_trip<T>(expected: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let serialized: Value = serde_json::to_value(expected).unwrap();
        let deserialized: T = serde_json::from_value(serialized).unwrap();
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
                Condition::Query(QueryCondition { is_empty: true }),
                Condition::Time(TimeCondition {
                    start: Some(datetime!(2025-11-28 0:00:00 UTC)),
                    end: Some(datetime!(2025-11-28 23:59:59 UTC)),
                }),
            ],
            actions: vec![
                RuleAction {
                    selector: Selector {
                        index_uid: Some("products".to_string()),
                        id: Some("123".to_string()),
                        filter: None,
                    },
                    action: Action::Pin(PinArgs { position: 3 }),
                },
                RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: None,
                        filter: Some(json!({
                            "attribute": "brand",
                            "op": "eq",
                            "value": "premium",
                        })),
                    },
                    action: Action::Boost(BoostArgs { score: 1.5 }),
                },
                RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: None,
                        filter: Some(json!({
                            "attribute": "category",
                            "op": "eq",
                            "value": "clearance",
                        })),
                    },
                    action: Action::Bury(BuryArgs { score: 0.5 }),
                },
                RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: Some("456".to_string()),
                        filter: None,
                    },
                    action: Action::Hide(HideArgs {}),
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
                selector: Selector { index_uid: None, id: Some("42".to_string()), filter: None },
                action: Action::Pin(PinArgs { position: 1 }),
            }],
        };

        round_trip(&expected);
        insta::assert_json_snapshot!(expected);
    }

    #[test]
    fn all_actions_round_trip() {
        let action = Action::Pin(PinArgs { position: 5 });
        round_trip(&action);
        insta::assert_json_snapshot!("pin", action);

        let action = Action::Boost(BoostArgs { score: 2.0 });
        round_trip(&action);
        insta::assert_json_snapshot!("boost", action);

        let action = Action::Bury(BuryArgs { score: 0.3 });
        round_trip(&action);
        insta::assert_json_snapshot!("bury", action);

        let action = Action::Hide(HideArgs {});
        round_trip(&action);
        insta::assert_json_snapshot!("hide", action);
    }

    #[test]
    fn all_conditions_round_trip() {
        let condition = Condition::Query(QueryCondition { is_empty: true });
        round_trip(&condition);
        insta::assert_json_snapshot!("query", condition);

        let condition = Condition::Time(TimeCondition {
            start: Some(datetime!(2025-01-01 0:00:00 UTC)),
            end: Some(datetime!(2025-12-31 23:59:59 UTC)),
        });
        round_trip(&condition);
        insta::assert_json_snapshot!("time_both", condition);

        let condition = Condition::Time(TimeCondition {
            start: Some(datetime!(2025-01-01 0:00:00 UTC)),
            end: None,
        });
        round_trip(&condition);
        insta::assert_json_snapshot!("time_start", condition);

        let condition = Condition::Time(TimeCondition {
            start: None,
            end: Some(datetime!(2025-12-31 23:59:59 UTC)),
        });
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
                    "action": { "type": "hide" }
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
                selector: Selector { index_uid: None, id: None, filter: None },
                action: Action::Hide(HideArgs {}),
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
        assert!(!selector.contains_key("filter"));
    }
}
