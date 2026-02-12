use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub type DynamicSearchRules = BTreeMap<String, DynamicSearchRule>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DynamicSearchRule {
    /// data format version to support upcasting if the format evolves over time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub uid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    pub actions: Vec<RuleAction>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "scope", content = "settings", rename_all = "camelCase")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
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
    pub filter: Option<Filter>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Filter {
    pub attribute: String,
    pub op: FilterOp,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "name", content = "args", rename_all = "camelCase")]
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
    use serde::de::DeserializeOwned;
    use serde_json::{json, Value};
    use std::fmt::Debug;

    use super::*;

    fn round_trip<T>(content: Value, expected: T)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let deserialized: T = serde_json::from_value(content.clone()).unwrap();
        assert_eq!(deserialized, expected);
        assert_eq!(content, serde_json::to_value(deserialized).unwrap());
    }

    #[test]
    fn full_rule_round_trip() {
        let json = json!({
            "version": "1",
            "uid": "black-friday-2025",
            "description": "Black Friday 2025 Merchandising rules",
            "priority": 1,
            "active": true,
            "conditions": [
                {
                    "scope": "query",
                    "settings": { "isEmpty": true }
                },
                {
                    "scope": "time",
                    "settings": {
                        "start": "2025-11-28T00:00:00Z",
                        "end": "2025-11-28T23:59:59Z"
                    }
                }
            ],
            "actions": [
                {
                    "selector": { "indexUid": "products", "id": "123" },
                    "action": { "name": "pin", "args": { "position": 3 } }
                },
                {
                    "selector": {
                        "filter": { "attribute": "brand", "op": "eq", "value": "premium" }
                    },
                    "action": { "name": "boost", "args": { "score": 1.5 } }
                },
                {
                    "selector": {
                        "filter": { "attribute": "category", "op": "eq", "value": "clearance" }
                    },
                    "action": { "name": "bury", "args": { "score": 0.5 } }
                },
                {
                    "selector": { "id": "456" },
                    "action": { "name": "hide", "args": {} }
                }
            ]
        });

        let expected = DynamicSearchRule {
            version: Some("1".to_string()),
            uid: "black-friday-2025".to_string(),
            description: Some("Black Friday 2025 Merchandising rules".to_string()),
            priority: Some(1),
            active: true,
            conditions: vec![
                Condition::Query(QueryCondition { is_empty: true }),
                Condition::Time(TimeCondition {
                    start: Some("2025-11-28T00:00:00Z".to_string()),
                    end: Some("2025-11-28T23:59:59Z".to_string()),
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
                        filter: Some(Filter {
                            attribute: "brand".to_string(),
                            op: FilterOp::Eq,
                            value: "premium".to_string(),
                        }),
                    },
                    action: Action::Boost(BoostArgs { score: 1.5 }),
                },
                RuleAction {
                    selector: Selector {
                        index_uid: None,
                        id: None,
                        filter: Some(Filter {
                            attribute: "category".to_string(),
                            op: FilterOp::Eq,
                            value: "clearance".to_string(),
                        }),
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

        round_trip(json, expected);
    }

    #[test]
    fn minimal_rule_round_trip() {
        let json = json!({
            "uid": "simple-rule",
            "active": false,
            "actions": [
                {
                    "selector": { "id": "42" },
                    "action": { "name": "pin", "args": { "position": 1 } }
                }
            ]
        });

        let expected = DynamicSearchRule {
            version: None,
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

        let deserialized: DynamicSearchRule = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized, expected);
    }

    #[test]
    fn all_filter_ops_round_trip() {
        let cases = vec![
            ("eq", FilterOp::Eq),
            ("neq", FilterOp::Neq),
            ("gt", FilterOp::Gt),
            ("lt", FilterOp::Lt),
            ("gte", FilterOp::Gte),
            ("lte", FilterOp::Lte),
        ];

        for (json_str, expected_op) in cases {
            round_trip(json!(json_str), expected_op);
        }
    }

    #[test]
    fn all_actions_round_trip() {
        round_trip(
            json!({"name": "pin", "args": {"position": 5}}),
            Action::Pin(PinArgs { position: 5 }),
        );
        round_trip(
            json!({"name": "boost", "args": {"score": 2.0}}),
            Action::Boost(BoostArgs { score: 2.0 }),
        );
        round_trip(
            json!({"name": "bury", "args": {"score": 0.3}}),
            Action::Bury(BuryArgs { score: 0.3 }),
        );
        round_trip(json!({"name": "hide", "args": {}}), Action::Hide(HideArgs {}));
    }

    #[test]
    fn all_conditions_round_trip() {
        round_trip(
            json!({"scope": "query", "settings": {"isEmpty": false}}),
            Condition::Query(QueryCondition { is_empty: false }),
        );
        round_trip(
            json!({"scope": "time", "settings": {"start": "2025-01-01T00:00:00Z", "end": "2025-12-31T23:59:59Z"}}),
            Condition::Time(TimeCondition {
                start: Some("2025-01-01T00:00:00Z".to_string()),
                end: Some("2025-12-31T23:59:59Z".to_string()),
            }),
        );
        // time with only start
        round_trip(
            json!({"scope": "time", "settings": {"start": "2025-06-01T00:00:00Z"}}),
            Condition::Time(TimeCondition {
                start: Some("2025-06-01T00:00:00Z".to_string()),
                end: None,
            }),
        );
        // time with only end
        round_trip(
            json!({"scope": "time", "settings": {"end": "2025-08-31T23:59:59Z"}}),
            Condition::Time(TimeCondition {
                start: None,
                end: Some("2025-08-31T23:59:59Z".to_string()),
            }),
        );
    }

    #[test]
    fn defaults_on_deserialization() {
        let json = json!({
            "uid": "defaults-test",
            "actions": [
                {
                    "selector": {},
                    "action": { "name": "hide", "args": {} }
                }
            ]
        });

        let rule: DynamicSearchRule = serde_json::from_value(json).unwrap();
        assert_eq!(rule.version, None);
        assert_eq!(rule.description, None);
        assert_eq!(rule.priority, None);
        assert!(!rule.active);
        assert!(rule.conditions.is_empty());
    }

    #[test]
    fn skip_serializing_none_fields() {
        let rule = DynamicSearchRule {
            version: None,
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
