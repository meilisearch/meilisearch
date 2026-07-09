use meilisearch_types::dynamic_search_rules::{
    Conditions as NewConditions, DynamicSearchRule as NewDynamicSearchRule,
    DynamicSearchRuleAction as NewDynamicSearchRuleAction, QueryCondition,
    RuleAction as NewRuleAction, RuleUid, Selector as NewSelector, TimeCondition,
};
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

mod db_name {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
}

#[derive(Clone)]
pub(crate) struct LegacyDynamicSearchRulesStore {
    pub(crate) persisted: Database<Str, SerdeJson<LegacyDynamicSearchRule>>,
}

impl LegacyDynamicSearchRulesStore {
    pub fn new(env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> crate::Result<Self> {
        let persisted = env.create_database(wtxn, Some(db_name::DYNAMIC_SEARCH_RULES))?;

        Ok(Self { persisted })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDynamicSearchRule {
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

impl LegacyDynamicSearchRule {
    pub fn into_dynamic_search_rule(self) -> Option<NewDynamicSearchRule> {
        let Self { uid, description, priority, active, conditions, actions } = self;

        let actions = actions
            .into_iter()
            // filter out actions that don't cleanly convert
            .filter_map(|action| action.into_new_action())
            .collect();

        let mut time = None;
        let mut query = None;

        for condition in conditions {
            match condition {
                Condition::Query { is_empty, contains } => match &mut query {
                    Some(QueryCondition { is_empty: existing_is_empty, words: existing_words }) => {
                        if let (Some(existing_is_empty), Some(is_empty)) =
                            (existing_is_empty, is_empty)
                        {
                            if *existing_is_empty != is_empty {
                                return None;
                            }
                        }

                        match (existing_words.as_mut(), contains) {
                            (None, contains) => *existing_words = contains,
                            (_, None) => (),
                            (Some(existing), Some(new)) => {
                                existing.push(' ');
                                existing.push_str(&new);
                            }
                        }
                    }
                    None => query = Some(QueryCondition { is_empty, words: contains }),
                },
                Condition::Time { start, end } => match &mut time {
                    Some(TimeCondition { start: existing_start, end: existing_end }) => {
                        let new_start = match (existing_start.as_ref().copied(), start) {
                            (None, None) => None,
                            (None, Some(start)) | (Some(start), None) => Some(start),
                            (Some(existing), Some(new)) => Some(existing.max(new)),
                        };

                        let new_end = match (existing_end.as_ref().copied(), end) {
                            (None, None) => None,
                            (None, Some(end)) | (Some(end), None) => Some(end),
                            (Some(existing), Some(new)) => Some(existing.min(new)),
                        };

                        if let (Some(start), Some(end)) = (new_start, new_end) {
                            if start > end {
                                return None;
                            }
                        }
                        *existing_start = new_start;
                        *existing_end = new_end;
                    }
                    None => time = Some(TimeCondition { start, end }),
                },
            }
        }

        let conditions = NewConditions { time, query, filter: None };

        Some(NewDynamicSearchRule {
            uid,
            description,
            precedence: priority,
            active,
            conditions,
            actions,
        })
    }
}

fn default_dynamic_search_rule_active() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "scope", rename_all = "camelCase")]
pub enum Condition {
    #[serde(rename_all = "camelCase")]
    Query {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        is_empty: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        contains: Option<String>,
    },

    #[serde(rename_all = "camelCase")]
    Time {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "time::serde::rfc3339::option"
        )]
        start: Option<OffsetDateTime>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "time::serde::rfc3339::option"
        )]
        end: Option<OffsetDateTime>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuleAction {
    /// Target document selector for this action.
    pub selector: Selector,
    // Use Object here because utoipa's tagged-enum schema generation combines
    // allOf with additionalProperties: false in a way that Spectral rejects.
    /// Action payload to apply to the selected document.
    pub action: DynamicSearchRuleAction,
}

impl RuleAction {
    fn into_new_action(self) -> Option<NewRuleAction> {
        let Self { selector, action } = self;

        let Selector { index_uid, id } = selector;
        let id = id?;

        let selector = NewSelector { index_uid, id };
        let action = match action {
            DynamicSearchRuleAction::Pin { position } => {
                NewDynamicSearchRuleAction::Pin { position }
            }
        };

        Some(NewRuleAction { selector, action })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Selector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<RuleUid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum DynamicSearchRuleAction {
    Pin { position: u32 },
}
