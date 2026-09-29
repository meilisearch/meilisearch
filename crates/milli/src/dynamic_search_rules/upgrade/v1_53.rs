use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{dynamic_search_rules::PinAction, update::Setting};

/// An action for a rule
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
    /// Turns this action into a current [`PinAction`].
    ///
    /// This is the only outcome as v1.53 does not support any other kind of actions.
    pub fn into_pin_action(self) -> PinAction {
        let DynamicSearchRuleAction::Pin { position } = self.action;
        let index_uid = self.selector.index_uid;
        let id = self.selector.id;

        PinAction { index_uid, id, position }
    }
}

/// Legacy selector type
#[routes::request(db, no_error)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    /// UID of the index to select, any if `None`
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    /// ID of the document to select
    #[request(required)]
    pub id: String,
}

/// Kinds of action, only Pin is available
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum DynamicSearchRuleAction {
    /// Pin the document selected by the [`Selector`] at the specified position
    Pin {
        /// Position where to pin the document
        position: u32,
    },
}

/// Type used to update a DSR
///
/// Appears in the task database
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DynamicSearchRuleUpdateRequest {
    /// Human-readable description of the dynamic search rule.
    #[serde(default)]
    pub description: Setting<String>,
    /// Precedence of the dynamic search rule. Lower numeric values take precedence over higher
    /// ones. If omitted, the rule is treated as having the lowest precedence. This precedence is
    /// used to resolve conflicts between matching rules:
    /// - If the same document is selected by multiple rules, the smallest `priority` number wins
    /// - If different documents are pinned to the same position, they are ordered by ascending `priority`
    #[serde(default)]
    pub precedence: Setting<u64>,
    /// Whether the dynamic search rule is active.
    #[serde(default)]
    pub active: Setting<bool>,
    /// Conditions that must match before the dynamic search rule applies.
    #[serde(default)]
    pub conditions: Setting<Conditions>,
    /// Actions to apply when the dynamic search rule matches.
    #[serde(default)]
    pub actions: Setting<Vec<RuleAction>>,
}

/// Conditions for the DSR to be enabled
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Conditions {
    /// Time range where the rule is active
    #[serde(default)]
    pub time: Option<TimeCondition>,
    /// Conditions on the search query that determines whether the rule is active
    #[serde(default)]
    pub query: Option<QueryCondition>,
    /// Conditions on the values matching the filter of the search query that determines whether the rule is active
    #[serde(default)]
    pub filter: Option<FilterCondition>,
}

/// Time condition
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TimeCondition {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
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
    pub end: Option<OffsetDateTime>,
}

/// Query condition
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct QueryCondition {
    /// If present and non-null, specifies either:
    ///
    /// - That this rule can only be active when the search query is empty
    /// - That this rule can only be active when the search query is non-empty (contains at least one word)
    #[serde(default)]
    pub is_empty: Option<bool>,

    /// If present and non-null, specifies that the rule can only be active if all the specified words are
    /// present in the search query.
    #[serde(default)]
    pub words: Option<String>,
}

/// Filter condition
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FilterCondition {
    /// A map of facet names to facet values.
    ///
    /// Arrays and nested facet names are supported
    #[serde(default)]
    pub values: BTreeMap<String, serde_json::Value>,
}
