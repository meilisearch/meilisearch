use serde::{Deserialize, Serialize};

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

#[routes::request(db, no_error)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    #[request(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    #[request(required)]
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum DynamicSearchRuleAction {
    Pin { position: u32 },
}
