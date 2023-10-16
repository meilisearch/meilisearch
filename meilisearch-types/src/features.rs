use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct RuntimeTogglableFeatures {
    pub score_details: bool,
    pub vector_store: bool,
    pub metrics: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InstanceTogglableFeatures {
    pub metrics: bool,
}

/// RuntimeToggledFeatures maintains the current value of the features that can be toggled at runtime.
/// This is not a persistent structure, it is only used in memory.
/// It is used for the features that are both togglable at runtime and instance-wide.
/// We avoid database calls by storing the current value in memory.
#[derive(Default, Debug, Clone, Copy)]
pub struct RuntimeToggledFeatures {
    pub metrics: (bool, bool), // (is_toggled, current_value)
}
