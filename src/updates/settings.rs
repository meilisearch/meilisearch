use std::num::NonZeroUsize;
use std::collections::HashMap;

use serde::{Serialize, Deserialize, de::Deserializer};

// Any value that is present is considered Some value, including null.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where T: Deserialize<'de>,
      D: Deserializer<'de>
{
    Deserialize::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    displayed_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    searchable_attributes: Option<Option<Vec<String>>>,

    #[serde(default)]
    faceted_attributes: Option<HashMap<String, String>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    criteria: Option<Option<Vec<String>>>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Facets {
    level_group_size: Option<NonZeroUsize>,
    min_level_size: Option<NonZeroUsize>,
}

