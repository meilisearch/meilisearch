use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    marker::PhantomData,
};

use serde::{Deserialize, Deserializer};

#[cfg(test)]
fn serialize_with_wildcard<S>(
    field: &Option<Option<Vec<String>>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let wildcard = vec!["*".to_string()];
    s.serialize_some(&field.as_ref().map(|o| o.as_ref().unwrap_or(&wildcard)))
}

fn deserialize_some<'de, T, D>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

#[derive(Clone, Default, Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Checked;
#[derive(Clone, Default, Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Unchecked;

#[derive(Debug, Clone, Default, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(bound(
    serialize = "T: serde::Serialize",
    deserialize = "T: Deserialize<'static>"
))]
pub struct Settings<T> {
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Option::is_none"
    )]
    pub displayed_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Option::is_none"
    )]
    pub searchable_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub filterable_attributes: Option<Option<HashSet<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub ranking_rules: Option<Option<Vec<String>>>,
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop_words: Option<Option<BTreeSet<String>>>,
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub synonyms: Option<Option<BTreeMap<String, Vec<String>>>>,
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub distinct_attribute: Option<Option<String>>,

    #[serde(skip)]
    pub _kind: PhantomData<T>,
}

impl Settings<Unchecked> {
    pub fn check(mut self) -> Settings<Checked> {
        let displayed_attributes = match self.displayed_attributes.take() {
            Some(Some(fields)) => {
                if fields.iter().any(|f| f == "*") {
                    Some(None)
                } else {
                    Some(Some(fields))
                }
            }
            otherwise => otherwise,
        };

        let searchable_attributes = match self.searchable_attributes.take() {
            Some(Some(fields)) => {
                if fields.iter().any(|f| f == "*") {
                    Some(None)
                } else {
                    Some(Some(fields))
                }
            }
            otherwise => otherwise,
        };

        Settings {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes: self.filterable_attributes,
            ranking_rules: self.ranking_rules,
            stop_words: self.stop_words,
            synonyms: self.synonyms,
            distinct_attribute: self.distinct_attribute,
            _kind: PhantomData,
        }
    }
}
