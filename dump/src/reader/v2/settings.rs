use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;
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
#[serde(bound(serialize = "T: serde::Serialize", deserialize = "T: Deserialize<'static>"))]
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
    pub filterable_attributes: Option<Option<BTreeSet<String>>>,

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

static ASC_DESC_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(asc|desc)\(([\w_-]+)\)"#).unwrap());

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub enum Criterion {
    /// Sorted by decreasing number of matched query terms.
    /// Query words at the front of an attribute is considered better than if it was at the back.
    Words,
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considred better.
    Attribute,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}

impl FromStr for Criterion {
    type Err = ();

    fn from_str(txt: &str) -> Result<Criterion, Self::Err> {
        match txt {
            "words" => Ok(Criterion::Words),
            "typo" => Ok(Criterion::Typo),
            "proximity" => Ok(Criterion::Proximity),
            "attribute" => Ok(Criterion::Attribute),
            "exactness" => Ok(Criterion::Exactness),
            text => {
                let caps = ASC_DESC_REGEX.captures(text).ok_or(())?;
                let order = caps.get(1).unwrap().as_str();
                let field_name = caps.get(2).unwrap().as_str();
                match order {
                    "asc" => Ok(Criterion::Asc(field_name.to_string())),
                    "desc" => Ok(Criterion::Desc(field_name.to_string())),
                    _text => Err(()),
                }
            }
        }
    }
}
