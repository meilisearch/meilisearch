use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::{Deserialize, Deserializer};

#[cfg(test)]
fn serialize_with_wildcard<S>(
    field: &Setting<Vec<String>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::Serialize;

    let wildcard = vec!["*".to_string()];
    match field {
        Setting::Set(value) => Some(value),
        Setting::Reset => Some(&wildcard),
        Setting::NotSet => None,
    }
    .serialize(s)
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
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    pub displayed_attributes: Setting<Vec<String>>,

    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    pub searchable_attributes: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub ranking_rules: Setting<Vec<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub stop_words: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub distinct_attribute: Setting<String>,

    #[serde(skip)]
    pub _kind: PhantomData<T>,
}

impl Settings<Unchecked> {
    pub fn check(self) -> Settings<Checked> {
        let displayed_attributes = match self.displayed_attributes {
            Setting::Set(fields) => {
                if fields.iter().any(|f| f == "*") {
                    Setting::Reset
                } else {
                    Setting::Set(fields)
                }
            }
            otherwise => otherwise,
        };

        let searchable_attributes = match self.searchable_attributes {
            Setting::Set(fields) => {
                if fields.iter().any(|f| f == "*") {
                    Setting::Reset
                } else {
                    Setting::Set(fields)
                }
            }
            otherwise => otherwise,
        };

        Settings {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes: self.filterable_attributes,
            sortable_attributes: self.sortable_attributes,
            ranking_rules: self.ranking_rules,
            stop_words: self.stop_words,
            synonyms: self.synonyms,
            distinct_attribute: self.distinct_attribute,
            _kind: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Setting<T> {
    Set(T),
    Reset,
    NotSet,
}

impl<T> Default for Setting<T> {
    fn default() -> Self {
        Self::NotSet
    }
}

impl<T> Setting<T> {
    pub const fn is_not_set(&self) -> bool {
        matches!(self, Self::NotSet)
    }

    pub fn map<A>(self, f: fn(T) -> A) -> Setting<A> {
        match self {
            Setting::Set(a) => Setting::Set(f(a)),
            Setting::Reset => Setting::Reset,
            Setting::NotSet => Setting::NotSet,
        }
    }
}

#[cfg(test)]
impl<T: serde::Serialize> serde::Serialize for Setting<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Set(value) => Some(value),
            // Usually not_set isn't serialized by setting skip_serializing_if field attribute
            Self::NotSet | Self::Reset => None,
        }
        .serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Setting<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(|x| match x {
            Some(x) => Self::Set(x),
            None => Self::Reset, // Reset is forced by sending null value
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Criterion {
    /// Sorted by decreasing number of matched query terms.
    /// Query words at the front of an attribute is considered better than if it was at the back.
    Words,
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considered better.
    Attribute,
    /// Dynamically sort at query time the documents. None, one or multiple Asc/Desc sortable
    /// attributes can be used in place of this criterion at query time.
    Sort,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}

impl Criterion {
    /// Returns the field name parameter of this criterion.
    pub fn field_name(&self) -> Option<&str> {
        match self {
            Criterion::Asc(name) | Criterion::Desc(name) => Some(name),
            _otherwise => None,
        }
    }
}

impl FromStr for Criterion {
    // since we're not going to show the custom error message we can override the
    // error type.
    type Err = ();

    fn from_str(text: &str) -> Result<Criterion, Self::Err> {
        match text {
            "words" => Ok(Criterion::Words),
            "typo" => Ok(Criterion::Typo),
            "proximity" => Ok(Criterion::Proximity),
            "attribute" => Ok(Criterion::Attribute),
            "sort" => Ok(Criterion::Sort),
            "exactness" => Ok(Criterion::Exactness),
            text => match AscDesc::from_str(text) {
                Ok(AscDesc::Asc(field)) => Ok(Criterion::Asc(field)),
                Ok(AscDesc::Desc(field)) => Ok(Criterion::Desc(field)),
                Err(_) => Err(()),
            },
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub enum AscDesc {
    Asc(String),
    Desc(String),
}

impl FromStr for AscDesc {
    type Err = ();

    // since we don't know if this comes from the old or new syntax we need to check
    // for both syntax.
    // WARN: this code doesn't come from the original meilisearch v0.22.0 but was
    // written specifically to be able to import the dump of meilisearch v0.21.0 AND
    // meilisearch v0.22.0.
    fn from_str(text: &str) -> Result<AscDesc, Self::Err> {
        if let Some((field_name, asc_desc)) = text.rsplit_once(':') {
            match asc_desc {
                "asc" => Ok(AscDesc::Asc(field_name.to_string())),
                "desc" => Ok(AscDesc::Desc(field_name.to_string())),
                _ => Err(()),
            }
        } else if text.starts_with("asc(") && text.ends_with(')') {
            Ok(AscDesc::Asc(
                text.strip_prefix("asc(").unwrap().strip_suffix(')').unwrap().to_string(),
            ))
        } else if text.starts_with("desc(") && text.ends_with(')') {
            Ok(AscDesc::Desc(
                text.strip_prefix("desc(").unwrap().strip_suffix(')').unwrap().to_string(),
            ))
        } else {
            Err(())
        }
    }
}

impl fmt::Display for Criterion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Criterion::*;

        match self {
            Words => f.write_str("words"),
            Typo => f.write_str("typo"),
            Proximity => f.write_str("proximity"),
            Attribute => f.write_str("attribute"),
            Sort => f.write_str("sort"),
            Exactness => f.write_str("exactness"),
            Asc(attr) => write!(f, "{}:asc", attr),
            Desc(attr) => write!(f, "{}:desc", attr),
        }
    }
}
