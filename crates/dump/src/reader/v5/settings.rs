use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Default, Debug, Serialize, PartialEq, Eq)]
pub struct Checked;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Unchecked;

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>"))]
pub struct Settings<T> {
    #[serde(default)]
    pub displayed_attributes: Setting<Vec<String>>,

    #[serde(default)]
    pub searchable_attributes: Setting<Vec<String>>,

    #[serde(default)]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    #[serde(default)]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    #[serde(default)]
    pub ranking_rules: Setting<Vec<String>>,
    #[serde(default)]
    pub stop_words: Setting<BTreeSet<String>>,
    #[serde(default)]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    pub distinct_attribute: Setting<String>,
    #[serde(default)]
    pub typo_tolerance: Setting<TypoSettings>,
    #[serde(default)]
    pub faceting: Setting<FacetingSettings>,
    #[serde(default)]
    pub pagination: Setting<PaginationSettings>,

    #[serde(skip)]
    pub _kind: PhantomData<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
#[cfg_attr(test, derive(serde::Serialize))]
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
    pub fn set(self) -> Option<T> {
        match self {
            Self::Set(value) => Some(value),
            _ => None,
        }
    }

    pub const fn as_ref(&self) -> Setting<&T> {
        match *self {
            Self::Set(ref value) => Setting::Set(value),
            Self::Reset => Setting::Reset,
            Self::NotSet => Setting::NotSet,
        }
    }

    pub const fn is_not_set(&self) -> bool {
        matches!(self, Self::NotSet)
    }

    /// If `Self` is `Reset`, then map self to `Set` with the provided `val`.
    pub fn or_reset(self, val: T) -> Self {
        match self {
            Self::Reset => Self::Set(val),
            otherwise => otherwise,
        }
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

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MinWordSizeTyposSetting {
    #[serde(default)]
    pub one_typo: Setting<u8>,
    #[serde(default)]
    pub two_typos: Setting<u8>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TypoSettings {
    #[serde(default)]
    pub enabled: Setting<bool>,
    #[serde(default)]
    pub min_word_size_for_typos: Setting<MinWordSizeTyposSetting>,
    #[serde(default)]
    pub disable_on_words: Setting<BTreeSet<String>>,
    #[serde(default)]
    pub disable_on_attributes: Setting<BTreeSet<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FacetingSettings {
    #[serde(default)]
    pub max_values_per_facet: Setting<usize>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct PaginationSettings {
    #[serde(default)]
    pub max_total_hits: Setting<usize>,
}

impl Settings<Checked> {
    pub fn cleared() -> Settings<Checked> {
        Settings {
            displayed_attributes: Setting::Reset,
            searchable_attributes: Setting::Reset,
            filterable_attributes: Setting::Reset,
            sortable_attributes: Setting::Reset,
            ranking_rules: Setting::Reset,
            stop_words: Setting::Reset,
            synonyms: Setting::Reset,
            distinct_attribute: Setting::Reset,
            typo_tolerance: Setting::Reset,
            faceting: Setting::Reset,
            pagination: Setting::Reset,
            _kind: PhantomData,
        }
    }

    pub fn into_unchecked(self) -> Settings<Unchecked> {
        let Self {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            ranking_rules,
            stop_words,
            synonyms,
            distinct_attribute,
            typo_tolerance,
            faceting,
            pagination,
            ..
        } = self;

        Settings {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes,
            sortable_attributes,
            ranking_rules,
            stop_words,
            synonyms,
            distinct_attribute,
            typo_tolerance,
            faceting,
            pagination,
            _kind: PhantomData,
        }
    }
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
            typo_tolerance: self.typo_tolerance,
            faceting: self.faceting,
            pagination: self.pagination,
            _kind: PhantomData,
        }
    }
}
