use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use deserr::{DeserializeError, DeserializeFromValue};
use fst::IntoStreamer;
use milli::{Index, DEFAULT_VALUES_PER_FACET};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The maximimum number of results that the engine
/// will be able to return in one search call.
pub const DEFAULT_PAGINATION_MAX_TOTAL_HITS: usize = 1000;

fn serialize_with_wildcard<S>(
    field: &Setting<Vec<String>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let wildcard = vec!["*".to_string()];
    match field {
        Setting::Set(value) => Some(value),
        Setting::Reset => Some(&wildcard),
        Setting::NotSet => None,
    }
    .serialize(s)
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
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

impl<T> From<Setting<T>> for milli::update::Setting<T> {
    fn from(value: Setting<T>) -> Self {
        match value {
            Setting::Set(x) => milli::update::Setting::Set(x),
            Setting::Reset => milli::update::Setting::Reset,
            Setting::NotSet => milli::update::Setting::NotSet,
        }
    }
}
impl<T> From<milli::update::Setting<T>> for Setting<T> {
    fn from(value: milli::update::Setting<T>) -> Self {
        match value {
            milli::update::Setting::Set(x) => Setting::Set(x),
            milli::update::Setting::Reset => Setting::Reset,
            milli::update::Setting::NotSet => Setting::NotSet,
        }
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

impl<T: Serialize> Serialize for Setting<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
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

impl<T, E> DeserializeFromValue<E> for Setting<T>
where
    T: DeserializeFromValue<E>,
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::Null => Ok(Setting::Reset),
            _ => T::deserialize_from_value(value, location).map(Setting::Set),
        }
    }
    fn default() -> Option<Self> {
        Some(Self::NotSet)
    }
}

#[derive(Clone, Default, Debug, Serialize, PartialEq, Eq)]
pub struct Checked;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Unchecked;

impl<E> DeserializeFromValue<E> for Unchecked
where
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        _value: deserr::Value<V>,
        _location: deserr::ValuePointerRef,
    ) -> Result<Self, E> {
        unreachable!()
    }
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct MinWordSizeTyposSetting {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub one_typo: Setting<u8>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub two_typos: Setting<u8>,
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct TypoSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub enabled: Setting<bool>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub min_word_size_for_typos: Setting<MinWordSizeTyposSetting>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub disable_on_words: Setting<BTreeSet<String>>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub disable_on_attributes: Setting<BTreeSet<String>>,
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FacetingSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub max_values_per_facet: Setting<usize>,
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct PaginationSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub max_total_hits: Setting<usize>,
}

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>"))]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Settings<T> {
    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub displayed_attributes: Setting<Vec<String>>,

    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub searchable_attributes: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub ranking_rules: Setting<Vec<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub stop_words: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub distinct_attribute: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub typo_tolerance: Setting<TypoSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub faceting: Setting<FacetingSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub pagination: Setting<PaginationSettings>,

    #[serde(skip)]
    pub _kind: PhantomData<T>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Facets {
    pub level_group_size: Option<NonZeroUsize>,
    pub min_level_size: Option<NonZeroUsize>,
}

pub fn apply_settings_to_builder(
    settings: &Settings<Checked>,
    builder: &mut milli::update::Settings,
) {
    match settings.searchable_attributes {
        Setting::Set(ref names) => builder.set_searchable_fields(names.clone()),
        Setting::Reset => builder.reset_searchable_fields(),
        Setting::NotSet => (),
    }

    match settings.displayed_attributes {
        Setting::Set(ref names) => builder.set_displayed_fields(names.clone()),
        Setting::Reset => builder.reset_displayed_fields(),
        Setting::NotSet => (),
    }

    match settings.filterable_attributes {
        Setting::Set(ref facets) => {
            builder.set_filterable_fields(facets.clone().into_iter().collect())
        }
        Setting::Reset => builder.reset_filterable_fields(),
        Setting::NotSet => (),
    }

    match settings.sortable_attributes {
        Setting::Set(ref fields) => builder.set_sortable_fields(fields.iter().cloned().collect()),
        Setting::Reset => builder.reset_sortable_fields(),
        Setting::NotSet => (),
    }

    match settings.ranking_rules {
        Setting::Set(ref criteria) => builder.set_criteria(criteria.clone()),
        Setting::Reset => builder.reset_criteria(),
        Setting::NotSet => (),
    }

    match settings.stop_words {
        Setting::Set(ref stop_words) => builder.set_stop_words(stop_words.clone()),
        Setting::Reset => builder.reset_stop_words(),
        Setting::NotSet => (),
    }

    match settings.synonyms {
        Setting::Set(ref synonyms) => builder.set_synonyms(synonyms.clone().into_iter().collect()),
        Setting::Reset => builder.reset_synonyms(),
        Setting::NotSet => (),
    }

    match settings.distinct_attribute {
        Setting::Set(ref attr) => builder.set_distinct_field(attr.clone()),
        Setting::Reset => builder.reset_distinct_field(),
        Setting::NotSet => (),
    }

    match settings.typo_tolerance {
        Setting::Set(ref value) => {
            match value.enabled {
                Setting::Set(val) => builder.set_autorize_typos(val),
                Setting::Reset => builder.reset_authorize_typos(),
                Setting::NotSet => (),
            }

            match value.min_word_size_for_typos {
                Setting::Set(ref setting) => {
                    match setting.one_typo {
                        Setting::Set(val) => builder.set_min_word_len_one_typo(val),
                        Setting::Reset => builder.reset_min_word_len_one_typo(),
                        Setting::NotSet => (),
                    }
                    match setting.two_typos {
                        Setting::Set(val) => builder.set_min_word_len_two_typos(val),
                        Setting::Reset => builder.reset_min_word_len_two_typos(),
                        Setting::NotSet => (),
                    }
                }
                Setting::Reset => {
                    builder.reset_min_word_len_one_typo();
                    builder.reset_min_word_len_two_typos();
                }
                Setting::NotSet => (),
            }

            match value.disable_on_words {
                Setting::Set(ref words) => {
                    builder.set_exact_words(words.clone());
                }
                Setting::Reset => builder.reset_exact_words(),
                Setting::NotSet => (),
            }

            match value.disable_on_attributes {
                Setting::Set(ref words) => {
                    builder.set_exact_attributes(words.iter().cloned().collect())
                }
                Setting::Reset => builder.reset_exact_attributes(),
                Setting::NotSet => (),
            }
        }
        Setting::Reset => {
            // all typo settings need to be reset here.
            builder.reset_authorize_typos();
            builder.reset_min_word_len_one_typo();
            builder.reset_min_word_len_two_typos();
            builder.reset_exact_words();
            builder.reset_exact_attributes();
        }
        Setting::NotSet => (),
    }

    match settings.faceting {
        Setting::Set(ref value) => match value.max_values_per_facet {
            Setting::Set(val) => builder.set_max_values_per_facet(val),
            Setting::Reset => builder.reset_max_values_per_facet(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_max_values_per_facet(),
        Setting::NotSet => (),
    }

    match settings.pagination {
        Setting::Set(ref value) => match value.max_total_hits {
            Setting::Set(val) => builder.set_pagination_max_total_hits(val),
            Setting::Reset => builder.reset_pagination_max_total_hits(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_pagination_max_total_hits(),
        Setting::NotSet => (),
    }
}

pub fn settings(
    index: &Index,
    rtxn: &crate::heed::RoTxn,
) -> Result<Settings<Checked>, milli::Error> {
    let displayed_attributes =
        index.displayed_fields(rtxn)?.map(|fields| fields.into_iter().map(String::from).collect());

    let searchable_attributes = index
        .user_defined_searchable_fields(rtxn)?
        .map(|fields| fields.into_iter().map(String::from).collect());

    let filterable_attributes = index.filterable_fields(rtxn)?.into_iter().collect();

    let sortable_attributes = index.sortable_fields(rtxn)?.into_iter().collect();

    let criteria = index.criteria(rtxn)?.into_iter().map(|c| c.to_string()).collect();

    let stop_words = index
        .stop_words(rtxn)?
        .map(|stop_words| -> Result<BTreeSet<_>, milli::Error> {
            Ok(stop_words.stream().into_strs()?.into_iter().collect())
        })
        .transpose()?
        .unwrap_or_default();
    let distinct_field = index.distinct_field(rtxn)?.map(String::from);

    // in milli each word in the synonyms map were split on their separator. Since we lost
    // this information we are going to put space between words.
    let synonyms = index
        .synonyms(rtxn)?
        .iter()
        .map(|(key, values)| (key.join(" "), values.iter().map(|value| value.join(" ")).collect()))
        .collect();

    let min_typo_word_len = MinWordSizeTyposSetting {
        one_typo: Setting::Set(index.min_word_len_one_typo(rtxn)?),
        two_typos: Setting::Set(index.min_word_len_two_typos(rtxn)?),
    };

    let disabled_words = match index.exact_words(rtxn)? {
        Some(fst) => fst.into_stream().into_strs()?.into_iter().collect(),
        None => BTreeSet::new(),
    };

    let disabled_attributes = index.exact_attributes(rtxn)?.into_iter().map(String::from).collect();

    let typo_tolerance = TypoSettings {
        enabled: Setting::Set(index.authorize_typos(rtxn)?),
        min_word_size_for_typos: Setting::Set(min_typo_word_len),
        disable_on_words: Setting::Set(disabled_words),
        disable_on_attributes: Setting::Set(disabled_attributes),
    };

    let faceting = FacetingSettings {
        max_values_per_facet: Setting::Set(
            index.max_values_per_facet(rtxn)?.unwrap_or(DEFAULT_VALUES_PER_FACET),
        ),
    };

    let pagination = PaginationSettings {
        max_total_hits: Setting::Set(
            index.pagination_max_total_hits(rtxn)?.unwrap_or(DEFAULT_PAGINATION_MAX_TOTAL_HITS),
        ),
    };

    Ok(Settings {
        displayed_attributes: match displayed_attributes {
            Some(attrs) => Setting::Set(attrs),
            None => Setting::Reset,
        },
        searchable_attributes: match searchable_attributes {
            Some(attrs) => Setting::Set(attrs),
            None => Setting::Reset,
        },
        filterable_attributes: Setting::Set(filterable_attributes),
        sortable_attributes: Setting::Set(sortable_attributes),
        ranking_rules: Setting::Set(criteria),
        stop_words: Setting::Set(stop_words),
        distinct_attribute: match distinct_field {
            Some(field) => Setting::Set(field),
            None => Setting::Reset,
        },
        synonyms: Setting::Set(synonyms),
        typo_tolerance: Setting::Set(typo_tolerance),
        faceting: Setting::Set(faceting),
        pagination: Setting::Set(pagination),
        _kind: PhantomData,
    })
}

#[cfg(test)]
pub(crate) mod test {
    use proptest::prelude::*;

    use super::*;

    pub(super) fn setting_strategy<T: Arbitrary + Clone>() -> impl Strategy<Value = Setting<T>> {
        prop_oneof![Just(Setting::NotSet), Just(Setting::Reset), any::<T>().prop_map(Setting::Set)]
    }

    #[test]
    fn test_setting_check() {
        // test no changes
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![String::from("hello")]),
            searchable_attributes: Setting::Set(vec![String::from("hello")]),
            filterable_attributes: Setting::NotSet,
            sortable_attributes: Setting::NotSet,
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.clone().check();
        assert_eq!(settings.displayed_attributes, checked.displayed_attributes);
        assert_eq!(settings.searchable_attributes, checked.searchable_attributes);

        // test wildcard
        // test no changes
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![String::from("*")]),
            searchable_attributes: Setting::Set(vec![String::from("hello"), String::from("*")]),
            filterable_attributes: Setting::NotSet,
            sortable_attributes: Setting::NotSet,
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.check();
        assert_eq!(checked.displayed_attributes, Setting::Reset);
        assert_eq!(checked.searchable_attributes, Setting::Reset);
    }
}
