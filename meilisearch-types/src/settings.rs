use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::str::FromStr;

use deserr::{DeserializeError, DeserializeFromValue, ErrorKind, MergeWithError, ValuePointerRef};
use fst::IntoStreamer;
use milli::update::Setting;
use milli::{Criterion, CriterionError, Index, DEFAULT_VALUES_PER_FACET};
use serde::{Deserialize, Serialize, Serializer};

use crate::deserr::DeserrJsonError;
use crate::error::deserr_codes::*;
use crate::error::unwrap_any;

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

fn validate_min_word_size_for_typo_setting<E: DeserializeError>(
    s: MinWordSizeTyposSetting,
    location: ValuePointerRef,
) -> Result<MinWordSizeTyposSetting, E> {
    if let (Setting::Set(one), Setting::Set(two)) = (s.one_typo, s.two_typos) {
        if one > two {
            return Err(unwrap_any(E::error::<Infallible>(None, ErrorKind::Unexpected { msg: format!("`minWordSizeForTypos` setting is invalid. `oneTypo` and `twoTypos` fields should be between `0` and `255`, and `twoTypos` should be greater or equals to `oneTypo` but found `oneTypo: {one}` and twoTypos: {two}`.") }, location)));
        }
    }
    Ok(s)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(deny_unknown_fields, rename_all = camelCase, validate = validate_min_word_size_for_typo_setting -> DeserrJsonError<InvalidSettingsTypoTolerance>)]
pub struct MinWordSizeTyposSetting {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub one_typo: Setting<u8>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub two_typos: Setting<u8>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(deny_unknown_fields, rename_all = camelCase, where_predicate = __Deserr_E: deserr::MergeWithError<DeserrJsonError<InvalidSettingsTypoTolerance>>)]
pub struct TypoSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub enabled: Setting<bool>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsTypoTolerance>)]
    pub min_word_size_for_typos: Setting<MinWordSizeTyposSetting>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub disable_on_words: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub disable_on_attributes: Setting<BTreeSet<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct FacetingSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub max_values_per_facet: Setting<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct PaginationSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    pub max_total_hits: Setting<usize>,
}

impl MergeWithError<milli::CriterionError> for DeserrJsonError<InvalidSettingsRankingRules> {
    fn merge(
        _self_: Option<Self>,
        other: milli::CriterionError,
        merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Self::error::<Infallible>(
            None,
            ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, DeserializeFromValue)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>")
)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct Settings<T> {
    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsDisplayedAttributes>)]
    pub displayed_attributes: Setting<Vec<String>>,

    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSearchableAttributes>)]
    pub searchable_attributes: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsFilterableAttributes>)]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSortableAttributes>)]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsRankingRules>)]
    pub ranking_rules: Setting<Vec<RankingRuleView>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsStopWords>)]
    pub stop_words: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSynonyms>)]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsDistinctAttribute>)]
    pub distinct_attribute: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsTypoTolerance>)]
    pub typo_tolerance: Setting<TypoSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsFaceting>)]
    pub faceting: Setting<FacetingSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsPagination>)]
    pub pagination: Setting<PaginationSettings>,

    #[serde(skip)]
    #[deserr(skip)]
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
        Setting::Set(ref criteria) => {
            builder.set_criteria(criteria.iter().map(|c| c.clone().into()).collect())
        }
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

    let criteria = index.criteria(rtxn)?;

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
        ranking_rules: Setting::Set(criteria.iter().map(|c| c.clone().into()).collect()),
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

#[derive(Debug, Clone, PartialEq, Eq, DeserializeFromValue)]
#[deserr(from(&String) = FromStr::from_str -> CriterionError)]
pub enum RankingRuleView {
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
impl Serialize for RankingRuleView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", Criterion::from(self.clone())))
    }
}
impl<'de> Deserialize<'de> for RankingRuleView {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RankingRuleView;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "the name of a valid ranking rule (string)")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let criterion = Criterion::from_str(v).map_err(|_| {
                    E::invalid_value(serde::de::Unexpected::Str(v), &"a valid ranking rule")
                })?;
                Ok(RankingRuleView::from(criterion))
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}
impl FromStr for RankingRuleView {
    type Err = <Criterion as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RankingRuleView::from(Criterion::from_str(s)?))
    }
}
impl fmt::Display for RankingRuleView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt::Display::fmt(&Criterion::from(self.clone()), f)
    }
}
impl From<Criterion> for RankingRuleView {
    fn from(value: Criterion) -> Self {
        match value {
            Criterion::Words => RankingRuleView::Words,
            Criterion::Typo => RankingRuleView::Typo,
            Criterion::Proximity => RankingRuleView::Proximity,
            Criterion::Attribute => RankingRuleView::Attribute,
            Criterion::Sort => RankingRuleView::Sort,
            Criterion::Exactness => RankingRuleView::Exactness,
            Criterion::Asc(x) => RankingRuleView::Asc(x),
            Criterion::Desc(x) => RankingRuleView::Desc(x),
        }
    }
}
impl From<RankingRuleView> for Criterion {
    fn from(value: RankingRuleView) -> Self {
        match value {
            RankingRuleView::Words => Criterion::Words,
            RankingRuleView::Typo => Criterion::Typo,
            RankingRuleView::Proximity => Criterion::Proximity,
            RankingRuleView::Attribute => Criterion::Attribute,
            RankingRuleView::Sort => Criterion::Sort,
            RankingRuleView::Exactness => Criterion::Exactness,
            RankingRuleView::Asc(x) => Criterion::Asc(x),
            RankingRuleView::Desc(x) => Criterion::Desc(x),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

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
