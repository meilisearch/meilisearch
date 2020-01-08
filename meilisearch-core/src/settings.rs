use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub ranking_rules: Option<Vec<String>>,
    pub ranking_distinct: Option<String>,
    pub attribute_identifier: Option<String>,
    pub attributes_searchable: Option<Vec<String>>,
    pub attributes_displayed: Option<Vec<String>>,
    pub attributes_ranked: Option<Vec<String>>,
    pub stop_words: Option<BTreeSet<String>>,
    pub synonyms: Option<BTreeMap<String, Vec<String>>>,
}

impl Into<SettingsUpdate> for Settings {
    fn into(self) -> SettingsUpdate {
        let settings = self.clone();
        SettingsUpdate {
            ranking_rules: settings.ranking_rules.into(),
            ranking_distinct: settings.ranking_distinct.into(),
            attribute_identifier: settings.attribute_identifier.into(),
            attributes_searchable: settings.attributes_searchable.into(),
            attributes_displayed: settings.attributes_displayed.into(),
            attributes_ranked: settings.attributes_ranked.into(),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateState<T> {
    Update(T),
    Add(T),
    Delete(T),
    Clear,
    Nothing,
}

impl <T> From<Option<T>> for UpdateState<T> {
    fn from(opt: Option<T>) -> UpdateState<T> {
        match opt {
            Some(t) => UpdateState::Update(t),
            None => UpdateState::Nothing,
        }
    }
}

impl<T> UpdateState<T> {
    pub fn is_changed(&self) -> bool {
        match self {
            UpdateState::Nothing => false,
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettingsUpdate {
    pub ranking_rules: UpdateState<Vec<String>>,
    pub ranking_distinct: UpdateState<String>,
    pub attribute_identifier: UpdateState<String>,
    pub attributes_searchable: UpdateState<Vec<String>>,
    pub attributes_displayed: UpdateState<Vec<String>>,
    pub attributes_ranked: UpdateState<Vec<String>>,
    pub stop_words: UpdateState<BTreeSet<String>>,
    pub synonyms: UpdateState<BTreeMap<String, Vec<String>>>,
}

impl Default for SettingsUpdate {
    fn default() -> Self {
        Self {
            ranking_rules: UpdateState::Nothing,
            ranking_distinct: UpdateState::Nothing,
            attribute_identifier: UpdateState::Nothing,
            attributes_searchable: UpdateState::Nothing,
            attributes_displayed: UpdateState::Nothing,
            attributes_ranked: UpdateState::Nothing,
            stop_words: UpdateState::Nothing,
            synonyms: UpdateState::Nothing,
        }
    }
}
