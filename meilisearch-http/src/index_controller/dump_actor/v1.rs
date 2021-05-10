use std::{collections::{BTreeMap, BTreeSet}, marker::PhantomData};

use log::warn;
use serde::{Deserialize, Serialize};
use crate::{index::Unchecked, index_controller};
use crate::index::deserialize_some;
use super::*;

/// This is the settings used in the last version of meilisearch exporting dump in V1
#[derive(Default, Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Settings {
    #[serde(default, deserialize_with = "deserialize_some")]
    pub ranking_rules: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub distinct_attribute: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub searchable_attributes: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub displayed_attributes: Option<Option<BTreeSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub stop_words: Option<Option<BTreeSet<String>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub synonyms: Option<Option<BTreeMap<String, Vec<String>>>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub attributes_for_faceting: Option<Option<Vec<String>>>,
}

/// we need to **always** be able to convert the old settings to the settings currently being used
impl From<Settings> for index_controller::Settings<Unchecked> {
    fn from(settings: Settings) -> Self {
        if settings.synonyms.flatten().is_some() {
            error!("`synonyms` are not yet implemented and thus will be ignored");
        }
        Self {
            distinct_attribute: settings.distinct_attribute,
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            displayed_attributes: settings.displayed_attributes.map(|o| o.map(|vec| vec.into_iter().collect())),
            searchable_attributes: settings.searchable_attributes,
            // we previously had a `Vec<String>` but now we have a `HashMap<String, String>`
            // representing the name of the faceted field + the type of the field. Since the type
            // was not known in the V1 of the dump we are just going to assume everything is a
            // String
            attributes_for_faceting: settings.attributes_for_faceting.map(|o| o.map(|vec| vec.into_iter().map(|key| (key, String::from("string"))).collect())),
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            ranking_rules: settings.ranking_rules.map(|o| o.map(|vec| vec.into_iter().filter_map(|criterion| {
                match criterion.as_str() {
                    "words" | "typo" | "proximity" => Some(criterion),
                    s if s.starts_with("asc") || s.starts_with("desc") => Some(criterion),
                    "wordsPosition" => {
                        warn!("The criteria `words` and `wordsPosition` have been merged into a single criterion `words` so `wordsPositon` will be ignored");
                        Some(String::from("words"))
                    }
                    "exactness" => {
                        error!("The criterion `{}` is not implemented currently and thus will be ignored", criterion);
                        None
                    }
                    s => {
                        error!("Unknown criterion found in the dump: `{}`, it will be ignored", s);
                        None
                    }
                    }
                }).collect())),
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            stop_words: settings.stop_words.map(|o| o.map(|vec| vec.into_iter().collect())),
            _kind: PhantomData,
        }
    }
}

/// Extract Settings from `settings.json` file present at provided `dir_path`
fn import_settings(dir_path: &Path) -> anyhow::Result<Settings> {
    let path = dir_path.join("settings.json");
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let metadata = serde_json::from_reader(reader)?;

    Ok(metadata)
}


pub fn import_index(size: usize, uuid: Uuid, dump_path: &Path, db_path: &Path, primary_key: Option<&str>) -> anyhow::Result<()> {
    let index_path = db_path.join(&format!("indexes/index-{}", uuid));
    info!("Importing a dump from an old version of meilisearch with dump version 1");

    std::fs::create_dir_all(&index_path)?;
    let mut options = EnvOpenOptions::new();
    options.map_size(size);
    let index = milli::Index::new(options, index_path)?;
    let index = Index(Arc::new(index));

    // extract `settings.json` file and import content
    let settings = import_settings(&dump_path)?;
    let settings: index_controller::Settings<Unchecked> = settings.into();
    let update_builder = UpdateBuilder::new(0);
    index.update_settings(&settings.check(), update_builder)?;

    let update_builder = UpdateBuilder::new(1);
    let file = File::open(&dump_path.join("documents.jsonl"))?;
    let reader = std::io::BufReader::new(file);

    // TODO: TAMO: waiting for milli. We should use the result
    let _ = index.update_documents(
        UpdateFormat::JsonStream,
        IndexDocumentsMethod::ReplaceDocuments,
        Some(reader),
        update_builder,
        primary_key,
    );

    // the last step: we extract the original milli::Index and close it
    Arc::try_unwrap(index.0)
        .map_err(|_e| "[dumps] At this point no one is supposed to have a reference on the index")
        .unwrap()
        .prepare_for_closing()
        .wait();

    // at this point we should handle the import of the updates, but since the update logic is not handled in
    // meilisearch we are just going to ignore this part

    Ok(())
}
