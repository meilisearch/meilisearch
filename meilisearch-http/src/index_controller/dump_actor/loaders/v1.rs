use std::collections::{BTreeMap, BTreeSet};
use std::fs::{create_dir_all, File};
use std::io::BufRead;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use heed::EnvOpenOptions;
use log::{error, info, warn};
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::index_controller::{self, uuid_resolver::HeedUuidStore, IndexMetadata};
use crate::{
    index::{deserialize_some, update_handler::UpdateHandler, Index, Unchecked},
    option::IndexerOpts,
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetadataV1 {
    db_version: String,
    indexes: Vec<IndexMetadata>,
}

impl MetadataV1 {
    pub fn load_dump(
        self,
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        size: usize,
        indexer_options: &IndexerOpts,
    ) -> anyhow::Result<()> {
        info!(
            "Loading dump, dump database version: {}, dump version: V1",
            self.db_version
        );

        let uuid_store = HeedUuidStore::new(&dst)?;
        for index in self.indexes {
            let uuid = Uuid::new_v4();
            uuid_store.insert(index.uid.clone(), uuid)?;
            let src = src.as_ref().join(index.uid);
            load_index(
                &src,
                &dst,
                uuid,
                index.meta.primary_key.as_deref(),
                size,
                indexer_options,
            )?;
        }

        Ok(())
    }
}

// These are the settings used in legacy meilisearch (<v0.21.0).
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

fn load_index(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    uuid: Uuid,
    primary_key: Option<&str>,
    size: usize,
    indexer_options: &IndexerOpts,
) -> anyhow::Result<()> {
    let index_path = dst.as_ref().join(&format!("indexes/index-{}", uuid));

    create_dir_all(&index_path)?;
    let mut options = EnvOpenOptions::new();
    options.map_size(size);
    let index = milli::Index::new(options, index_path)?;
    let index = Index(Arc::new(index));

    // extract `settings.json` file and import content
    let settings = import_settings(&src)?;
    let settings: index_controller::Settings<Unchecked> = settings.into();

    let mut txn = index.write_txn()?;

    let handler = UpdateHandler::new(indexer_options)?;

    index.update_settings_txn(&mut txn, &settings.check(), handler.update_builder(0))?;

    let file = File::open(&src.as_ref().join("documents.jsonl"))?;
    let mut reader = std::io::BufReader::new(file);
    reader.fill_buf()?;
    if !reader.buffer().is_empty() {
        index.update_documents_txn(
            &mut txn,
            UpdateFormat::JsonStream,
            IndexDocumentsMethod::ReplaceDocuments,
            Some(reader),
            handler.update_builder(0),
            primary_key,
        )?;
    }

    txn.commit()?;

    // Finaly, we extract the original milli::Index and close it
    Arc::try_unwrap(index.0)
        .map_err(|_e| "Couldn't close the index properly")
        .unwrap()
        .prepare_for_closing()
        .wait();

    // Updates are ignored in dumps V1.

    Ok(())
}

/// we need to **always** be able to convert the old settings to the settings currently being used
impl From<Settings> for index_controller::Settings<Unchecked> {
    fn from(settings: Settings) -> Self {
        Self {
            distinct_attribute: settings.distinct_attribute,
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            displayed_attributes: settings.displayed_attributes.map(|o| o.map(|vec| vec.into_iter().collect())),
            searchable_attributes: settings.searchable_attributes,
            // we previously had a `Vec<String>` but now we have a `HashMap<String, String>`
            // representing the name of the faceted field + the type of the field. Since the type
            // was not known in the V1 of the dump we are just going to assume everything is a
            // String
            filterable_attributes: settings.attributes_for_faceting.map(|o| o.map(|vec| vec.into_iter().collect())),
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            ranking_rules: settings.ranking_rules.map(|o| o.map(|vec| vec.into_iter().filter(|criterion| {
                match criterion.as_str() {
                    "words" | "typo" | "proximity" | "attribute" | "exactness" => true,
                    s if s.starts_with("asc") || s.starts_with("desc") => true,
                    "wordsPosition" => {
                        warn!("The criteria `attribute` and `wordsPosition` have been merged into a single criterion `attribute` so `wordsPositon` will be ignored");
                        false
                    }
                    s => {
                        error!("Unknown criterion found in the dump: `{}`, it will be ignored", s);
                        false
                    }
                    }
                }).collect())),
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            stop_words: settings.stop_words.map(|o| o.map(|vec| vec.into_iter().collect())),
            // we need to convert the old `Vec<String>` into a `BTreeMap<String>`
            synonyms: settings.synonyms.map(|o| o.map(|vec| vec.into_iter().collect())),
            _kind: PhantomData,
        }
    }
}

/// Extract Settings from `settings.json` file present at provided `dir_path`
fn import_settings(dir_path: impl AsRef<Path>) -> anyhow::Result<Settings> {
    let path = dir_path.as_ref().join("settings.json");
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let metadata = serde_json::from_reader(reader)?;

    Ok(metadata)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn settings_format_regression() {
        let settings = Settings::default();
        assert_eq!(
            r##"{"rankingRules":null,"distinctAttribute":null,"searchableAttributes":null,"displayedAttributes":null,"stopWords":null,"synonyms":null,"attributesForFaceting":null}"##,
            serde_json::to_string(&settings).unwrap()
        );
    }
}
