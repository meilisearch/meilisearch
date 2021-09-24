use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::Path;

use log::{error, info, warn};
use milli::update::Setting;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::index_controller::index_resolver::uuid_store::HeedUuidStore;
use crate::index_controller::{self, IndexMetadata};
use crate::index_controller::{asc_ranking_rule, desc_ranking_rule};
use crate::{
    index::Unchecked,
    options::IndexerOpts,
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

pub fn deserialize_some<'de, T, D>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
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
    _src: impl AsRef<Path>,
    _dst: impl AsRef<Path>,
    _uuid: Uuid,
    _primary_key: Option<&str>,
    _size: usize,
    _indexer_options: &IndexerOpts,
) -> anyhow::Result<()> {
    todo!("fix dump obkv documents")
    //let index_path = dst.as_ref().join(&format!("indexes/index-{}", uuid));

    //create_dir_all(&index_path)?;
    //let mut options = EnvOpenOptions::new();
    //options.map_size(size);
    //let index = milli::Index::new(options, index_path)?;
    //let index = Index(Arc::new(index));

    //// extract `settings.json` file and import content
    //let settings = import_settings(&src)?;
    //let settings: index_controller::Settings<Unchecked> = settings.into();

    //let mut txn = index.write_txn()?;

    //let handler = UpdateHandler::new(indexer_options)?;

    //index.update_settings_txn(&mut txn, &settings.check(), handler.update_builder(0))?;

    //let file = File::open(&src.as_ref().join("documents.jsonl"))?;
    //let mut reader = std::io::BufReader::new(file);
    //reader.fill_buf()?;
    //if !reader.buffer().is_empty() {
        //index.update_documents_txn(
            //&mut txn,
            //IndexDocumentsMethod::ReplaceDocuments,
            //Some(reader),
            //handler.update_builder(0),
            //primary_key,
        //)?;
    //}

    //txn.commit()?;

    //// Finaly, we extract the original milli::Index and close it
    //Arc::try_unwrap(index.0)
        //.map_err(|_e| "Couldn't close the index properly")
        //.unwrap()
        //.prepare_for_closing()
        //.wait();

    //// Updates are ignored in dumps V1.

    //Ok(())
}

/// we need to **always** be able to convert the old settings to the settings currently being used
impl From<Settings> for index_controller::Settings<Unchecked> {
    fn from(settings: Settings) -> Self {
        Self {
            distinct_attribute: match settings.distinct_attribute {
                Some(Some(attr)) => Setting::Set(attr),
                Some(None) => Setting::Reset,
                None => Setting::NotSet
            },
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            displayed_attributes: match settings.displayed_attributes {
                Some(Some(attrs)) => Setting::Set(attrs.into_iter().collect()),
                Some(None) => Setting::Reset,
                None => Setting::NotSet
            },
            searchable_attributes: match settings.searchable_attributes {
                Some(Some(attrs)) => Setting::Set(attrs),
                Some(None) => Setting::Reset,
                None => Setting::NotSet
            },
            filterable_attributes: match settings.attributes_for_faceting {
                Some(Some(attrs)) => Setting::Set(attrs.into_iter().collect()),
                Some(None) => Setting::Reset,
                None => Setting::NotSet
            },
            sortable_attributes: Setting::NotSet,
            ranking_rules: match settings.ranking_rules {
                Some(Some(ranking_rules)) => Setting::Set(ranking_rules.into_iter().filter_map(|criterion| {
                    match criterion.as_str() {
                        "words" | "typo" | "proximity" | "attribute" | "exactness" => Some(criterion),
                        s if s.starts_with("asc") => asc_ranking_rule(s).map(|f| format!("{}:asc", f)),
                        s if s.starts_with("desc") => desc_ranking_rule(s).map(|f| format!("{}:desc", f)),
                        "wordsPosition" => {
                            warn!("The criteria `attribute` and `wordsPosition` have been merged \
                                into a single criterion `attribute` so `wordsPositon` will be \
                                ignored");
                            None
                        }
                        s => {
                            error!("Unknown criterion found in the dump: `{}`, it will be ignored", s);
                            None
                        }
                    }
                }).collect()),
                Some(None) => Setting::Reset,
                None => Setting::NotSet
            },
            // we need to convert the old `Vec<String>` into a `BTreeSet<String>`
            stop_words: match settings.stop_words {
                Some(Some(stop_words)) => Setting::Set(stop_words.into_iter().collect()),
                Some(None) => Setting::Reset,
                None => Setting::NotSet
            },
            // we need to convert the old `Vec<String>` into a `BTreeMap<String>`
            synonyms: match settings.synonyms {
                Some(Some(synonyms)) => Setting::Set(synonyms.into_iter().collect()),
                Some(None) => Setting::Reset,
                None => Setting::NotSet
            },
            _kind: PhantomData,
        }
    }
}

// /// Extract Settings from `settings.json` file present at provided `dir_path`
//fn import_settings(dir_path: impl AsRef<Path>) -> anyhow::Result<Settings> {
    //let path = dir_path.as_ref().join("settings.json");
    //let file = File::open(path)?;
    //let reader = std::io::BufReader::new(file);
    //let metadata = serde_json::from_reader(reader)?;

    //Ok(metadata)
//}

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
