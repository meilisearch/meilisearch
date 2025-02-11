use std::fs::{self, File};
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::Path;

pub use meilisearch_types::milli;
use tempfile::TempDir;
use time::OffsetDateTime;
use tracing::debug;
use uuid::Uuid;

use super::Document;
use crate::{Error, IndexMetadata, Result, Version};

pub type Metadata = crate::Metadata;

pub type Settings<T> = meilisearch_types::settings::Settings<T>;
pub type Checked = meilisearch_types::settings::Checked;
pub type Unchecked = meilisearch_types::settings::Unchecked;

pub type Task = crate::TaskDump;
pub type Batch = meilisearch_types::batches::Batch;
pub type Key = meilisearch_types::keys::Key;
pub type RuntimeTogglableFeatures = meilisearch_types::features::RuntimeTogglableFeatures;
pub type Network = meilisearch_types::features::Network;

// ===== Other types to clarify the code of the compat module
// everything related to the tasks
pub type Status = meilisearch_types::tasks::Status;
pub type Kind = crate::KindDump;
pub type Details = meilisearch_types::tasks::Details;

// everything related to the settings
pub type Setting<T> = meilisearch_types::milli::update::Setting<T>;
pub type TypoTolerance = meilisearch_types::settings::TypoSettings;
pub type MinWordSizeForTypos = meilisearch_types::settings::MinWordSizeTyposSetting;
pub type FacetingSettings = meilisearch_types::settings::FacetingSettings;
pub type PaginationSettings = meilisearch_types::settings::PaginationSettings;

// everything related to the api keys
pub type Action = meilisearch_types::keys::Action;
pub type IndexUidPattern = meilisearch_types::index_uid_pattern::IndexUidPattern;

// everything related to the errors
pub type ResponseError = meilisearch_types::error::ResponseError;
pub type Code = meilisearch_types::error::Code;
pub type RankingRuleView = meilisearch_types::settings::RankingRuleView;

pub struct V6Reader {
    dump: TempDir,
    instance_uid: Option<Uuid>,
    metadata: Metadata,
    tasks: BufReader<File>,
    batches: Option<BufReader<File>>,
    keys: BufReader<File>,
    features: Option<RuntimeTogglableFeatures>,
    network: Option<Network>,
}

impl V6Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let instance_uid = match fs::read_to_string(dump.path().join("instance_uid.uuid")) {
            Ok(uuid) => Some(Uuid::parse_str(&uuid)?),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };

        let feature_file = match fs::read(dump.path().join("experimental-features.json")) {
            Ok(feature_file) => Some(feature_file),
            Err(error) => match error.kind() {
                // Allows the file to be missing, this will only result in all experimental features disabled.
                ErrorKind::NotFound => {
                    debug!("`experimental-features.json` not found in dump");
                    None
                }
                _ => return Err(error.into()),
            },
        };
        let features = if let Some(feature_file) = feature_file {
            Some(serde_json::from_reader(&*feature_file)?)
        } else {
            None
        };
        let batches = match File::open(dump.path().join("batches").join("queue.jsonl")) {
            Ok(file) => Some(BufReader::new(file)),
            // The batch file was only introduced during the v1.13, anything prior to that won't have batches
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };

        let network_file = match fs::read(dump.path().join("network.json")) {
            Ok(network_file) => Some(network_file),
            Err(error) => match error.kind() {
                // Allows the file to be missing, this will only result in all experimental features disabled.
                ErrorKind::NotFound => {
                    debug!("`network.json` not found in dump");
                    None
                }
                _ => return Err(error.into()),
            },
        };
        let network = if let Some(network_file) = network_file {
            Some(serde_json::from_reader(&*network_file)?)
        } else {
            None
        };

        Ok(V6Reader {
            metadata: serde_json::from_reader(&*meta_file)?,
            instance_uid,
            tasks: BufReader::new(File::open(dump.path().join("tasks").join("queue.jsonl"))?),
            batches,
            keys: BufReader::new(File::open(dump.path().join("keys.jsonl"))?),
            features,
            network,
            dump,
        })
    }

    pub fn version(&self) -> Version {
        Version::V6
    }

    pub fn date(&self) -> Option<OffsetDateTime> {
        Some(self.metadata.dump_date)
    }

    pub fn instance_uid(&self) -> Result<Option<Uuid>> {
        Ok(self.instance_uid)
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<V6IndexReader>> + '_>> {
        let entries = fs::read_dir(self.dump.path().join("indexes"))?;
        Ok(Box::new(
            entries
                .map(|entry| -> Result<Option<_>> {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        let index = V6IndexReader::new(
                            entry.file_name().to_str().ok_or(Error::BadIndexName)?.to_string(),
                            &entry.path(),
                        )?;
                        Ok(Some(index))
                    } else {
                        Ok(None)
                    }
                })
                .filter_map(|entry| entry.transpose()),
        ))
    }

    pub fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(Task, Option<Box<super::UpdateFile>>)>> + '_> {
        Box::new((&mut self.tasks).lines().map(|line| -> Result<_> {
            let task: Task = serde_json::from_str(&line?)?;

            let update_file_path = self
                .dump
                .path()
                .join("tasks")
                .join("update_files")
                .join(format!("{}.jsonl", task.uid));

            if update_file_path.exists() {
                Ok((
                    task,
                    Some(Box::new(UpdateFile::new(&update_file_path)?) as Box<super::UpdateFile>),
                ))
            } else {
                Ok((task, None))
            }
        }))
    }

    pub fn batches(&mut self) -> Box<dyn Iterator<Item = Result<Batch>> + '_> {
        match self.batches.as_mut() {
            Some(batches) => Box::new((batches).lines().map(|line| -> Result<_> {
                let batch = serde_json::from_str(&line?)?;
                Ok(batch)
            })),
            None => Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Result<Batch>> + '_>,
        }
    }

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<Key>> + '_> {
        Box::new(
            (&mut self.keys).lines().map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) }),
        )
    }

    pub fn features(&self) -> Option<RuntimeTogglableFeatures> {
        self.features
    }

    pub fn network(&self) -> Option<&Network> {
        self.network.as_ref()
    }
}

pub struct UpdateFile {
    reader: BufReader<File>,
}

impl UpdateFile {
    fn new(path: &Path) -> Result<Self> {
        Ok(UpdateFile { reader: BufReader::new(File::open(path)?) })
    }
}

impl Iterator for UpdateFile {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        (&mut self.reader)
            .lines()
            .map(|line| {
                line.map_err(Error::from)
                    .and_then(|line| serde_json::from_str(&line).map_err(Error::from))
            })
            .next()
    }
}

pub struct V6IndexReader {
    metadata: IndexMetadata,
    documents: BufReader<File>,
    settings: BufReader<File>,
}

impl V6IndexReader {
    pub fn new(_name: String, path: &Path) -> Result<Self> {
        let metadata = File::open(path.join("metadata.json"))?;

        let ret = V6IndexReader {
            metadata: serde_json::from_reader(metadata)?,
            documents: BufReader::new(File::open(path.join("documents.jsonl"))?),
            settings: BufReader::new(File::open(path.join("settings.json"))?),
        };

        Ok(ret)
    }

    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    pub fn documents(&mut self) -> Result<impl Iterator<Item = Result<Document>> + '_> {
        Ok((&mut self.documents)
            .lines()
            .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) }))
    }

    pub fn settings(&mut self) -> Result<Settings<Checked>> {
        let settings: Settings<Unchecked> = serde_json::from_reader(&mut self.settings)?;
        Ok(settings.check())
    }
}
