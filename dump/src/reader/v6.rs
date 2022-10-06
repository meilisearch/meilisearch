use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
    str::FromStr,
};

use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Error, IndexMetadata, Result, Version};

use super::{DumpReader, IndexReader};

pub type Metadata = crate::Metadata;

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type Settings<T> = index::Settings<T>;
pub type Checked = index::Checked;
pub type Unchecked = index::Unchecked;

pub type Task = index_scheduler::TaskView;
pub type UpdateFile = File;
pub type Key = meilisearch_auth::Key;

// ===== Other types to clarify the code of the compat module
// everything related to the tasks
pub type Status = index_scheduler::Status;
pub type Kind = index_scheduler::Kind;
pub type Details = index_scheduler::Details;

// everything related to the settings
pub type Setting<T> = index::Setting<T>;
pub type TypoTolerance = index::updates::TypoSettings;
pub type MinWordSizeForTypos = index::updates::MinWordSizeTyposSetting;
pub type FacetingSettings = index::updates::FacetingSettings;
pub type PaginationSettings = index::updates::PaginationSettings;

// everything related to the api keys
pub type Action = meilisearch_auth::Action;
pub type StarOr<T> = meilisearch_types::star_or::StarOr<T>;
pub type IndexUid = meilisearch_types::index_uid::IndexUid;

// everything related to the errors
pub type ResponseError = meilisearch_types::error::ResponseError;
pub type Code = meilisearch_types::error::Code;

pub struct V6Reader {
    dump: TempDir,
    instance_uid: Uuid,
    metadata: Metadata,
    tasks: BufReader<File>,
    keys: BufReader<File>,
}

impl V6Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let instance_uid = fs::read_to_string(dump.path().join("instance_uid.uuid"))?;
        let instance_uid = Uuid::from_str(&instance_uid)?;

        Ok(V6Reader {
            metadata: serde_json::from_reader(&*meta_file)?,
            instance_uid,
            tasks: BufReader::new(File::open(dump.path().join("tasks").join("queue.jsonl"))?),
            keys: BufReader::new(File::open(dump.path().join("keys.jsonl"))?),
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
        Ok(Some(self.instance_uid))
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<V6IndexReader>> + '_>> {
        let entries = fs::read_dir(self.dump.path().join("indexes"))?;
        Ok(Box::new(
            entries
                .map(|entry| -> Result<Option<_>> {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        let index = V6IndexReader::new(
                            entry
                                .file_name()
                                .to_str()
                                .ok_or(Error::BadIndexName)?
                                .to_string(),
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

    pub fn tasks(&mut self) -> Box<dyn Iterator<Item = Result<(Task, Option<UpdateFile>)>> + '_> {
        Box::new((&mut self.tasks).lines().map(|line| -> Result<_> {
            let mut task: index_scheduler::TaskView = serde_json::from_str(&line?)?;
            // TODO: this can be removed once we can `Deserialize` the duration from the `TaskView`.
            if let Some((started_at, finished_at)) = task.started_at.zip(task.finished_at) {
                task.duration = Some(finished_at - started_at);
            }
            let update_file_path = self
                .dump
                .path()
                .join("tasks")
                .join("update_files")
                .join(task.uid.to_string());

            if update_file_path.exists() {
                Ok((task, Some(File::open(update_file_path)?)))
            } else {
                Ok((task, None))
            }
        }))
    }

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<Key>> + '_> {
        Box::new(
            (&mut self.keys)
                .lines()
                .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) }),
        )
    }
}

impl DumpReader for V6Reader {
    fn version(&self) -> Version {
        self.version()
    }

    fn date(&self) -> Option<OffsetDateTime> {
        self.date()
    }

    fn instance_uid(&self) -> Result<Option<Uuid>> {
        self.instance_uid()
    }

    fn indexes(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Box<dyn super::IndexReader + '_>>> + '_>> {
        self.indexes().map(|iter| {
            Box::new(iter.map(|result| {
                result.map(|index| Box::new(index) as Box<dyn super::IndexReader + '_>)
            })) as Box<dyn Iterator<Item = _>>
        })
    }

    fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(self::Task, Option<self::UpdateFile>)>> + '_> {
        Box::new(self.tasks())
    }

    fn keys(&mut self) -> Box<dyn Iterator<Item = Result<self::Key>> + '_> {
        Box::new(self.keys())
    }
}

pub struct V6IndexReader {
    metadata: IndexMetadata,
    documents: BufReader<File>,
    settings: BufReader<File>,
}

impl V6IndexReader {
    pub fn new(name: String, path: &Path) -> Result<Self> {
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

impl IndexReader for V6IndexReader {
    fn metadata(&self) -> &IndexMetadata {
        self.metadata()
    }

    fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        self.documents()
            .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>)
    }

    fn settings(&mut self) -> Result<Settings<Checked>> {
        self.settings()
    }
}
