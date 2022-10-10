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

use super::Document;
pub use index;

pub type Metadata = crate::Metadata;

pub type Settings<T> = index::Settings<T>;
pub type Checked = index::Checked;
pub type Unchecked = index::Unchecked;

pub type Task = index_scheduler::TaskView;
pub type Key = meilisearch_auth::Key;

// ===== Other types to clarify the code of the compat module
// everything related to the tasks
pub type Status = index_scheduler::Status;
pub type Kind = index_scheduler::Kind;
pub type Details = index_scheduler::Details;

// everything related to the settings
pub type Setting<T> = index::milli::update::Setting<T>;
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

    pub fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(Task, Option<Box<super::UpdateFile>>)>> + '_> {
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
                .join(format!("{}.jsonl", task.uid.to_string()));

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

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<Key>> + '_> {
        Box::new(
            (&mut self.keys)
                .lines()
                .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) }),
        )
    }
}

pub struct UpdateFile {
    reader: BufReader<File>,
}

impl UpdateFile {
    fn new(path: &Path) -> Result<Self> {
        Ok(UpdateFile {
            reader: BufReader::new(File::open(path)?),
        })
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
