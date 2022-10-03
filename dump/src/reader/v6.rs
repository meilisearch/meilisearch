use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
};

use index::{Checked, Unchecked};
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Error, Result, Version};

use super::{DumpReader, IndexReader};

type Metadata = crate::Metadata;

pub fn date(dump: &Path) -> Result<OffsetDateTime> {
    let metadata = fs::read(dump.join("metadata.json"))?;
    let metadata: Metadata = serde_json::from_reader(&*metadata)?;
    Ok(metadata.dump_date)
}

pub struct V6Reader {
    dump: TempDir,
    metadata: Metadata,
    tasks: BufReader<File>,
    keys: BufReader<File>,
}

struct V6IndexReader {
    name: String,
    documents: BufReader<File>,
    settings: BufReader<File>,
}

impl V6IndexReader {
    pub fn new(name: String, path: &Path) -> Result<Self> {
        let ret = V6IndexReader {
            name,
            documents: BufReader::new(File::open(path.join("documents.jsonl"))?),
            settings: BufReader::new(File::open(path.join("settings.json"))?),
        };

        Ok(ret)
    }
}

impl V6Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let metadata = serde_json::from_reader(&*meta_file)?;

        Ok(V6Reader {
            metadata,
            tasks: BufReader::new(File::open(dump.path().join("tasks").join("queue.jsonl"))?),
            keys: BufReader::new(File::open(dump.path().join("keys.jsonl"))?),
            dump,
        })
    }
}

impl DumpReader for V6Reader {
    type Document = serde_json::Map<String, serde_json::Value>;
    type Settings = index::Settings<Checked>;

    type Task = index_scheduler::TaskView;
    type UpdateFile = File;

    type Key = meilisearch_auth::Key;

    fn version(&self) -> Version {
        Version::V6
    }

    fn date(&self) -> Option<OffsetDateTime> {
        Some(self.metadata.dump_date)
    }

    fn instance_uid(&self) -> Result<Option<Uuid>> {
        let uuid = fs::read_to_string(self.dump.path().join("instance-uid"))?;
        Ok(Some(Uuid::parse_str(&uuid)?))
    }

    fn indexes(
        &self,
    ) -> Result<
        Box<
            dyn Iterator<
                Item = Result<
                    Box<
                        dyn super::IndexReader<
                            Document = Self::Document,
                            Settings = Self::Settings,
                        >,
                    >,
                >,
            >,
        >,
    > {
        let entries = fs::read_dir(self.dump.path().join("indexes"))?;
        Ok(Box::new(
            entries
                .map(|entry| -> Result<Option<_>> {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        let index = Box::new(V6IndexReader::new(
                            entry
                                .file_name()
                                .to_str()
                                .ok_or(Error::BadIndexName)?
                                .to_string(),
                            &entry.path(),
                        )?)
                            as Box<
                                dyn IndexReader<
                                    Document = Self::Document,
                                    Settings = Self::Settings,
                                >,
                            >;
                        Ok(Some(index))
                    } else {
                        Ok(None)
                    }
                })
                .filter_map(|entry| entry.transpose()),
        ))
    }

    fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(Self::Task, Option<Self::UpdateFile>)>> + '_> {
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

    fn keys(&mut self) -> Box<dyn Iterator<Item = Result<Self::Key>> + '_> {
        Box::new(
            (&mut self.keys)
                .lines()
                .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) }),
        )
    }
}

impl IndexReader for V6IndexReader {
    type Document = serde_json::Map<String, serde_json::Value>;
    type Settings = index::Settings<Checked>;

    fn name(&self) -> &str {
        &self.name
    }

    fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Self::Document>> + '_>> {
        Ok(Box::new((&mut self.documents).lines().map(
            |line| -> Result<_> { Ok(serde_json::from_str(&line?)?) },
        )))
    }

    fn settings(&mut self) -> Result<Self::Settings> {
        let settings: index::Settings<Unchecked> = serde_json::from_reader(&mut self.settings)?;
        Ok(settings.check())
    }
}
