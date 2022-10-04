//! Here is what a dump v5 look like.
//!
//! ```text
//! .
//! ├── indexes
//! │   ├── 22c269d8-fbbd-4416-bd46-7c7c02849325
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   ├── 6d0471ba-2ed1-41de-8ea6-10db10fa2bb8
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   └── f7d53ec4-0748-48e6-b66f-1fca9944b0fa
//! │       ├── documents.jsonl
//! │       └── meta.json
//! ├── index_uuids
//! │   └── data.jsonl
//! ├── instance-uid
//! ├── keys
//! ├── metadata.json
//! └── updates
//!     ├── data.jsonl
//!     └── updates_files
//!         └── c83a004a-da98-4b94-b245-3256266c7281
//! ```
//!
//! Here is what `index_uuids/data.jsonl` looks like;
//!
//! ```json
//! {"uid":"dnd_spells","index_meta":{"uuid":"22c269d8-fbbd-4416-bd46-7c7c02849325","creation_task_id":9}}
//! {"uid":"movies","index_meta":{"uuid":"6d0471ba-2ed1-41de-8ea6-10db10fa2bb8","creation_task_id":1}}
//! {"uid":"products","index_meta":{"uuid":"f7d53ec4-0748-48e6-b66f-1fca9944b0fa","creation_task_id":4}}
//! ```
//!

use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{IndexMetadata, Result, Version};

use self::{
    keys::Key,
    meta::{DumpMeta, IndexUuid},
    settings::{Checked, Settings, Unchecked},
    tasks::Task,
};

use super::{DumpReader, IndexReader};

mod keys;
mod meta;
mod settings;
mod tasks;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    #[serde(with = "time::serde::rfc3339")]
    dump_date: OffsetDateTime,
}

pub struct V5Reader {
    dump: TempDir,
    metadata: Metadata,
    tasks: BufReader<File>,
    keys: BufReader<File>,
    index_uuid: Vec<IndexUuid>,
}

impl V5Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let metadata = serde_json::from_reader(&*meta_file)?;
        let index_uuid = File::open(dump.path().join("index_uuids/data.jsonl"))?;
        let index_uuid = BufReader::new(index_uuid);
        let index_uuid = index_uuid
            .lines()
            .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) })
            .collect::<Result<Vec<_>>>()?;

        Ok(V5Reader {
            metadata,
            tasks: BufReader::new(File::open(dump.path().join("tasks").join("queue.jsonl"))?),
            keys: BufReader::new(File::open(dump.path().join("keys.jsonl"))?),
            index_uuid,
            dump,
        })
    }
}

impl DumpReader for V5Reader {
    type Document = serde_json::Map<String, serde_json::Value>;
    type Settings = Settings<Checked>;

    type Task = Task;
    type UpdateFile = File;

    type Key = Key;

    fn version(&self) -> Version {
        Version::V5
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
                                > + '_,
                        >,
                    >,
                > + '_,
        >,
    > {
        Ok(Box::new(self.index_uuid.iter().map(|index| -> Result<_> {
            Ok(Box::new(V5IndexReader::new(
                index.uid.clone(),
                &self
                    .dump
                    .path()
                    .join("indexes")
                    .join(index.index_meta.uuid.to_string()),
            )?)
                as Box<
                    dyn IndexReader<Document = Self::Document, Settings = Self::Settings>,
                >)
        })))
    }

    fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(Self::Task, Option<Self::UpdateFile>)>> + '_> {
        Box::new((&mut self.tasks).lines().map(|line| -> Result<_> {
            let task: Self::Task = serde_json::from_str(&line?)?;
            if let Some(uuid) = task.get_content_uuid() {
                let update_file_path = self
                    .dump
                    .path()
                    .join("updates")
                    .join("update_files")
                    .join(uuid.to_string());
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

struct V5IndexReader {
    metadata: IndexMetadata,
    settings: Settings<Checked>,

    documents: BufReader<File>,
}

impl V5IndexReader {
    pub fn new(name: String, path: &Path) -> Result<Self> {
        let meta = File::open(path.join("meta.json"))?;
        let meta: DumpMeta = serde_json::from_reader(meta)?;

        let metadata = IndexMetadata {
            uid: name,
            primary_key: meta.primary_key,
            // FIXME: Iterate over the whole task queue to find the creation and last update date.
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
        };

        let ret = V5IndexReader {
            metadata,
            settings: meta.settings.check(),
            documents: BufReader::new(File::open(path.join("documents.jsonl"))?),
        };

        Ok(ret)
    }
}

impl IndexReader for V5IndexReader {
    type Document = serde_json::Map<String, serde_json::Value>;
    type Settings = Settings<Checked>;

    fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Self::Document>> + '_>> {
        Ok(Box::new((&mut self.documents).lines().map(
            |line| -> Result<_> { Ok(serde_json::from_str(&line?)?) },
        )))
    }

    fn settings(&mut self) -> Result<Self::Settings> {
        Ok(self.settings.clone())
    }
}
