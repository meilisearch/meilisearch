//! ```text
//! .
//! ├── indexes
//! │   ├── 01d7dd17-8241-4f1f-a7d1-2d1cb255f5b0
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   ├── 78be64a3-cae1-449e-b7ed-13e77c9a8a0c
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   ├── ba553439-18fe-4733-ba53-44eed898280c
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   └── c408bc22-5859-49d1-8e9f-c88e2fa95cb0
//! │       ├── documents.jsonl
//! │       └── meta.json
//! ├── index_uuids
//! │   └── data.jsonl
//! ├── metadata.json
//! └── updates
//!     ├── data.jsonl
//!     └── updates_files
//!         └── 66d3f12d-fcf3-4b53-88cb-407017373de7
//! ```

use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

pub mod errors;
mod meta;
pub mod settings;
pub mod updates;

use crate::{IndexMetadata, Result, Version};

use self::meta::{DumpMeta, IndexUuid};

use super::{DumpReader, IndexReader};

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type Settings<T> = settings::Settings<T>;
pub type Checked = settings::Checked;
pub type Unchecked = settings::Unchecked;

pub type Task = updates::UpdateEntry;
pub type UpdateFile = File;

// ===== Other types to clarify the code of the compat module
// everything related to the tasks
pub type Status = updates::UpdateStatus;
pub type Kind = updates::Update;
pub type Details = updates::UpdateResult;

// everything related to the settings
pub type Setting<T> = settings::Setting<T>;

// everything related to the errors
// pub type ResponseError = errors::ResponseError;
pub type Code = meilisearch_types::error::Code;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    #[serde(with = "time::serde::rfc3339")]
    dump_date: OffsetDateTime,
}

pub struct V3Reader {
    dump: TempDir,
    metadata: Metadata,
    tasks: BufReader<File>,
    index_uuid: Vec<IndexUuid>,
}

impl V3Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let metadata = serde_json::from_reader(&*meta_file)?;
        let index_uuid = File::open(dump.path().join("index_uuids/data.jsonl"))?;
        let index_uuid = BufReader::new(index_uuid);
        let index_uuid = index_uuid
            .lines()
            .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) })
            .collect::<Result<Vec<_>>>()?;

        Ok(V3Reader {
            metadata,
            tasks: BufReader::new(
                File::open(dump.path().join("updates").join("data.jsonl")).unwrap(),
            ),
            index_uuid,
            dump,
        })
    }

    // pub fn to_v4(self) -> CompatV3ToV4 {
    //     CompatV3ToV4::new(self)
    // }

    pub fn version(&self) -> Version {
        Version::V3
    }

    pub fn date(&self) -> Option<OffsetDateTime> {
        Some(self.metadata.dump_date)
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<V3IndexReader>> + '_> {
        Ok(self.index_uuid.iter().map(|index| -> Result<_> {
            Ok(V3IndexReader::new(
                index.uid.clone(),
                &self
                    .dump
                    .path()
                    .join("indexes")
                    .join(index.uuid.to_string()),
            )?)
        }))
    }

    pub fn tasks(&mut self) -> Box<dyn Iterator<Item = Result<(Task, Option<UpdateFile>)>> + '_> {
        Box::new((&mut self.tasks).lines().map(|line| -> Result<_> {
            let task: Task = serde_json::from_str(&line?)?;
            if !task.is_finished() {
                if let Some(uuid) = task.get_content_uuid() {
                    let update_file_path = self
                        .dump
                        .path()
                        .join("updates")
                        .join("updates_files")
                        .join(uuid.to_string());
                    Ok((task, Some(File::open(update_file_path).unwrap())))
                } else {
                    Ok((task, None))
                }
            } else {
                Ok((task, None))
            }
        }))
    }
}

pub struct V3IndexReader {
    metadata: IndexMetadata,
    settings: Settings<Checked>,

    documents: BufReader<File>,
}

impl V3IndexReader {
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

        let ret = V3IndexReader {
            metadata,
            settings: meta.settings.check(),
            documents: BufReader::new(File::open(path.join("documents.jsonl"))?),
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
        Ok(self.settings.clone())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{fs::File, io::BufReader};

    use flate2::bufread::GzDecoder;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn read_dump_v3() {
        let dump = File::open("tests/assets/v3.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = V3Reader::open(dir).unwrap();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-07 11:39:03.709153554 +00:00:00");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        insta::assert_json_snapshot!(tasks);
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies2 = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_debug_snapshot!(products.settings());
        let documents = products
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        insta::assert_json_snapshot!(documents);

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_debug_snapshot!(movies.settings());
        let documents = movies
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 110);
        insta::assert_debug_snapshot!(documents);

        // movies2
        insta::assert_json_snapshot!(movies2.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies_2",
          "primaryKey": null,
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_debug_snapshot!(movies2.settings());
        let documents = movies2
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 0);
        insta::assert_debug_snapshot!(documents);

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_debug_snapshot!(spells.settings());
        let documents = spells
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        insta::assert_json_snapshot!(documents);
    }
}
