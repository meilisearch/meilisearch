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

use super::{compat::v5_to_v6::CompatV5ToV6, DumpReader, IndexReader};

mod keys;
mod meta;
mod settings;
mod tasks;

pub use keys::*;
pub use meta::*;
pub use settings::*;
pub use tasks::*;

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type UpdateFile = File;

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
            tasks: BufReader::new(
                File::open(dump.path().join("updates").join("data.jsonl")).unwrap(),
            ),
            keys: BufReader::new(File::open(dump.path().join("keys"))?),
            index_uuid,
            dump,
        })
    }

    pub fn to_v6(self) -> CompatV5ToV6 {
        CompatV5ToV6::new(self)
    }

    pub fn version(&self) -> Version {
        Version::V5
    }

    pub fn date(&self) -> Option<OffsetDateTime> {
        Some(self.metadata.dump_date)
    }

    pub fn instance_uid(&self) -> Result<Option<Uuid>> {
        let uuid = fs::read_to_string(self.dump.path().join("instance-uid"))?;
        Ok(Some(Uuid::parse_str(&uuid)?))
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<V5IndexReader>> + '_> {
        Ok(self.index_uuid.iter().map(|index| -> Result<_> {
            Ok(V5IndexReader::new(
                index.uid.clone(),
                &self
                    .dump
                    .path()
                    .join("indexes")
                    .join(index.index_meta.uuid.to_string()),
            )?)
        }))
    }

    pub fn tasks(&mut self) -> impl Iterator<Item = Result<(Task, Option<UpdateFile>)>> + '_ {
        (&mut self.tasks).lines().map(|line| -> Result<_> {
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
        })
    }

    pub fn keys(&mut self) -> impl Iterator<Item = Result<Key>> + '_ {
        (&mut self.keys)
            .lines()
            .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) })
    }
}

pub struct V5IndexReader {
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
    fn read_dump_v5() {
        let dump = File::open("tests/assets/v5.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = V5Reader::open(dir).unwrap();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-04 15:55:10.344982459 +00:00:00");
        insta::assert_display_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        insta::assert_json_snapshot!(tasks);
        assert_eq!(update_files.len(), 22);
        assert!(update_files[0].is_none()); // the dump creation
        assert!(update_files[1].is_some()); // the enqueued document addition
        assert!(update_files[2..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        insta::assert_json_snapshot!(keys);

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
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
        assert_eq!(documents.len(), 200);
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
