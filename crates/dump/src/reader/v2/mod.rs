//! ```text
//! .
//! ├── indexes
//! │   ├── index-40d14c5f-37ae-4873-9d51-b69e014a0d30
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   ├── index-88202369-4524-4410-9b3d-3e924c867fec
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   ├── index-b7f2d03b-bf9b-40d9-a25b-94dc5ec60c32
//! │   │   ├── documents.jsonl
//! │   │   └── meta.json
//! │   └── index-dc9070b3-572d-4f30-ab45-d4903ab71708
//! │       ├── documents.jsonl
//! │       └── meta.json
//! ├── index_uuids
//! │   └── data.jsonl
//! ├── metadata.json
//! └── updates
//!     ├── data.jsonl
//!     └── update_files
//!         └── update_202573df-718b-4d80-9a65-2ee397c23dc3
//! ```

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use time::OffsetDateTime;

pub mod errors;
pub mod meta;
pub mod settings;
pub mod updates;

use self::meta::{DumpMeta, IndexUuid};
use super::compat::v2_to_v3::CompatV2ToV3;
use super::Document;
use crate::{IndexMetadata, Result, Version};

pub type Settings<T> = settings::Settings<T>;
pub type Setting<T> = settings::Setting<T>;
pub type Checked = settings::Checked;
pub type Unchecked = settings::Unchecked;

pub type Task = updates::UpdateEntry;
pub type Kind = updates::UpdateMeta;

// everything related to the errors
pub type ResponseError = errors::ResponseError;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    #[serde(with = "time::serde::rfc3339")]
    dump_date: OffsetDateTime,
}

pub struct V2Reader {
    dump: TempDir,
    metadata: Metadata,
    tasks: BufReader<File>,
    pub index_uuid: Vec<IndexUuid>,
}

impl V2Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let metadata = serde_json::from_reader(&*meta_file)?;
        let index_uuid = File::open(dump.path().join("index_uuids/data.jsonl"))?;
        let index_uuid = BufReader::new(index_uuid);
        let index_uuid = index_uuid
            .lines()
            .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) })
            .collect::<Result<Vec<_>>>()?;

        Ok(V2Reader {
            metadata,
            tasks: BufReader::new(
                File::open(dump.path().join("updates").join("data.jsonl")).unwrap(),
            ),
            index_uuid,
            dump,
        })
    }

    pub fn to_v3(self) -> CompatV2ToV3 {
        CompatV2ToV3::new(self)
    }

    pub fn index_uuid(&self) -> Vec<IndexUuid> {
        self.index_uuid.clone()
    }

    pub fn version(&self) -> Version {
        Version::V2
    }

    pub fn date(&self) -> Option<OffsetDateTime> {
        Some(self.metadata.dump_date)
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<V2IndexReader>> + '_> {
        Ok(self.index_uuid.iter().map(|index| -> Result<_> {
            V2IndexReader::new(
                &self.dump.path().join("indexes").join(format!("index-{}", index.uuid)),
                index,
                BufReader::new(
                    File::open(self.dump.path().join("updates").join("data.jsonl")).unwrap(),
                ),
            )
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
                        .join("update_files")
                        .join(format!("update_{}", uuid));
                    Ok((task, Some(UpdateFile::new(&update_file_path)?)))
                } else {
                    Ok((task, None))
                }
            } else {
                Ok((task, None))
            }
        }))
    }
}

pub struct V2IndexReader {
    metadata: IndexMetadata,
    settings: Settings<Checked>,

    documents: BufReader<File>,
}

impl V2IndexReader {
    pub fn new(path: &Path, index_uuid: &IndexUuid, tasks: BufReader<File>) -> Result<Self> {
        let meta = File::open(path.join("meta.json"))?;
        let meta: DumpMeta = serde_json::from_reader(meta)?;

        let mut created_at = None;
        let mut updated_at = None;

        for line in tasks.lines() {
            let task: Task = serde_json::from_str(&line?)?;
            if !(task.uuid == index_uuid.uuid && task.is_finished()) {
                continue;
            }

            let new_created_at = match task.update.meta() {
                Kind::DocumentsAddition { .. } | Kind::Settings(_) => task.update.finished_at(),
                _ => None,
            };
            let new_updated_at = task.update.finished_at();

            if created_at.is_none() || created_at > new_created_at {
                created_at = new_created_at;
            }

            if updated_at.is_none() || updated_at < new_updated_at {
                updated_at = new_updated_at;
            }
        }

        let current_time = OffsetDateTime::now_utc();

        let metadata = IndexMetadata {
            uid: index_uuid.uid.clone(),
            primary_key: meta.primary_key,
            created_at: created_at.unwrap_or(current_time),
            updated_at: updated_at.unwrap_or(current_time),
        };

        let ret = V2IndexReader {
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

pub struct UpdateFile {
    documents: Vec<Document>,
    index: usize,
}

impl UpdateFile {
    fn new(path: &Path) -> Result<Self> {
        let reader = BufReader::new(File::open(path)?);
        Ok(UpdateFile { documents: serde_json::from_reader(reader)?, index: 0 })
    }
}

impl Iterator for UpdateFile {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.documents.get(self.index - 1).cloned().map(Ok)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs::File;
    use std::io::BufReader;

    use flate2::bufread::GzDecoder;
    use meili_snap::insta;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn read_dump_v2() {
        let dump = File::open("tests/assets/v2.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = V2Reader::open(dir).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2022-10-09 20:27:59.904096267 +00:00:00");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, mut update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"ec5fc0a14bf735ad4e361d5aa8a89ac6");
        assert_eq!(update_files.len(), 9);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        let update_file = update_files.remove(0).unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(update_file), @"7b8889539b669c7b9ddba448bafa385d");

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
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2022-10-09T20:27:22.688964637Z",
          "updatedAt": "2022-10-09T20:27:23.951017769Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2022-10-09T20:27:22.197788495Z",
          "updatedAt": "2022-10-09T20:28:01.93111053Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d153b5a81d8b3cdcbe1dec270b574022");

        // movies2
        insta::assert_json_snapshot!(movies2.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies_2",
          "primaryKey": null,
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_json_snapshot!(movies2.settings().unwrap());
        let documents = movies2.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 0);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d751713988987e9331980363e24189ce");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2022-10-09T20:27:24.242683494Z",
          "updatedAt": "2022-10-09T20:27:24.312809641Z"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }

    #[test]
    fn read_dump_v2_from_meilisearch_v0_22_0_issue_3435() {
        let dump = File::open("tests/assets/v2-v0.22.0.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = V2Reader::open(dir).unwrap();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2023-01-30 16:26:09.247261 +00:00:00");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"aca8ba13046272664eb3ea2da3031633");
        assert_eq!(update_files.len(), 8);
        assert!(update_files[0..].iter().all(|u| u.is_none())); // everything has already been processed

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2023-01-30T16:25:56.595257Z",
          "updatedAt": "2023-01-30T16:25:58.70348Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2023-01-30T16:25:56.192178Z",
          "updatedAt": "2023-01-30T16:25:56.455714Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"0227598af846e574139ee0b80e03a720");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2023-01-30T16:25:58.876405Z",
          "updatedAt": "2023-01-30T16:25:59.079906Z"
        }
        "###);

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
