use std::fs::{self, File};
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

pub mod errors;
pub mod keys;
pub mod meta;
pub mod settings;
pub mod tasks;

use self::meta::{DumpMeta, IndexUuid};
use super::compat::v4_to_v5::CompatV4ToV5;
use crate::{Error, IndexMetadata, Result, Version};

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type Settings<T> = settings::Settings<T>;
pub type Checked = settings::Checked;
pub type Unchecked = settings::Unchecked;

pub type Task = tasks::Task;
pub type Key = keys::Key;

// everything related to the settings
pub type Setting<T> = settings::Setting<T>;

// everything related to the api keys
pub type Action = keys::Action;

// everything related to the errors
pub type ResponseError = errors::ResponseError;
pub type Code = errors::Code;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    #[serde(with = "time::serde::rfc3339")]
    dump_date: OffsetDateTime,
}

pub struct V4Reader {
    dump: TempDir,
    metadata: Metadata,
    tasks: BufReader<File>,
    keys: BufReader<File>,
    index_uuid: Vec<IndexUuid>,
}

impl V4Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let metadata = serde_json::from_reader(&*meta_file)?;
        let index_uuid = File::open(dump.path().join("index_uuids/data.jsonl"))?;
        let index_uuid = BufReader::new(index_uuid);
        let index_uuid = index_uuid
            .lines()
            .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) })
            .collect::<Result<Vec<_>>>()?;

        Ok(V4Reader {
            metadata,
            tasks: BufReader::new(
                File::open(dump.path().join("updates").join("data.jsonl")).unwrap(),
            ),
            keys: BufReader::new(File::open(dump.path().join("keys"))?),
            index_uuid,
            dump,
        })
    }

    pub fn to_v5(self) -> CompatV4ToV5 {
        CompatV4ToV5::new(self)
    }

    pub fn version(&self) -> Version {
        Version::V4
    }

    pub fn date(&self) -> Option<OffsetDateTime> {
        Some(self.metadata.dump_date)
    }

    pub fn instance_uid(&self) -> Result<Option<Uuid>> {
        match fs::read_to_string(self.dump.path().join("instance-uid")) {
            Ok(uuid) => Ok(Some(Uuid::parse_str(&uuid)?)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<V4IndexReader>> + '_> {
        Ok(self.index_uuid.iter().map(|index| -> Result<_> {
            V4IndexReader::new(
                index.uid.clone(),
                &self.dump.path().join("indexes").join(index.index_meta.uuid.to_string()),
            )
        }))
    }

    pub fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(Task, Option<Box<super::UpdateFile>>)>> + '_> {
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
                    Ok((
                        task,
                        Some(
                            Box::new(UpdateFile::new(&update_file_path)?) as Box<super::UpdateFile>
                        ),
                    ))
                } else {
                    Ok((task, None))
                }
            } else {
                Ok((task, None))
            }
        }))
    }

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<Key>> + '_> {
        Box::new(
            (&mut self.keys).lines().map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) }),
        )
    }
}

pub struct V4IndexReader {
    metadata: IndexMetadata,
    settings: Settings<Checked>,

    documents: BufReader<File>,
}

impl V4IndexReader {
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

        let ret = V4IndexReader {
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

#[cfg(test)]
pub(crate) mod test {
    use std::fs::File;
    use std::io::BufReader;

    use flate2::bufread::GzDecoder;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn read_dump_v4() {
        let dump = File::open("tests/assets/v4.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = V4Reader::open(dir).unwrap();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-06 12:53:49.131989609 +00:00:00");
        insta::assert_display_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, mut update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"f4efacbea0c1a4400873f4b2ee33f975");
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        let update_file = update_files.remove(0).unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(update_file), @"7b8889539b669c7b9ddba448bafa385d");

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys, { "[].uid" => "[uuid]" }), @"9240300dca8f962cdf58359ef4c76f09");

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

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"ace6546a6eb856ecb770b2409975c01d");
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"06aa1988493485d9b2cda7c751e6bb15");
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"786022a66ecb992c8a2a60fee070a5ab");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"7d722fc2629eaa45032ed3deb0c9b4ce");
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
