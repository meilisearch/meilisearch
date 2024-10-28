use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tempfile::TempDir;
use time::OffsetDateTime;

use super::compat::v1_to_v2::CompatV1ToV2;
use super::Document;
use crate::{IndexMetadata, Result, Version};

pub mod settings;
pub mod update;

pub struct V1Reader {
    pub dump: TempDir,
    pub db_version: String,
    pub dump_version: crate::Version,
    indexes: Vec<V1Index>,
}

pub struct IndexUuid {
    pub name: String,
    pub uid: String,
}
pub type Task = self::update::UpdateStatus;

struct V1Index {
    metadata: IndexMetadataV1,
    path: PathBuf,
}

impl V1Index {
    pub fn new(path: PathBuf, metadata: Index) -> Self {
        Self { metadata: metadata.into(), path }
    }

    pub fn open(&self) -> Result<V1IndexReader> {
        V1IndexReader::new(&self.path, self.metadata.clone())
    }

    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata.metadata
    }
}

pub struct V1IndexReader {
    metadata: IndexMetadataV1,
    documents: BufReader<File>,
    settings: BufReader<File>,
    updates: BufReader<File>,
}

impl V1IndexReader {
    pub fn new(path: &Path, metadata: IndexMetadataV1) -> Result<Self> {
        Ok(V1IndexReader {
            metadata,
            documents: BufReader::new(File::open(path.join("documents.jsonl"))?),
            settings: BufReader::new(File::open(path.join("settings.json"))?),
            updates: BufReader::new(File::open(path.join("updates.jsonl"))?),
        })
    }

    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata.metadata
    }

    pub fn documents(&mut self) -> Result<impl Iterator<Item = Result<Document>> + '_> {
        Ok((&mut self.documents)
            .lines()
            .map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) }))
    }

    pub fn settings(&mut self) -> Result<self::settings::Settings> {
        Ok(serde_json::from_reader(&mut self.settings)?)
    }

    pub fn tasks(self) -> impl Iterator<Item = Result<Task>> {
        self.updates.lines().map(|line| -> Result<_> { Ok(serde_json::from_str(&line?)?) })
    }
}

impl V1Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let meta_file = fs::read(dump.path().join("metadata.json"))?;
        let metadata: Metadata = serde_json::from_reader(&*meta_file)?;

        let mut indexes = Vec::new();

        for index in metadata.indexes.into_iter() {
            let index_path = dump.path().join(&index.uid);
            indexes.push(V1Index::new(index_path, index));
        }

        Ok(V1Reader {
            dump,
            indexes,
            db_version: metadata.db_version,
            dump_version: metadata.dump_version,
        })
    }

    pub fn to_v2(self) -> CompatV1ToV2 {
        CompatV1ToV2 { from: self }
    }

    pub fn index_uuid(&self) -> Vec<IndexUuid> {
        self.indexes
            .iter()
            .map(|index| IndexUuid {
                name: index.metadata.name.to_owned(),
                uid: index.metadata().uid.to_owned(),
            })
            .collect()
    }

    pub fn version(&self) -> Version {
        Version::V1
    }

    pub fn date(&self) -> Option<OffsetDateTime> {
        None
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<V1IndexReader>> + '_> {
        Ok(self.indexes.iter().map(|index| index.open()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    pub name: String,
    pub uid: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    pub primary_key: Option<String>,
}

#[derive(Clone)]
pub struct IndexMetadataV1 {
    pub name: String,
    pub metadata: crate::IndexMetadata,
}

impl From<Index> for IndexMetadataV1 {
    fn from(index: Index) -> Self {
        IndexMetadataV1 {
            name: index.name,
            metadata: crate::IndexMetadata {
                uid: index.uid,
                primary_key: index.primary_key,
                created_at: index.created_at,
                updated_at: index.updated_at,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub indexes: Vec<Index>,
    pub db_version: String,
    pub dump_version: crate::Version,
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
    fn read_dump_v1() {
        let dump = File::open("tests/assets/v1.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let dump = V1Reader::open(dir).unwrap();

        // top level infos
        assert_eq!(dump.date(), None);

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();

        let mut products = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut dnd_spells = indexes.pop().unwrap();

        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "2022-10-02T13:23:39.976870431Z",
          "updatedAt": "2022-10-02T13:27:54.353262482Z"
        }
        "###);

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // products tasks
        let tasks = products.tasks().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"91de507f206ad21964584021932ba7a7");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "2022-10-02T13:15:29.477512777Z",
          "updatedAt": "2022-10-02T13:21:12.671204856Z"
        }
        "###);

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b63dbed5bbc059f3e32bc471ae699bf5");

        // movies tasks
        let tasks = movies.tasks().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"55eef4de2bef7e84c5ce0bee47488f56");

        // spells
        insta::assert_json_snapshot!(dnd_spells.metadata(), @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "2022-10-02T13:38:26.358882984Z",
          "updatedAt": "2022-10-02T13:38:26.385609433Z"
        }
        "###);

        insta::assert_json_snapshot!(dnd_spells.settings().unwrap());
        let documents = dnd_spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"aa24c0cfc733d66c396237ad44263bed");

        // spells tasks
        let tasks = dnd_spells.tasks().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"836dd7d64d5ad20ad901c44b1b161a4c");
    }
}
