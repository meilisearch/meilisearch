use std::{
    convert::Infallible,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
};

use tempfile::TempDir;
use time::OffsetDateTime;

use self::update::UpdateStatus;

use super::{DumpReader, IndexReader};
use crate::{Error, Result, Version};

pub mod settings;
pub mod update;
pub mod v1;

pub struct V1Reader {
    dump: TempDir,
    metadata: v1::Metadata,
    indexes: Vec<V1IndexReader>,
}

struct V1IndexReader {
    name: String,
    documents: BufReader<File>,
    settings: BufReader<File>,
    updates: BufReader<File>,

    current_update: Option<UpdateStatus>,
}

impl V1IndexReader {
    pub fn new(name: String, path: &Path) -> Result<Self> {
        let mut ret = V1IndexReader {
            name,
            documents: BufReader::new(File::open(path.join("documents.jsonl"))?),
            settings: BufReader::new(File::open(path.join("settings.json"))?),
            updates: BufReader::new(File::open(path.join("updates.jsonl"))?),
            current_update: None,
        };
        ret.next_update();

        Ok(ret)
    }

    pub fn next_update(&mut self) -> Result<Option<UpdateStatus>> {
        let current_update = if let Some(line) = self.updates.lines().next() {
            Some(serde_json::from_str(&line?)?)
        } else {
            None
        };

        Ok(std::mem::replace(&mut self.current_update, current_update))
    }
}

impl V1Reader {
    pub fn open(dump: TempDir) -> Result<Self> {
        let mut meta_file = fs::read(dump.path().join("metadata.json"))?;
        let metadata = serde_json::from_reader(&*meta_file)?;

        let mut indexes = Vec::new();

        let entries = fs::read_dir(dump.path())?;
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                indexes.push(V1IndexReader::new(
                    entry
                        .file_name()
                        .to_str()
                        .ok_or(Error::BadIndexName)?
                        .to_string(),
                    &entry.path(),
                )?);
            }
        }

        Ok(V1Reader {
            dump,
            metadata,
            indexes,
        })
    }

    fn next_update(&mut self) -> Result<Option<UpdateStatus>> {
        if let Some((idx, _)) = self
            .indexes
            .iter()
            .map(|index| index.current_update)
            .enumerate()
            .filter_map(|(idx, update)| update.map(|u| (idx, u)))
            .min_by_key(|(_, update)| update.enqueued_at())
        {
            self.indexes[idx].next_update()
        } else {
            Ok(None)
        }
    }
}

impl IndexReader for &V1IndexReader {
    type Document = serde_json::Map<String, serde_json::Value>;
    type Settings = settings::Settings;

    fn name(&self) -> &str {
        todo!()
    }

    fn documents(&self) -> Result<Box<dyn Iterator<Item = Result<Self::Document>>>> {
        todo!()
    }

    fn settings(&self) -> Result<Self::Settings> {
        todo!()
    }
}

impl DumpReader for V1Reader {
    type Document = serde_json::Map<String, serde_json::Value>;
    type Settings = settings::Settings;

    type Task = update::UpdateStatus;
    type UpdateFile = Infallible;

    type Key = Infallible;

    fn date(&self) -> Option<OffsetDateTime> {
        None
    }

    fn version(&self) -> Version {
        Version::V1
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
        Ok(Box::new(self.indexes.iter().map(|index| {
            let index = Box::new(index)
                as Box<dyn IndexReader<Document = Self::Document, Settings = Self::Settings>>;
            Ok(index)
        })))
    }

    fn tasks(&self) -> Box<dyn Iterator<Item = Result<(Self::Task, Option<Self::UpdateFile>)>>> {
        Box::new(std::iter::from_fn(|| {
            self.next_update()
                .transpose()
                .map(|result| result.map(|task| (task, None)))
        }))
    }

    fn keys(&self) -> Box<dyn Iterator<Item = Result<Self::Key>>> {
        Box::new(std::iter::empty())
    }
}
