use std::path::Path;
use std::{fs::File, io::BufReader};

use flate2::{bufread::GzDecoder, Compression};
use index::{Settings, Unchecked};
use index_scheduler::TaskView;
use meilisearch_auth::Key;
use serde::{Deserialize, Serialize};

use tempfile::TempDir;
use time::OffsetDateTime;

use crate::{Result, Version};

// use self::loaders::{v2, v3, v4, v5};

// pub mod error;
// mod compat;
// mod loaders;
mod v1;
// mod v6;

pub fn open(
    dump_path: &Path,
) -> Result<
    impl DumpReader<
        Document = serde_json::Value,
        Settings = Settings<Unchecked>,
        Task = TaskView,
        UpdateFile = (),
        Key = Key,
    >,
> {
    let path = TempDir::new()?;

    let dump = File::open(dump_path)?;
    let mut dump = BufReader::new(dump);

    let gz = GzDecoder::new(&mut dump);
    let mut archive = tar::Archive::new(gz);
    archive.unpack(path.path())?;

    #[derive(Deserialize)]
    struct MetadataVersion {
        pub dump_version: Version,
    }
    let mut meta_file = File::open(path.path().join("metadata.json"))?;
    let MetadataVersion { dump_version } = serde_json::from_reader(&mut meta_file)?;

    match dump_version {
        // Version::V1 => Ok(Box::new(v1::Reader::open(path)?)),
        Version::V1 => todo!(),
        Version::V2 => todo!(),
        Version::V3 => todo!(),
        Version::V4 => todo!(),
        Version::V5 => todo!(),
        Version::V6 => todo!(),
    };

    todo!()
}

pub trait DumpReader {
    type Document;
    type Settings;

    type Task;
    type UpdateFile;

    type Key;

    /// Return the version of the dump.
    fn version(&self) -> Version;

    /// Return at which date the index was created.
    fn date(&self) -> Result<Option<OffsetDateTime>>;

    /// Return an iterator over each indexes.
    fn indexes(
        &self,
    ) -> Result<
        Box<
            dyn Iterator<
                Item = Box<dyn IndexReader<Document = Self::Document, Settings = Self::Settings>>,
            >,
        >,
    >;

    /// Return all the tasks in the dump with a possible update file.
    fn tasks(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<(Self::Task, Option<Self::UpdateFile>)>>>>;

    /// Return all the keys.
    fn keys(&self) -> Result<Box<dyn Iterator<Item = Self::Key>>>;
}

pub trait IndexReader {
    type Document;
    type Settings;

    fn name(&self) -> &str;
    fn documents(&self) -> Result<Box<dyn Iterator<Item = Self::Document>>>;
    fn settings(&self) -> Result<Self::Settings>;
}
