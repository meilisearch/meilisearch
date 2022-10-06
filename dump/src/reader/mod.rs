use std::io::Read;
use std::{fs::File, io::BufReader};

use flate2::bufread::GzDecoder;
use index_scheduler::TaskView;
use meilisearch_auth::Key;
use serde::Deserialize;

use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

// use crate::reader::compat::Compat;
use crate::{IndexMetadata, Result, Version};

// use self::loaders::{v2, v3, v4, v5};

// pub mod error;
// mod compat;
// mod loaders;
// mod v1;
pub(self) mod v4;
pub(self) mod v5;
pub(self) mod v6;

pub fn open(dump: impl Read) -> Result<Box<dyn DumpReader>> {
    let path = TempDir::new()?;
    let mut dump = BufReader::new(dump);
    let gz = GzDecoder::new(&mut dump);
    let mut archive = tar::Archive::new(gz);
    archive.unpack(path.path())?;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
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
        Version::V5 => {
            /*
            let dump_reader = Box::new(v5::V5Reader::open(path)?);
            let dump_reader = Box::new(Compat::<
                dyn DumpReader<
                    Document = v5::Document,
                    Settings = v5::Settings<v5::Checked>,
                    Task = v5::Task,
                    UpdateFile = v5::UpdateFile,
                    Key = v5::Key,
                >,
            >::new(dump_reader))
                as Box<
                    dyn DumpReader<
                        Document = v6::Document,
                        Settings = v6::Settings<v6::Checked>,
                        Task = v6::Task,
                        UpdateFile = v6::UpdateFile,
                        Key = v6::Key,
                    >,
                >;
            Ok(dump_reader)
            */
            todo!()
        }
        Version::V6 => Ok(Box::new(v6::V6Reader::open(path)?)),
    }
}

pub trait DumpReader {
    /// Return the version of the dump.
    fn version(&self) -> Version;

    /// Return at which date the dump was created if there was one.
    fn date(&self) -> Option<OffsetDateTime>;

    /// Return the instance-uid if there was one.
    fn instance_uid(&self) -> Result<Option<Uuid>>;

    /// Return an iterator over each indexes.
    fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<Box<dyn IndexReader + '_>>> + '_>>;

    /// Return all the tasks in the dump with a possible update file.
    fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(v6::Task, Option<v6::UpdateFile>)>> + '_>;

    /// Return all the keys.
    fn keys(&mut self) -> Box<dyn Iterator<Item = Result<v6::Key>> + '_>;
}

pub trait IndexReader {
    fn metadata(&self) -> &IndexMetadata;
    fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<v6::Document>> + '_>>;
    fn settings(&mut self) -> Result<v6::Settings<v6::Checked>>;
}
