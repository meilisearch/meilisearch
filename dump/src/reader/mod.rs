use std::io::Read;
use std::{fs::File, io::BufReader};

use flate2::bufread::GzDecoder;

use serde::Deserialize;

use tempfile::TempDir;
use time::OffsetDateTime;
use uuid::Uuid;

// use crate::reader::compat::Compat;
use crate::{IndexMetadata, Result, Version};

use self::compat::Compat;

// use self::loaders::{v2, v3, v4, v5};

// pub mod error;
mod compat;
// mod loaders;
// mod v1;
pub(self) mod v2;
pub(self) mod v3;
pub(self) mod v4;
pub(self) mod v5;
pub(self) mod v6;

pub type Document = serde_json::Map<String, serde_json::Value>;
pub type UpdateFile = dyn Iterator<Item = Result<Document>>;

pub fn open(dump: impl Read) -> Result<Compat> {
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
        Version::V3 => Ok(v3::V3Reader::open(path)?.to_v4().to_v5().to_v6().into()),
        Version::V4 => Ok(v4::V4Reader::open(path)?.to_v5().to_v6().into()),
        Version::V5 => Ok(v5::V5Reader::open(path)?.to_v6().into()),
        Version::V6 => Ok(v6::V6Reader::open(path)?.into()),
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
