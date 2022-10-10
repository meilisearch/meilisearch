use std::io::Read;
use std::{fs::File, io::BufReader};

use flate2::bufread::GzDecoder;

use serde::Deserialize;

use tempfile::TempDir;

use crate::{Result, Version};

use self::compat::Compat;

mod compat;

// pub(self) mod v1;
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
        Version::V2 => Ok(v2::V2Reader::open(path)?
            .to_v3()
            .to_v4()
            .to_v5()
            .to_v6()
            .into()),
        Version::V3 => Ok(v3::V3Reader::open(path)?.to_v4().to_v5().to_v6().into()),
        Version::V4 => Ok(v4::V4Reader::open(path)?.to_v5().to_v6().into()),
        Version::V5 => Ok(v5::V5Reader::open(path)?.to_v6().into()),
        Version::V6 => Ok(v6::V6Reader::open(path)?.into()),
    }
}
