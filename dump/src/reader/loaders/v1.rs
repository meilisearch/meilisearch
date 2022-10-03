use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::index_controller::IndexMetadata;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    indexes: Vec<Index>,
    db_version: String,
    dump_version: crate::Version,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Index {
    pub name: String,
    pub uid: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    pub primary_key: Option<String>,
}

pub struct V1Reader {
    dump: TempDir,
    metadata: Metadata,
}

impl Reader {
    pub fn open(dump: &TempDir) -> Result<Self> {
        let mut meta_file = File::open(path.path().join("metadata.json"))?;
        let metadata = serde_json::from_reader(&mut meta_file)?;

        Ok(Reader { dump, metadata })
    }

    pub fn date(&self) -> Result<Option<OffsetDateTime>> {
        Ok(None)
    }
}
