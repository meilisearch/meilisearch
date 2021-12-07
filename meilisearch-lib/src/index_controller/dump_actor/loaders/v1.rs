use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::index_controller::IndexMetadata;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetadataV1 {
    pub db_version: String,
    indexes: Vec<IndexMetadata>,
}

impl MetadataV1 {
    #[allow(dead_code, unreachable_code, unused_variables)]
    pub fn load_dump(
        self,
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        size: usize,
        indexer_options: &IndexerOpts,
    ) -> anyhow::Result<()> {
        anyhow::bail!("The version 1 of the dumps is not supported anymore. You can re-export your dump from a version between 0.21 and 0.24, or start fresh from a version 0.25 onwards.")
}
