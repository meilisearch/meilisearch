use std::path::Path;

use chrono::{DateTime, Utc};
use log::info;
use serde::{Deserialize, Serialize};

use crate::index_controller::index_resolver::IndexResolver;
use crate::index_controller::update_file_store::UpdateFileStore;
use crate::index_controller::updates::store::UpdateStore;
use crate::options::IndexerOpts;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetadataV2 {
    db_version: String,
    index_db_size: usize,
    update_db_size: usize,
    dump_date: DateTime<Utc>,
}

impl MetadataV2 {
    pub fn new(index_db_size: usize, update_db_size: usize) -> Self {
        Self {
            db_version: env!("CARGO_PKG_VERSION").to_string(),
            index_db_size,
            update_db_size,
            dump_date: Utc::now(),
        }
    }

    pub fn load_dump(
        self,
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        index_db_size: usize,
        update_db_size: usize,
        indexing_options: &IndexerOpts,
    ) -> anyhow::Result<()> {
        info!(
            "Loading dump from {}, dump database version: {}, dump version: V2",
            self.dump_date, self.db_version
        );

        IndexResolver::load_dump(src.as_ref(), &dst, index_db_size, indexing_options)?;
        UpdateFileStore::load_dump(src.as_ref(), &dst)?;
        UpdateStore::load_dump(&src, &dst, update_db_size)?;

        info!("Loading indexes.");

        Ok(())
    }
}
