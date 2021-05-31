use std::path::Path;

use chrono::{DateTime, Utc};
use log::info;
use serde::{Deserialize, Serialize};

use crate::index::Index;
use crate::index_controller::{update_actor::UpdateStore, uuid_resolver::HeedUuidStore};
use crate::option::IndexerOpts;

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

        info!("Loading index database.");
        HeedUuidStore::load_dump(src.as_ref(), &dst)?;

        info!("Loading updates.");
        UpdateStore::load_dump(&src, &dst, update_db_size)?;

        info!("Loading indexes.");
        let indexes_path = src.as_ref().join("indexes");
        let indexes = indexes_path.read_dir()?;
        for index in indexes {
            let index = index?;
            Index::load_dump(&index.path(), &dst, index_db_size, indexing_options)?;
        }

        Ok(())
    }
}
