use std::path::Path;

use log::info;

use crate::dump::Metadata;
use crate::options::IndexerOpts;

pub fn load_dump(
    meta: Metadata,
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    index_db_size: usize,
    meta_env_size: usize,
    indexing_options: &IndexerOpts,
) -> anyhow::Result<()> {
    info!("Patching dump V4 to dump V5...");

    super::v5::load_dump(
        meta,
        src,
        dst,
        index_db_size,
        meta_env_size,
        indexing_options,
    )
}
