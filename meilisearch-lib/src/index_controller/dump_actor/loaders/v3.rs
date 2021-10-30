use std::path::Path;

use log::info;

use crate::analytics;
use crate::index_controller::dump_actor::Metadata;
use crate::index_controller::index_resolver::IndexResolver;
use crate::index_controller::update_file_store::UpdateFileStore;
use crate::index_controller::updates::store::UpdateStore;
use crate::options::IndexerOpts;

pub fn load_dump(
    meta: Metadata,
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    index_db_size: usize,
    update_db_size: usize,
    indexing_options: &IndexerOpts,
) -> anyhow::Result<()> {
    info!(
        "Loading dump from {}, dump database version: {}, dump version: V3",
        meta.dump_date, meta.db_version
    );

    IndexResolver::load_dump(src.as_ref(), &dst, index_db_size, indexing_options)?;
    UpdateFileStore::load_dump(src.as_ref(), &dst)?;
    UpdateStore::load_dump(&src, &dst, update_db_size)?;
    analytics::copy_user_id(src.as_ref(), dst.as_ref());

    info!("Loading indexes.");

    Ok(())
}
