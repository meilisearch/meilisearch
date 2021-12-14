use std::path::Path;

use heed::EnvOpenOptions;
use log::info;
use meilisearch_auth::AuthController;

use crate::analytics;
use crate::index_controller::dump_actor::Metadata;
use crate::index_resolver::IndexResolver;
use crate::options::IndexerOpts;
use crate::tasks::TaskStore;
use crate::update_file_store::UpdateFileStore;

pub fn load_dump(
    meta: Metadata,
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    index_db_size: usize,
    meta_env_size: usize,
    indexing_options: &IndexerOpts,
) -> anyhow::Result<()> {
    info!(
        "Loading dump from {}, dump database version: {}, dump version: V4",
        meta.dump_date, meta.db_version
    );

    let mut options = EnvOpenOptions::new();
    options.map_size(meta_env_size);
    options.max_dbs(100);
    let env = options.open(&dst)?;

    IndexResolver::load_dump(
        src.as_ref(),
        &dst,
        index_db_size,
        env.clone(),
        indexing_options,
    )?;
    UpdateFileStore::load_dump(src.as_ref(), &dst)?;
    TaskStore::load_dump(&src, env)?;
    AuthController::load_dump(&src, &dst)?;
    analytics::copy_user_id(src.as_ref(), dst.as_ref());

    info!("Loading indexes.");

    Ok(())
}
