#[cfg(not(test))]
pub use real::DumpHandler;

#[cfg(test)]
pub use test::MockDumpHandler as DumpHandler;

use time::{macros::format_description, OffsetDateTime};

/// Generate uid from creation date
pub fn generate_uid() -> String {
    OffsetDateTime::now_utc()
        .format(format_description!(
            "[year repr:full][month repr:numerical][day padding:zero]-[hour padding:zero][minute padding:zero][second padding:zero][subsecond digits:3]"
        ))
        .unwrap()
}

mod real {
    use std::{fs::File, path::PathBuf, sync::Arc};

    use log::{info, trace};
    use meilisearch_auth::AuthController;
    use milli::heed::Env;
    use tokio::fs::create_dir_all;

    use crate::analytics;
    use crate::compression::to_tar_gz;
    use crate::dump::error::{DumpError, Result};
    use crate::dump::{MetadataVersion, META_FILE_NAME};
    use crate::index_resolver::{
        index_store::IndexStore, meta_store::IndexMetaStore, IndexResolver,
    };
    use crate::tasks::TaskStore;
    use crate::update_file_store::UpdateFileStore;

    pub struct DumpHandler<U, I> {
        dump_path: PathBuf,
        db_path: PathBuf,
        update_file_store: UpdateFileStore,
        task_store_size: usize,
        index_db_size: usize,
        env: Arc<Env>,
        index_resolver: Arc<IndexResolver<U, I>>,
    }

    impl<U, I> DumpHandler<U, I>
    where
        U: IndexMetaStore + Sync + Send + 'static,
        I: IndexStore + Sync + Send + 'static,
    {
        pub fn new(
            dump_path: PathBuf,
            db_path: PathBuf,
            update_file_store: UpdateFileStore,
            task_store_size: usize,
            index_db_size: usize,
            env: Arc<Env>,
            index_resolver: Arc<IndexResolver<U, I>>,
        ) -> Self {
            Self {
                dump_path,
                db_path,
                update_file_store,
                task_store_size,
                index_db_size,
                env,
                index_resolver,
            }
        }

        pub async fn run(&self, uid: String) -> Result<()> {
            trace!("Performing dump.");

            create_dir_all(&self.dump_path).await?;

            let temp_dump_dir = tokio::task::spawn_blocking(tempfile::TempDir::new).await??;
            let temp_dump_path = temp_dump_dir.path().to_owned();

            let meta = MetadataVersion::new_v5(self.index_db_size, self.task_store_size);
            let meta_path = temp_dump_path.join(META_FILE_NAME);
            // TODO: blocking
            let mut meta_file = File::create(&meta_path)?;
            serde_json::to_writer(&mut meta_file, &meta)?;
            analytics::copy_user_id(&self.db_path, &temp_dump_path);

            create_dir_all(&temp_dump_path.join("indexes")).await?;

            // TODO: this is blocking!!
            AuthController::dump(&self.db_path, &temp_dump_path)?;
            TaskStore::dump(
                self.env.clone(),
                &temp_dump_path,
                self.update_file_store.clone(),
            )
            .await?;
            self.index_resolver.dump(&temp_dump_path).await?;

            let dump_path = self.dump_path.clone();
            let dump_path = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
                // for now we simply copy the updates/updates_files
                // FIXME: We may copy more files than necessary, if new files are added while we are
                // performing the dump. We need a way to filter them out.

                let temp_dump_file = tempfile::NamedTempFile::new_in(&dump_path)?;
                to_tar_gz(temp_dump_path, temp_dump_file.path())
                    .map_err(|e| DumpError::Internal(e.into()))?;

                let dump_path = dump_path.join(uid).with_extension("dump");
                temp_dump_file.persist(&dump_path)?;

                Ok(dump_path)
            })
            .await??;

            info!("Created dump in {:?}.", dump_path);

            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use milli::heed::Env;
    use nelson::Mocker;

    use crate::dump::error::Result;
    use crate::index_resolver::IndexResolver;
    use crate::index_resolver::{index_store::IndexStore, meta_store::IndexMetaStore};
    use crate::update_file_store::UpdateFileStore;

    use super::*;

    pub enum MockDumpHandler<U, I> {
        Real(super::real::DumpHandler<U, I>),
        Mock(Mocker),
    }

    impl<U, I> MockDumpHandler<U, I> {
        pub fn mock(mocker: Mocker) -> Self {
            Self::Mock(mocker)
        }
    }

    impl<U, I> MockDumpHandler<U, I>
    where
        U: IndexMetaStore + Sync + Send + 'static,
        I: IndexStore + Sync + Send + 'static,
    {
        pub fn new(
            dump_path: PathBuf,
            db_path: PathBuf,
            update_file_store: UpdateFileStore,
            task_store_size: usize,
            index_db_size: usize,
            env: Arc<Env>,
            index_resolver: Arc<IndexResolver<U, I>>,
        ) -> Self {
            Self::Real(super::real::DumpHandler::new(
                dump_path,
                db_path,
                update_file_store,
                task_store_size,
                index_db_size,
                env,
                index_resolver,
            ))
        }
        pub async fn run(&self, uid: String) -> Result<()> {
            match self {
                DumpHandler::Real(real) => real.run(uid).await,
                DumpHandler::Mock(mocker) => unsafe { mocker.get("run").call(uid) },
            }
        }
    }
}
