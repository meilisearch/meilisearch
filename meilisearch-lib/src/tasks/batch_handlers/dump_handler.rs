use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::{error, trace};
use time::{macros::format_description, OffsetDateTime};

use crate::dump::DumpJob;
use crate::index_resolver::index_store::IndexStore;
use crate::index_resolver::meta_store::IndexMetaStore;
use crate::index_resolver::IndexResolver;
use crate::tasks::batch::{Batch, BatchContent};
use crate::tasks::BatchHandler;
use crate::update_file_store::UpdateFileStore;

pub struct DumpHandler<U, I> {
    update_file_store: UpdateFileStore,
    index_resolver: Arc<IndexResolver<U, I>>,
    dump_path: PathBuf,
    db_path: PathBuf,
    update_db_size: usize,
    index_db_size: usize,
}

/// Generate uid from creation date
fn generate_uid() -> String {
    OffsetDateTime::now_utc()
        .format(format_description!(
            "[year repr:full][month repr:numerical][day padding:zero]-[hour padding:zero][minute padding:zero][second padding:zero][subsecond digits:3]"
        ))
        .unwrap()
}

impl<U, I> DumpHandler<U, I>
where
    U: IndexMetaStore + Send + Sync + 'static,
    I: IndexStore + Send + Sync + 'static,
{
    pub fn new(
        update_file_store: UpdateFileStore,
        dump_path: impl AsRef<Path>,
        db_path: impl AsRef<Path>,
        index_db_size: usize,
        update_db_size: usize,
        index_resolver: Arc<IndexResolver<U, I>>,
    ) -> Self {
        Self {
            update_file_store,
            dump_path: dump_path.as_ref().into(),
            db_path: db_path.as_ref().into(),
            index_db_size,
            update_db_size,
            index_resolver,
        }
    }

    async fn create_dump(&self) {
        let uid = generate_uid();

        let task = DumpJob {
            dump_path: self.dump_path.clone(),
            db_path: self.db_path.clone(),
            update_file_store: self.update_file_store.clone(),
            uid: uid.clone(),
            update_db_size: self.update_db_size,
            index_db_size: self.index_db_size,
            index_resolver: self.index_resolver.clone(),
        };

        let task_result = tokio::task::spawn_local(task.run()).await;

        match task_result {
            Ok(Ok(())) => {
                trace!("Dump succeed");
            }
            Ok(Err(e)) => {
                error!("Dump failed: {}", e);
            }
            Err(_) => {
                error!("Dump panicked. Dump status set to failed");
            }
        };
    }
}

#[async_trait::async_trait]
impl<U, I> BatchHandler for DumpHandler<U, I>
where
    U: IndexMetaStore + Send + Sync + 'static,
    I: IndexStore + Send + Sync + 'static,
{
    fn accept(&self, batch: &Batch) -> bool {
        matches!(batch.content, BatchContent::Dump { .. })
    }

    async fn process_batch(&self, batch: Batch) -> Batch {
        match batch.content {
            BatchContent::Dump { .. } => {
                self.create_dump().await;
                batch
            }
            _ => unreachable!("invalid batch content for dump"),
        }
    }

    async fn finish(&self, _: &Batch) {
        ()
    }
}
