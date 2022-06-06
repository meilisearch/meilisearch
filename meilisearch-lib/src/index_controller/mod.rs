use meilisearch_auth::SearchRules;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use actix_web::error::PayloadError;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use uuid::Uuid;

use crate::document_formats::{read_csv, read_json, read_ndjson};
use crate::dump::{self, load_dump, DumpHandler};
use crate::index::{
    Checked, Document, IndexMeta, IndexStats, SearchQuery, SearchResult, Settings, Unchecked,
};
use crate::options::{IndexerOpts, SchedulerConfig};
use crate::snapshot::{load_snapshot, SnapshotService};
use crate::tasks::error::TaskError;
use crate::tasks::task::{DocumentDeletion, Task, TaskContent, TaskId};
use crate::tasks::{
    BatchHandler, EmptyBatchHandler, Scheduler, SnapshotHandler, TaskFilter, TaskStore,
};
use error::Result;

use self::error::IndexControllerError;
use crate::index_resolver::index_store::{IndexStore, MapIndexStore};
use crate::index_resolver::meta_store::{HeedMetaStore, IndexMetaStore};
pub use crate::index_resolver::IndexUid;
use crate::index_resolver::{create_index_resolver, IndexResolver};
use crate::update_file_store::UpdateFileStore;

pub mod error;
pub mod versioning;

/// Concrete implementation of the IndexController, exposed by meilisearch-lib
pub type MeiliSearch = IndexController<HeedMetaStore, MapIndexStore>;

pub type Payload = Box<
    dyn Stream<Item = std::result::Result<Bytes, PayloadError>> + Send + Sync + 'static + Unpin,
>;

pub fn open_meta_env(path: &Path, size: usize) -> milli::heed::Result<milli::heed::Env> {
    let mut options = milli::heed::EnvOpenOptions::new();
    options.map_size(size);
    options.max_dbs(20);
    options.open(path)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    #[serde(skip)]
    pub uuid: Uuid,
    pub uid: String,
    #[serde(flatten)]
    pub meta: IndexMeta,
}

#[derive(Clone, Debug)]
pub struct IndexSettings {
    pub uid: Option<String>,
    pub primary_key: Option<String>,
}

pub struct IndexController<U, I> {
    pub index_resolver: Arc<IndexResolver<U, I>>,
    scheduler: Arc<RwLock<Scheduler>>,
    task_store: TaskStore,
    pub update_file_store: UpdateFileStore,
}

/// Need a custom implementation for clone because deriving require that U and I are clone.
impl<U, I> Clone for IndexController<U, I> {
    fn clone(&self) -> Self {
        Self {
            index_resolver: self.index_resolver.clone(),
            scheduler: self.scheduler.clone(),
            update_file_store: self.update_file_store.clone(),
            task_store: self.task_store.clone(),
        }
    }
}

#[derive(Debug)]
pub enum DocumentAdditionFormat {
    Json,
    Csv,
    Ndjson,
}

impl fmt::Display for DocumentAdditionFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocumentAdditionFormat::Json => write!(f, "json"),
            DocumentAdditionFormat::Ndjson => write!(f, "ndjson"),
            DocumentAdditionFormat::Csv => write!(f, "csv"),
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub database_size: u64,
    #[serde(serialize_with = "time::serde::rfc3339::option::serialize")]
    pub last_update: Option<OffsetDateTime>,
    pub indexes: BTreeMap<String, IndexStats>,
}

#[allow(clippy::large_enum_variant)]
#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub enum Update {
    DeleteDocuments(Vec<String>),
    ClearDocuments,
    Settings {
        settings: Settings<Unchecked>,
        /// Indicates whether the update was a deletion
        is_deletion: bool,
        allow_index_creation: bool,
    },
    DocumentAddition {
        #[derivative(Debug = "ignore")]
        payload: Payload,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        format: DocumentAdditionFormat,
        allow_index_creation: bool,
    },
    DeleteIndex,
    CreateIndex {
        primary_key: Option<String>,
    },
    UpdateIndex {
        primary_key: Option<String>,
    },
}

#[derive(Default, Debug)]
pub struct IndexControllerBuilder {
    max_index_size: Option<usize>,
    max_task_store_size: Option<usize>,
    snapshot_dir: Option<PathBuf>,
    import_snapshot: Option<PathBuf>,
    snapshot_interval: Option<Duration>,
    ignore_snapshot_if_db_exists: bool,
    ignore_missing_snapshot: bool,
    schedule_snapshot: bool,
    dump_src: Option<PathBuf>,
    dump_dst: Option<PathBuf>,
    ignore_dump_if_db_exists: bool,
    ignore_missing_dump: bool,
}

impl IndexControllerBuilder {
    pub fn build(
        self,
        db_path: impl AsRef<Path>,
        indexer_options: IndexerOpts,
        scheduler_config: SchedulerConfig,
    ) -> anyhow::Result<MeiliSearch> {
        let index_size = self
            .max_index_size
            .ok_or_else(|| anyhow::anyhow!("Missing index size"))?;
        let task_store_size = self
            .max_task_store_size
            .ok_or_else(|| anyhow::anyhow!("Missing update database size"))?;

        if let Some(ref path) = self.import_snapshot {
            log::info!("Loading from snapshot {:?}", path);
            load_snapshot(
                db_path.as_ref(),
                path,
                self.ignore_snapshot_if_db_exists,
                self.ignore_missing_snapshot,
            )?;
        } else if let Some(ref src_path) = self.dump_src {
            load_dump(
                db_path.as_ref(),
                src_path,
                self.ignore_dump_if_db_exists,
                self.ignore_missing_dump,
                index_size,
                task_store_size,
                &indexer_options,
            )?;
        } else if db_path.as_ref().exists() {
            // Directory could be pre-created without any database in.
            let db_is_empty = db_path.as_ref().read_dir()?.next().is_none();
            if !db_is_empty {
                versioning::check_version_file(db_path.as_ref())?;
            }
        }

        std::fs::create_dir_all(db_path.as_ref())?;

        let meta_env = Arc::new(open_meta_env(db_path.as_ref(), task_store_size)?);

        let update_file_store = UpdateFileStore::new(&db_path)?;
        // Create or overwrite the version file for this DB
        versioning::create_version_file(db_path.as_ref())?;

        let index_resolver = Arc::new(create_index_resolver(
            &db_path,
            index_size,
            &indexer_options,
            meta_env.clone(),
            update_file_store.clone(),
        )?);

        let dump_path = self
            .dump_dst
            .ok_or_else(|| anyhow::anyhow!("Missing dump directory path"))?;

        let dump_handler = Arc::new(DumpHandler::new(
            dump_path,
            db_path.as_ref().into(),
            update_file_store.clone(),
            task_store_size,
            index_size,
            meta_env.clone(),
            index_resolver.clone(),
        ));
        let task_store = TaskStore::new(meta_env)?;

        // register all the batch handlers for use with the scheduler.
        let handlers: Vec<Arc<dyn BatchHandler + Sync + Send + 'static>> = vec![
            index_resolver.clone(),
            dump_handler,
            Arc::new(SnapshotHandler),
            // dummy handler to catch all empty batches
            Arc::new(EmptyBatchHandler),
        ];
        let scheduler = Scheduler::new(task_store.clone(), handlers, scheduler_config)?;

        if self.schedule_snapshot {
            let snapshot_period = self
                .snapshot_interval
                .ok_or_else(|| anyhow::anyhow!("Snapshot interval not provided."))?;
            let snapshot_path = self
                .snapshot_dir
                .ok_or_else(|| anyhow::anyhow!("Snapshot path not provided."))?;

            let snapshot_service = SnapshotService {
                db_path: db_path.as_ref().to_path_buf(),
                snapshot_period,
                snapshot_path,
                index_size,
                meta_env_size: task_store_size,
                scheduler: scheduler.clone(),
            };

            tokio::task::spawn_local(snapshot_service.run());
        }

        Ok(IndexController {
            index_resolver,
            scheduler,
            update_file_store,
            task_store,
        })
    }

    /// Set the index controller builder's max update store size.
    pub fn set_max_task_store_size(&mut self, max_update_store_size: usize) -> &mut Self {
        self.max_task_store_size.replace(max_update_store_size);
        self
    }

    pub fn set_max_index_size(&mut self, size: usize) -> &mut Self {
        self.max_index_size.replace(size);
        self
    }

    /// Set the index controller builder's snapshot path.
    pub fn set_snapshot_dir(&mut self, snapshot_dir: PathBuf) -> &mut Self {
        self.snapshot_dir.replace(snapshot_dir);
        self
    }

    /// Set the index controller builder's ignore snapshot if db exists.
    pub fn set_ignore_snapshot_if_db_exists(
        &mut self,
        ignore_snapshot_if_db_exists: bool,
    ) -> &mut Self {
        self.ignore_snapshot_if_db_exists = ignore_snapshot_if_db_exists;
        self
    }

    /// Set the index controller builder's ignore missing snapshot.
    pub fn set_ignore_missing_snapshot(&mut self, ignore_missing_snapshot: bool) -> &mut Self {
        self.ignore_missing_snapshot = ignore_missing_snapshot;
        self
    }

    /// Set the index controller builder's import snapshot.
    pub fn set_import_snapshot(&mut self, import_snapshot: PathBuf) -> &mut Self {
        self.import_snapshot.replace(import_snapshot);
        self
    }

    /// Set the index controller builder's snapshot interval sec.
    pub fn set_snapshot_interval(&mut self, snapshot_interval: Duration) -> &mut Self {
        self.snapshot_interval = Some(snapshot_interval);
        self
    }

    /// Set the index controller builder's schedule snapshot.
    pub fn set_schedule_snapshot(&mut self) -> &mut Self {
        self.schedule_snapshot = true;
        self
    }

    /// Set the index controller builder's dump src.
    pub fn set_dump_src(&mut self, dump_src: PathBuf) -> &mut Self {
        self.dump_src.replace(dump_src);
        self
    }

    /// Set the index controller builder's dump dst.
    pub fn set_dump_dst(&mut self, dump_dst: PathBuf) -> &mut Self {
        self.dump_dst.replace(dump_dst);
        self
    }

    /// Set the index controller builder's ignore dump if db exists.
    pub fn set_ignore_dump_if_db_exists(&mut self, ignore_dump_if_db_exists: bool) -> &mut Self {
        self.ignore_dump_if_db_exists = ignore_dump_if_db_exists;
        self
    }

    /// Set the index controller builder's ignore missing dump.
    pub fn set_ignore_missing_dump(&mut self, ignore_missing_dump: bool) -> &mut Self {
        self.ignore_missing_dump = ignore_missing_dump;
        self
    }
}

impl<U, I> IndexController<U, I>
where
    U: IndexMetaStore,
    I: IndexStore,
{
    pub fn builder() -> IndexControllerBuilder {
        IndexControllerBuilder::default()
    }

    pub async fn register_update(&self, uid: String, update: Update) -> Result<Task> {
        let index_uid = IndexUid::new(uid)?;
        let content = match update {
            Update::DeleteDocuments(ids) => TaskContent::DocumentDeletion {
                index_uid,
                deletion: DocumentDeletion::Ids(ids),
            },
            Update::ClearDocuments => TaskContent::DocumentDeletion {
                index_uid,
                deletion: DocumentDeletion::Clear,
            },
            Update::Settings {
                settings,
                is_deletion,
                allow_index_creation,
            } => TaskContent::SettingsUpdate {
                settings,
                is_deletion,
                allow_index_creation,
                index_uid,
            },
            Update::DocumentAddition {
                mut payload,
                primary_key,
                format,
                method,
                allow_index_creation,
            } => {
                let mut buffer = Vec::new();
                while let Some(bytes) = payload.next().await {
                    let bytes = bytes?;
                    buffer.extend_from_slice(&bytes);
                }
                let (content_uuid, mut update_file) = self.update_file_store.new_update()?;
                let documents_count = tokio::task::spawn_blocking(move || -> Result<_> {
                    // check if the payload is empty, and return an error
                    if buffer.is_empty() {
                        return Err(IndexControllerError::MissingPayload(format));
                    }

                    let reader = Cursor::new(buffer);
                    let count = match format {
                        DocumentAdditionFormat::Json => read_json(reader, &mut *update_file)?,
                        DocumentAdditionFormat::Csv => read_csv(reader, &mut *update_file)?,
                        DocumentAdditionFormat::Ndjson => read_ndjson(reader, &mut *update_file)?,
                    };

                    update_file.persist()?;

                    Ok(count)
                })
                .await??;

                TaskContent::DocumentAddition {
                    content_uuid,
                    merge_strategy: method,
                    primary_key,
                    documents_count,
                    allow_index_creation,
                    index_uid,
                }
            }
            Update::DeleteIndex => TaskContent::IndexDeletion { index_uid },
            Update::CreateIndex { primary_key } => TaskContent::IndexCreation {
                primary_key,
                index_uid,
            },
            Update::UpdateIndex { primary_key } => TaskContent::IndexUpdate {
                primary_key,
                index_uid,
            },
        };

        let task = self.task_store.register(content).await?;
        self.scheduler.read().await.notify();

        Ok(task)
    }

    pub async fn register_dump_task(&self) -> Result<Task> {
        let uid = dump::generate_uid();
        let content = TaskContent::Dump { uid };
        let task = self.task_store.register(content).await?;
        self.scheduler.read().await.notify();
        Ok(task)
    }

    pub async fn get_task(&self, id: TaskId, filter: Option<TaskFilter>) -> Result<Task> {
        let task = self.scheduler.read().await.get_task(id, filter).await?;
        Ok(task)
    }

    pub async fn get_index_task(&self, index_uid: String, task_id: TaskId) -> Result<Task> {
        let creation_task_id = self
            .index_resolver
            .get_index_creation_task_id(index_uid.clone())
            .await?;
        if task_id < creation_task_id {
            return Err(TaskError::UnexistingTask(task_id).into());
        }

        let mut filter = TaskFilter::default();
        filter.filter_index(index_uid);
        let task = self
            .scheduler
            .read()
            .await
            .get_task(task_id, Some(filter))
            .await?;

        Ok(task)
    }

    pub async fn list_tasks(
        &self,
        filter: Option<TaskFilter>,
        limit: Option<usize>,
        offset: Option<TaskId>,
    ) -> Result<Vec<Task>> {
        let tasks = self
            .scheduler
            .read()
            .await
            .list_tasks(offset, filter, limit)
            .await?;

        Ok(tasks)
    }

    pub async fn list_index_task(
        &self,
        index_uid: String,
        limit: Option<usize>,
        offset: Option<TaskId>,
    ) -> Result<Vec<Task>> {
        let task_id = self
            .index_resolver
            .get_index_creation_task_id(index_uid.clone())
            .await?;

        let mut filter = TaskFilter::default();
        filter.filter_index(index_uid);

        let tasks = self
            .scheduler
            .read()
            .await
            .list_tasks(
                Some(offset.unwrap_or_default() + task_id),
                Some(filter),
                limit,
            )
            .await?;

        Ok(tasks)
    }

    pub async fn list_indexes(&self) -> Result<Vec<IndexMetadata>> {
        let indexes = self.index_resolver.list().await?;
        let mut ret = Vec::new();
        for (uid, index) in indexes {
            let meta = index.meta()?;
            let meta = IndexMetadata {
                uuid: index.uuid(),
                uid,
                meta,
            };
            ret.push(meta);
        }

        Ok(ret)
    }

    pub async fn settings(&self, uid: String) -> Result<Settings<Checked>> {
        let index = self.index_resolver.get_index(uid).await?;
        let settings = spawn_blocking(move || index.settings()).await??;
        Ok(settings)
    }

    /// Return the total number of documents contained in the index + the selected documents.
    pub async fn documents(
        &self,
        uid: String,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<(u64, Vec<Document>)> {
        let index = self.index_resolver.get_index(uid).await?;
        let result =
            spawn_blocking(move || index.retrieve_documents(offset, limit, attributes_to_retrieve))
                .await??;
        Ok(result)
    }

    pub async fn document(
        &self,
        uid: String,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        let index = self.index_resolver.get_index(uid).await?;
        let document =
            spawn_blocking(move || index.retrieve_document(doc_id, attributes_to_retrieve))
                .await??;
        Ok(document)
    }

    pub async fn search(&self, uid: String, query: SearchQuery) -> Result<SearchResult> {
        let index = self.index_resolver.get_index(uid).await?;
        let result = spawn_blocking(move || index.perform_search(query)).await??;
        Ok(result)
    }

    pub async fn get_index(&self, uid: String) -> Result<IndexMetadata> {
        let index = self.index_resolver.get_index(uid.clone()).await?;
        let uuid = index.uuid();
        let meta = spawn_blocking(move || index.meta()).await??;
        let meta = IndexMetadata { uuid, uid, meta };
        Ok(meta)
    }

    pub async fn get_index_stats(&self, uid: String) -> Result<IndexStats> {
        let processing_tasks = self.scheduler.read().await.get_processing_tasks().await?;
        // Check if the currently indexing update is from our index.
        let is_indexing = processing_tasks
            .first()
            .map_or(false, |task| task.index_uid().map_or(false, |u| u == uid));

        let index = self.index_resolver.get_index(uid).await?;
        let mut stats = spawn_blocking(move || index.stats()).await??;
        stats.is_indexing = Some(is_indexing);

        Ok(stats)
    }

    pub async fn get_all_stats(&self, search_rules: &SearchRules) -> Result<Stats> {
        let mut last_task: Option<OffsetDateTime> = None;
        let mut indexes = BTreeMap::new();
        let mut database_size = 0;
        let processing_tasks = self.scheduler.read().await.get_processing_tasks().await?;

        for (index_uid, index) in self.index_resolver.list().await? {
            if !search_rules.is_index_authorized(&index_uid) {
                continue;
            }

            let (mut stats, meta) =
                spawn_blocking::<_, Result<(IndexStats, IndexMeta)>>(move || {
                    Ok((index.stats()?, index.meta()?))
                })
                .await??;

            database_size += stats.size;

            last_task = last_task.map_or(Some(meta.updated_at), |last| {
                Some(last.max(meta.updated_at))
            });

            // Check if the currently indexing update is from our index.
            stats.is_indexing = processing_tasks
                .first()
                .and_then(|p| p.index_uid().map(|u| u == index_uid))
                .or(Some(false));

            indexes.insert(index_uid, stats);
        }

        Ok(Stats {
            database_size,
            last_update: last_task,
            indexes,
        })
    }
}

pub async fn get_arc_ownership_blocking<T>(mut item: Arc<T>) -> T {
    loop {
        match Arc::try_unwrap(item) {
            Ok(item) => return item,
            Err(item_arc) => {
                item = item_arc;
                sleep(Duration::from_millis(100)).await;
                continue;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use futures::future::ok;
    use mockall::predicate::eq;
    use nelson::Mocker;

    use crate::index::error::Result as IndexResult;
    use crate::index::Index;
    use crate::index::{
        DEFAULT_CROP_MARKER, DEFAULT_HIGHLIGHT_POST_TAG, DEFAULT_HIGHLIGHT_PRE_TAG,
    };
    use crate::index_resolver::index_store::MockIndexStore;
    use crate::index_resolver::meta_store::MockIndexMetaStore;
    use crate::index_resolver::IndexResolver;

    use super::*;

    impl IndexController<MockIndexMetaStore, MockIndexStore> {
        pub fn mock(
            index_resolver: Arc<IndexResolver<MockIndexMetaStore, MockIndexStore>>,
            task_store: TaskStore,
            update_file_store: UpdateFileStore,
            scheduler: Arc<RwLock<Scheduler>>,
        ) -> Self {
            IndexController {
                index_resolver,
                task_store,
                update_file_store,
                scheduler,
            }
        }
    }

    #[actix_rt::test]
    async fn test_search_simple() {
        let index_uid = "test";
        let index_uuid = Uuid::new_v4();
        let query = SearchQuery {
            q: Some(String::from("hello world")),
            offset: Some(10),
            limit: 0,
            attributes_to_retrieve: Some(vec!["string".to_owned()].into_iter().collect()),
            attributes_to_crop: None,
            crop_length: 18,
            attributes_to_highlight: None,
            show_matches_position: true,
            filter: None,
            sort: None,
            facets: None,
            highlight_pre_tag: DEFAULT_HIGHLIGHT_PRE_TAG(),
            highlight_post_tag: DEFAULT_HIGHLIGHT_POST_TAG(),
            crop_marker: DEFAULT_CROP_MARKER(),
        };

        let result = SearchResult {
            hits: vec![],
            estimated_total_hits: 29,
            query: "hello world".to_string(),
            limit: 24,
            offset: 0,
            processing_time_ms: 50,
            facet_distribution: None,
        };

        let mut uuid_store = MockIndexMetaStore::new();
        uuid_store
            .expect_get()
            .with(eq(index_uid.to_owned()))
            .returning(move |s| {
                Box::pin(ok((
                    s,
                    Some(crate::index_resolver::meta_store::IndexMeta {
                        uuid: index_uuid,
                        creation_task_id: 0,
                    }),
                )))
            });

        let mut index_store = MockIndexStore::new();
        let result_clone = result.clone();
        let query_clone = query.clone();
        index_store
            .expect_get()
            .with(eq(index_uuid))
            .returning(move |_uuid| {
                let result = result_clone.clone();
                let query = query_clone.clone();
                let mocker = Mocker::default();
                mocker
                    .when::<SearchQuery, IndexResult<SearchResult>>("perform_search")
                    .once()
                    .then(move |q| {
                        assert_eq!(&q, &query);
                        Ok(result.clone())
                    });
                let index = Index::mock(mocker);
                Box::pin(ok(Some(index)))
            });

        let task_store_mocker = nelson::Mocker::default();
        let mocker = Mocker::default();
        let update_file_store = UpdateFileStore::mock(mocker);
        let index_resolver = Arc::new(IndexResolver::new(
            uuid_store,
            index_store,
            update_file_store.clone(),
        ));
        let task_store = TaskStore::mock(task_store_mocker);
        let scheduler = Scheduler::new(
            task_store.clone(),
            vec![index_resolver.clone()],
            SchedulerConfig::default(),
        )
        .unwrap();
        let index_controller =
            IndexController::mock(index_resolver, task_store, update_file_store, scheduler);

        let r = index_controller
            .search(index_uid.to_owned(), query.clone())
            .await
            .unwrap();
        assert_eq!(r, result);
    }
}
