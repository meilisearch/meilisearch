use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use actix_web::error::PayloadError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use log::info;
use milli::update::IndexDocumentsMethod;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use uuid::Uuid;

use dump_actor::DumpActorHandle;
pub use dump_actor::{DumpInfo, DumpStatus};
use snapshot::load_snapshot;

use crate::index::{Checked, Document, IndexMeta, IndexStats, SearchQuery, SearchResult, Settings, Unchecked};
use crate::index_controller::index_resolver::create_index_resolver;
use crate::index_controller::snapshot::SnapshotService;
use crate::options::IndexerOpts;
use error::Result;
use crate::index::error::Result as IndexResult;

use self::dump_actor::load_dump;
use self::index_resolver::HardStateIndexResolver;
use self::index_resolver::error::IndexResolverError;
use self::updates::status::UpdateStatus;
use self::updates::UpdateMsg;

mod dump_actor;
pub mod error;
//pub mod indexes;
mod snapshot;
pub mod update_file_store;
pub mod updates;
//mod uuid_resolver;
mod index_resolver;

pub type Payload = Box<
    dyn Stream<Item = std::result::Result<Bytes, PayloadError>> + Send + Sync + 'static + Unpin,
>;

macro_rules! time {
    ($e:expr) => {
        {
            let now = std::time::Instant::now();
            let result = $e;
            let elapsed = now.elapsed();
            println!("elapsed at line {}: {}ms ({}ns)", line!(), elapsed.as_millis(), elapsed.as_nanos());
            result
        }
    };
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    #[serde(skip)]
    pub uuid: Uuid,
    pub uid: String,
    name: String,
    #[serde(flatten)]
    pub meta: IndexMeta,
}

#[derive(Clone, Debug)]
pub struct IndexSettings {
    pub uid: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Clone)]
pub struct IndexController {
    index_resolver: Arc<HardStateIndexResolver>,
    update_sender: updates::UpdateSender,
    dump_handle: dump_actor::DumpActorHandleImpl,
}

#[derive(Debug)]
pub enum DocumentAdditionFormat {
    Json,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub database_size: u64,
    pub last_update: Option<DateTime<Utc>>,
    pub indexes: BTreeMap<String, IndexStats>,
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub enum Update {
    DeleteDocuments(Vec<String>),
    ClearDocuments,
    Settings(Settings<Unchecked>),
    DocumentAddition {
        #[derivative(Debug="ignore")]
        payload: Payload,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        format: DocumentAdditionFormat,
    },
}

#[derive(Default, Debug)]
pub struct IndexControllerBuilder {
    max_index_size: Option<usize>,
    max_update_store_size: Option<usize>,
    snapshot_dir: Option<PathBuf>,
    import_snapshot: Option<PathBuf>,
    snapshot_interval: Option<Duration>,
    ignore_snapshot_if_db_exists: bool,
    ignore_missing_snapshot: bool,
    schedule_snapshot: bool,
    dump_src: Option<PathBuf>,
    dump_dst: Option<PathBuf>,
}

impl IndexControllerBuilder {
    pub fn build(
        self,
        db_path: impl AsRef<Path>,
        indexer_options: IndexerOpts,
    ) -> anyhow::Result<IndexController> {
        let index_size = self
            .max_index_size
            .ok_or_else(|| anyhow::anyhow!("Missing index size"))?;
        let update_store_size = self
            .max_index_size
            .ok_or_else(|| anyhow::anyhow!("Missing update database size"))?;

        if let Some(ref path) = self.import_snapshot {
            info!("Loading from snapshot {:?}", path);
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
                index_size,
                update_store_size,
                &indexer_options,
            )?;
        }

        std::fs::create_dir_all(db_path.as_ref())?;

        let index_resolver = Arc::new(create_index_resolver(&db_path, index_size, &indexer_options)?);

        #[allow(unreachable_code)]
        let update_sender = updates::create_update_handler(index_resolver.clone(), &db_path, update_store_size)?;

        let dump_path = self.dump_dst.ok_or_else(|| anyhow::anyhow!("Missing dump directory path"))?;
        let dump_handle = dump_actor::DumpActorHandleImpl::new(
            dump_path,
            index_resolver.clone(),
            update_sender.clone(),
            index_size,
            update_store_size,
        )?;

        if self.schedule_snapshot {
            let snapshot_service = SnapshotService::new(
                index_resolver.clone(),
                update_sender.clone(),
                self.snapshot_interval.ok_or_else(|| anyhow::anyhow!("Snapshot interval not provided."))?,
                self.snapshot_dir.ok_or_else(|| anyhow::anyhow!("Snapshot path not provided."))?,
                db_path
                .as_ref()
                .file_name()
                .map(|n| n.to_owned().into_string().expect("invalid path"))
                .unwrap_or_else(|| String::from("data.ms")),
            );

            tokio::task::spawn(snapshot_service.run());
        }

        Ok(IndexController {
            index_resolver,
            update_sender,
            dump_handle,
        })
    }

    /// Set the index controller builder's max update store size.
    pub fn set_max_update_store_size(&mut self, max_update_store_size: usize) -> &mut Self {
        self.max_update_store_size.replace(max_update_store_size);
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
}

impl IndexController {
    pub fn builder() -> IndexControllerBuilder {
        IndexControllerBuilder::default()
    }

    pub async fn register_update(&self, uid: String, update: Update) -> Result<UpdateStatus> {
        match self.index_resolver.get_uuid(uid).await {
            Ok(uuid) => {
                let update_result = UpdateMsg::update(&self.update_sender, uuid, update).await?;
                Ok(update_result)
            }
            Err(IndexResolverError::UnexistingIndex(name)) => {
                let (uuid, _) = self.index_resolver.create_index(name, None).await?;
                let update_result = UpdateMsg::update(&self.update_sender, uuid, update).await?;
                // ignore if index creation fails now, since it may already have been created

                Ok(update_result)
            }
            Err(e) => Err(e.into()),
        }
    }

    //pub async fn add_documents(
    //&self,
    //uid: String,
    //method: milli::update::IndexDocumentsMethod,
    //payload: Payload,
    //primary_key: Option<String>,
    //) -> Result<UpdateStatus> {
    //let perform_update = |uuid| async move {
    //let meta = UpdateMeta::DocumentsAddition {
    //method,
    //primary_key,
    //};
    //let (sender, receiver) = mpsc::channel(10);

    //// It is necessary to spawn a local task to send the payload to the update handle to
    //// prevent dead_locking between the update_handle::update that waits for the update to be
    //// registered and the update_actor that waits for the the payload to be sent to it.
    //tokio::task::spawn_local(async move {
    //payload
    //.for_each(|r| async {
    //let _ = sender.send(r).await;
    //})
    //.await
    //});

    //// This must be done *AFTER* spawning the task.
    //self.update_handle.update(meta, receiver, uuid).await
    //};

    //match self.uuid_resolver.get(uid).await {
    //Ok(uuid) => Ok(perform_update(uuid).await?),
    //Err(UuidResolverError::UnexistingIndex(name)) => {
    //let uuid = Uuid::new_v4();
    //let status = perform_update(uuid).await?;
    //// ignore if index creation fails now, since it may already have been created
    //let _ = self.index_handle.create_index(uuid, None).await;
    //self.uuid_resolver.insert(name, uuid).await?;
    //Ok(status)
    //}
    //Err(e) => Err(e.into()),
    //}
    //}

    //pub async fn clear_documents(&self, uid: String) -> Result<UpdateStatus> {
    //let uuid = self.uuid_resolver.get(uid).await?;
    //let meta = UpdateMeta::ClearDocuments;
    //let (_, receiver) = mpsc::channel(1);
    //let status = self.update_handle.update(meta, receiver, uuid).await?;
    //Ok(status)
    //}

    //pub async fn delete_documents(
    //&self,
    //uid: String,
    //documents: Vec<String>,
    //) -> Result<UpdateStatus> {
    //let uuid = self.uuid_resolver.get(uid).await?;
    //let meta = UpdateMeta::DeleteDocuments { ids: documents };
    //let (_, receiver) = mpsc::channel(1);
    //let status = self.update_handle.update(meta, receiver, uuid).await?;
    //Ok(status)
    //}

    //pub async fn update_settings(
    //&self,
    //uid: String,
    //settings: Settings<Checked>,
    //create: bool,
    //) -> Result<UpdateStatus> {
    //let perform_udpate = |uuid| async move {
    //let meta = UpdateMeta::Settings(settings.into_unchecked());
    //// Nothing so send, drop the sender right away, as not to block the update actor.
    //let (_, receiver) = mpsc::channel(1);
    //self.update_handle.update(meta, receiver, uuid).await
    //};

    //match self.uuid_resolver.get(uid).await {
    //Ok(uuid) => Ok(perform_udpate(uuid).await?),
    //Err(UuidResolverError::UnexistingIndex(name)) if create => {
    //let uuid = Uuid::new_v4();
    //let status = perform_udpate(uuid).await?;
    //// ignore if index creation fails now, since it may already have been created
    //let _ = self.index_handle.create_index(uuid, None).await;
    //self.uuid_resolver.insert(name, uuid).await?;
    //Ok(status)
    //}
    //Err(e) => Err(e.into()),
    //}
    //}

    //pub async fn create_index(&self, index_settings: IndexSettings) -> Result<IndexMetadata> {
    //let IndexSettings { uid, primary_key } = index_settings;
    //let uid = uid.ok_or(IndexControllerError::MissingUid)?;
    //let uuid = Uuid::new_v4();
    //let meta = self.index_handle.create_index(uuid, primary_key).await?;
    //self.uuid_resolver.insert(uid.clone(), uuid).await?;
    //let meta = IndexMetadata {
    //uuid,
    //name: uid.clone(),
    //uid,
    //meta,
    //};

    //Ok(meta)
    //}

    //pub async fn delete_index(&self, uid: String) -> Result<()> {
    //let uuid = self.uuid_resolver.delete(uid).await?;

    //// We remove the index from the resolver synchronously, and effectively perform the index
    //// deletion as a background task.
    //let update_handle = self.update_handle.clone();
    //let index_handle = self.index_handle.clone();
    //tokio::spawn(async move {
    //if let Err(e) = update_handle.delete(uuid).await {
    //error!("Error while deleting index: {}", e);
    //}
    //if let Err(e) = index_handle.delete(uuid).await {
    //error!("Error while deleting index: {}", e);
    //}
    //});

    //Ok(())
    //}

    pub async fn update_status(&self, uid: String, id: u64) -> Result<UpdateStatus> {
        let uuid = self.index_resolver.get_uuid(uid).await?;
        let result = UpdateMsg::get_update(&self.update_sender, uuid, id).await?;
        Ok(result)
    }

    pub async fn all_update_status(&self, uid: String) -> Result<Vec<UpdateStatus>> {
        let uuid = self.index_resolver.get_uuid(uid).await?;
        let result = UpdateMsg::list_updates(&self.update_sender, uuid).await?;
        Ok(result)
    }

    pub async fn list_indexes(&self) -> Result<Vec<IndexMetadata>> {
        let indexes = self.index_resolver.list().await?;
        let mut ret = Vec::new();
        for (uid, index) in indexes {
            let meta = index.meta()?;
            let meta = IndexMetadata {
                uuid: index.uuid,
                name: uid.clone(),
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

    pub async fn documents(
        &self,
        uid: String,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        let index = self.index_resolver.get_index(uid).await?;
        let documents = spawn_blocking(move || index.retrieve_documents(offset, limit, attributes_to_retrieve)).await??;
        Ok(documents)
    }

    pub async fn document(
        &self,
        uid: String,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        let index = self.index_resolver.get_index(uid).await?;
        let document = spawn_blocking(move || index.retrieve_document(doc_id, attributes_to_retrieve)).await??;
        Ok(document)
    }

    pub async fn update_index(
        &self,
        uid: String,
        mut index_settings: IndexSettings,
    ) -> Result<IndexMetadata> {

        index_settings.uid.take();

        let index = self.index_resolver.get_index(uid.clone()).await?;
        let uuid = index.uuid;
        let meta = spawn_blocking(move || index.update_primary_key(index_settings.primary_key)).await??;
        let meta = IndexMetadata {
            uuid,
            name: uid.clone(),
            uid,
            meta,
        };
        Ok(meta)
    }

    pub async fn search(&self, uid: String, query: SearchQuery) -> Result<SearchResult> {
        let index = time!(self.index_resolver.get_index(uid.clone()).await?);
        let result = time!(spawn_blocking(move || time!(index.perform_search(query))).await??);
        Ok(result)
    }

    pub async fn get_index(&self, uid: String) -> Result<IndexMetadata> {
        let index = self.index_resolver.get_index(uid.clone()).await?;
        let uuid = index.uuid;
        let meta = spawn_blocking(move || index.meta()).await??;
        let meta = IndexMetadata {
            uuid,
            name: uid.clone(),
            uid,
            meta,
        };
        Ok(meta)
    }

    pub async fn get_index_stats(&self, uid: String) -> Result<IndexStats> {
        let update_infos = UpdateMsg::get_info(&self.update_sender).await?;
        let index = self.index_resolver.get_index(uid).await?;
        let uuid = index.uuid;
        let mut stats = spawn_blocking(move || index.stats()).await??;
        // Check if the currently indexing update is from our index.
        stats.is_indexing = Some(Some(uuid) == update_infos.processing);
        Ok(stats)
    }

    pub async fn get_all_stats(&self) -> Result<Stats> {
        let update_infos = UpdateMsg::get_info(&self.update_sender).await?;
        let mut database_size = self.index_resolver.get_uuids_size().await? + update_infos.size;
        let mut last_update: Option<DateTime<_>> = None;
        let mut indexes = BTreeMap::new();

        for (index_uid, index) in self.index_resolver.list().await? {
            let uuid = index.uuid;
            let (mut stats, meta) = spawn_blocking::<_, IndexResult<_>>(move || {
                let stats = index.stats()?;
                let meta = index.meta()?;
                Ok((stats, meta))
            }).await??;

            database_size += stats.size;

            last_update = last_update.map_or(Some(meta.updated_at), |last| {
                Some(last.max(meta.updated_at))
            });

            // Check if the currently indexing update is from our index.
            stats.is_indexing = Some(Some(uuid) == update_infos.processing);

            indexes.insert(index_uid, stats);
        }

        Ok(Stats {
            database_size,
            last_update,
            indexes,
        })
    }

    pub async fn create_dump(&self) -> Result<DumpInfo> {
        Ok(self.dump_handle.create_dump().await?)
    }

    pub async fn dump_info(&self, uid: String) -> Result<DumpInfo> {
        Ok(self.dump_handle.dump_info(uid).await?)
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

/// Parses the v1 version of the Asc ranking rules `asc(price)`and returns the field name.
pub fn asc_ranking_rule(text: &str) -> Option<&str> {
    text.split_once("asc(")
        .and_then(|(_, tail)| tail.rsplit_once(")"))
        .map(|(field, _)| field)
}

/// Parses the v1 version of the Desc ranking rules `asc(price)`and returns the field name.
pub fn desc_ranking_rule(text: &str) -> Option<&str> {
    text.split_once("desc(")
        .and_then(|(_, tail)| tail.rsplit_once(")"))
        .map(|(field, _)| field)
}

fn update_files_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().join("updates/updates_files")
}
