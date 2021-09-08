use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use actix_web::web::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use log::error;
use log::info;
use milli::FieldDistribution;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::sleep;
use uuid::Uuid;

use dump_actor::DumpActorHandle;
pub use dump_actor::{DumpInfo, DumpStatus};
use index_actor::IndexActorHandle;
use snapshot::{load_snapshot, SnapshotService};
use update_actor::UpdateActorHandle;
pub use updates::*;
use uuid_resolver::{error::UuidResolverError, UuidResolverHandle};

use crate::extractors::payload::Payload;
use crate::index::{Checked, Document, SearchQuery, SearchResult, Settings};
use crate::option::Opt;
use error::Result;

use self::dump_actor::load_dump;
use self::error::IndexControllerError;

mod dump_actor;
pub mod error;
pub mod index_actor;
mod snapshot;
mod update_actor;
mod updates;
mod uuid_resolver;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    #[serde(skip)]
    pub uuid: Uuid,
    pub uid: String,
    name: String,
    #[serde(flatten)]
    pub meta: index_actor::IndexMeta,
}

#[derive(Clone, Debug)]
pub struct IndexSettings {
    pub uid: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct IndexStats {
    #[serde(skip)]
    pub size: u64,
    pub number_of_documents: u64,
    /// Whether the current index is performing an update. It is initially `None` when the
    /// index returns it, since it is the `UpdateStore` that knows what index is currently indexing. It is
    /// later set to either true or false, we we retrieve the information from the `UpdateStore`
    pub is_indexing: Option<bool>,
    pub field_distribution: FieldDistribution,
}

#[derive(Clone)]
pub struct IndexController {
    uuid_resolver: uuid_resolver::UuidResolverHandleImpl,
    index_handle: index_actor::IndexActorHandleImpl,
    update_handle: update_actor::UpdateActorHandleImpl<Bytes>,
    dump_handle: dump_actor::DumpActorHandleImpl,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub database_size: u64,
    pub last_update: Option<DateTime<Utc>>,
    pub indexes: BTreeMap<String, IndexStats>,
}

impl IndexController {
    pub fn new(path: impl AsRef<Path>, options: &Opt) -> anyhow::Result<Self> {
        let index_size = options.max_index_size.get_bytes() as usize;
        let update_store_size = options.max_index_size.get_bytes() as usize;

        if let Some(ref path) = options.import_snapshot {
            info!("Loading from snapshot {:?}", path);
            load_snapshot(
                &options.db_path,
                path,
                options.ignore_snapshot_if_db_exists,
                options.ignore_missing_snapshot,
            )?;
        } else if let Some(ref src_path) = options.import_dump {
            load_dump(
                &options.db_path,
                src_path,
                options.max_index_size.get_bytes() as usize,
                options.max_udb_size.get_bytes() as usize,
                &options.indexer_options,
            )?;
        }

        std::fs::create_dir_all(&path)?;

        let uuid_resolver = uuid_resolver::UuidResolverHandleImpl::new(&path)?;
        let index_handle =
            index_actor::IndexActorHandleImpl::new(&path, index_size, &options.indexer_options)?;
        let update_handle = update_actor::UpdateActorHandleImpl::new(
            index_handle.clone(),
            &path,
            update_store_size,
        )?;
        let dump_handle = dump_actor::DumpActorHandleImpl::new(
            &options.dumps_dir,
            uuid_resolver.clone(),
            update_handle.clone(),
            options.max_index_size.get_bytes() as usize,
            options.max_udb_size.get_bytes() as usize,
        )?;

        if options.schedule_snapshot {
            let snapshot_service = SnapshotService::new(
                uuid_resolver.clone(),
                update_handle.clone(),
                Duration::from_secs(options.snapshot_interval_sec),
                options.snapshot_dir.clone(),
                options
                    .db_path
                    .file_name()
                    .map(|n| n.to_owned().into_string().expect("invalid path"))
                    .unwrap_or_else(|| String::from("data.ms")),
            );

            tokio::task::spawn(snapshot_service.run());
        }

        Ok(Self {
            uuid_resolver,
            index_handle,
            update_handle,
            dump_handle,
        })
    }

    pub async fn add_documents(
        &self,
        uid: String,
        method: milli::update::IndexDocumentsMethod,
        format: milli::update::UpdateFormat,
        payload: Payload,
        primary_key: Option<String>,
    ) -> Result<UpdateStatus> {
        let perform_update = |uuid| async move {
            let meta = UpdateMeta::DocumentsAddition {
                method,
                format,
                primary_key,
            };
            let (sender, receiver) = mpsc::channel(10);

            // It is necessary to spawn a local task to send the payload to the update handle to
            // prevent dead_locking between the update_handle::update that waits for the update to be
            // registered and the update_actor that waits for the the payload to be sent to it.
            tokio::task::spawn_local(async move {
                payload
                    .for_each(|r| async {
                        let _ = sender.send(r).await;
                    })
                    .await
            });

            // This must be done *AFTER* spawning the task.
            self.update_handle.update(meta, receiver, uuid).await
        };

        match self.uuid_resolver.get(uid).await {
            Ok(uuid) => Ok(perform_update(uuid).await?),
            Err(UuidResolverError::UnexistingIndex(name)) => {
                let uuid = Uuid::new_v4();
                let status = perform_update(uuid).await?;
                // ignore if index creation fails now, since it may already have been created
                let _ = self.index_handle.create_index(uuid, None).await;
                self.uuid_resolver.insert(name, uuid).await?;
                Ok(status)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn clear_documents(&self, uid: String) -> Result<UpdateStatus> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let meta = UpdateMeta::ClearDocuments;
        let (_, receiver) = mpsc::channel(1);
        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn delete_documents(
        &self,
        uid: String,
        documents: Vec<String>,
    ) -> Result<UpdateStatus> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let meta = UpdateMeta::DeleteDocuments { ids: documents };
        let (_, receiver) = mpsc::channel(1);
        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn update_settings(
        &self,
        uid: String,
        settings: Settings<Checked>,
        create: bool,
    ) -> Result<UpdateStatus> {
        let perform_udpate = |uuid| async move {
            let meta = UpdateMeta::Settings(settings.into_unchecked());
            // Nothing so send, drop the sender right away, as not to block the update actor.
            let (_, receiver) = mpsc::channel(1);
            self.update_handle.update(meta, receiver, uuid).await
        };

        match self.uuid_resolver.get(uid).await {
            Ok(uuid) => Ok(perform_udpate(uuid).await?),
            Err(UuidResolverError::UnexistingIndex(name)) if create => {
                let uuid = Uuid::new_v4();
                let status = perform_udpate(uuid).await?;
                // ignore if index creation fails now, since it may already have been created
                let _ = self.index_handle.create_index(uuid, None).await;
                self.uuid_resolver.insert(name, uuid).await?;
                Ok(status)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_index(&self, index_settings: IndexSettings) -> Result<IndexMetadata> {
        let IndexSettings { uid, primary_key } = index_settings;
        let uid = uid.ok_or(IndexControllerError::MissingUid)?;
        let uuid = Uuid::new_v4();
        let meta = self.index_handle.create_index(uuid, primary_key).await?;
        self.uuid_resolver.insert(uid.clone(), uuid).await?;
        let meta = IndexMetadata {
            uuid,
            name: uid.clone(),
            uid,
            meta,
        };

        Ok(meta)
    }

    pub async fn delete_index(&self, uid: String) -> Result<()> {
        let uuid = self.uuid_resolver.delete(uid).await?;

        // We remove the index from the resolver synchronously, and effectively perform the index
        // deletion as a background task.
        let update_handle = self.update_handle.clone();
        let index_handle = self.index_handle.clone();
        tokio::spawn(async move {
            if let Err(e) = update_handle.delete(uuid).await {
                error!("Error while deleting index: {}", e);
            }
            if let Err(e) = index_handle.delete(uuid).await {
                error!("Error while deleting index: {}", e);
            }
        });

        Ok(())
    }

    pub async fn update_status(&self, uid: String, id: u64) -> Result<UpdateStatus> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let result = self.update_handle.update_status(uuid, id).await?;
        Ok(result)
    }

    pub async fn all_update_status(&self, uid: String) -> Result<Vec<UpdateStatus>> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let result = self.update_handle.get_all_updates_status(uuid).await?;
        Ok(result)
    }

    pub async fn list_indexes(&self) -> Result<Vec<IndexMetadata>> {
        let uuids = self.uuid_resolver.list().await?;

        let mut ret = Vec::new();

        for (uid, uuid) in uuids {
            let meta = self.index_handle.get_index_meta(uuid).await?;
            let meta = IndexMetadata {
                uuid,
                name: uid.clone(),
                uid,
                meta,
            };
            ret.push(meta);
        }

        Ok(ret)
    }

    pub async fn settings(&self, uid: String) -> Result<Settings<Checked>> {
        let uuid = self.uuid_resolver.get(uid.clone()).await?;
        let settings = self.index_handle.settings(uuid).await?;
        Ok(settings)
    }

    pub async fn documents(
        &self,
        uid: String,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        let uuid = self.uuid_resolver.get(uid.clone()).await?;
        let documents = self
            .index_handle
            .documents(uuid, offset, limit, attributes_to_retrieve)
            .await?;
        Ok(documents)
    }

    pub async fn document(
        &self,
        uid: String,
        doc_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Document> {
        let uuid = self.uuid_resolver.get(uid.clone()).await?;
        let document = self
            .index_handle
            .document(uuid, doc_id, attributes_to_retrieve)
            .await?;
        Ok(document)
    }

    pub async fn update_index(
        &self,
        uid: String,
        mut index_settings: IndexSettings,
    ) -> Result<IndexMetadata> {
        if index_settings.uid.is_some() {
            index_settings.uid.take();
        }

        let uuid = self.uuid_resolver.get(uid.clone()).await?;
        let meta = self.index_handle.update_index(uuid, index_settings).await?;
        let meta = IndexMetadata {
            uuid,
            name: uid.clone(),
            uid,
            meta,
        };
        Ok(meta)
    }

    pub async fn search(&self, uid: String, query: SearchQuery) -> Result<SearchResult> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let result = self.index_handle.search(uuid, query).await?;
        Ok(result)
    }

    pub async fn get_index(&self, uid: String) -> Result<IndexMetadata> {
        let uuid = self.uuid_resolver.get(uid.clone()).await?;
        let meta = self.index_handle.get_index_meta(uuid).await?;
        let meta = IndexMetadata {
            uuid,
            name: uid.clone(),
            uid,
            meta,
        };
        Ok(meta)
    }

    pub async fn get_uuids_size(&self) -> Result<u64> {
        Ok(self.uuid_resolver.get_size().await?)
    }

    pub async fn get_index_stats(&self, uid: String) -> Result<IndexStats> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let update_infos = self.update_handle.get_info().await?;
        let mut stats = self.index_handle.get_index_stats(uuid).await?;
        // Check if the currently indexing update is from out index.
        stats.is_indexing = Some(Some(uuid) == update_infos.processing);
        Ok(stats)
    }

    pub async fn get_all_stats(&self) -> Result<Stats> {
        let update_infos = self.update_handle.get_info().await?;
        let mut database_size = self.get_uuids_size().await? + update_infos.size;
        let mut last_update: Option<DateTime<_>> = None;
        let mut indexes = BTreeMap::new();

        for index in self.list_indexes().await? {
            let mut index_stats = self.index_handle.get_index_stats(index.uuid).await?;
            database_size += index_stats.size;

            last_update = last_update.map_or(Some(index.meta.updated_at), |last| {
                Some(last.max(index.meta.updated_at))
            });

            index_stats.is_indexing = Some(Some(index.uuid) == update_infos.processing);

            indexes.insert(index.uid, index_stats);
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
