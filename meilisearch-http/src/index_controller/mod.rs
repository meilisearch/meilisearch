use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use actix_web::web::{Bytes, Payload};
use anyhow::bail;
use futures::stream::StreamExt;
use log::info;
use milli::update::{IndexDocumentsMethod, UpdateFormat};
use milli::FieldsDistribution;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::sleep;
use uuid::Uuid;

use index_actor::IndexActorHandle;
use snapshot::load_snapshot;
use snapshot::SnapshotService;
use update_actor::UpdateActorHandle;
pub use updates::{Failed, Processed, Processing};
use uuid_resolver::UuidError;
use uuid_resolver::UuidResolverHandle;

use crate::index::{Document, SearchQuery, SearchResult};
use crate::index::{Facets, Settings, UpdateResult};
use crate::option::Opt;

mod index_actor;
mod snapshot;
mod update_actor;
mod update_handler;
mod updates;
mod uuid_resolver;

pub type UpdateStatus = updates::UpdateStatus<UpdateMeta, UpdateResult, String>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    pub uid: String,
    name: String,
    #[serde(flatten)]
    pub meta: index_actor::IndexMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition {
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        primary_key: Option<String>,
    },
    ClearDocuments,
    DeleteDocuments,
    Settings(Settings),
    Facets(Facets),
}

#[derive(Clone, Debug)]
pub struct IndexSettings {
    pub uid: Option<String>,
    pub primary_key: Option<String>,
}

#[derive(Clone, Debug)]
pub struct IndexStats {
    pub size: u64,
    pub number_of_documents: u64,
    pub is_indexing: bool,
    pub fields_distribution: FieldsDistribution,
}

pub struct IndexController {
    uuid_resolver: uuid_resolver::UuidResolverHandleImpl,
    index_handle: index_actor::IndexActorHandleImpl,
    update_handle: update_actor::UpdateActorHandleImpl<Bytes>,
}

impl IndexController {
    pub fn new(path: impl AsRef<Path>, options: &Opt) -> anyhow::Result<Self> {
        let index_size = options.max_mdb_size.get_bytes() as usize;
        let update_store_size = options.max_udb_size.get_bytes() as usize;

        if let Some(ref path) = options.import_snapshot {
            info!("Loading from snapshot {:?}", path);
            load_snapshot(
                &options.db_path,
                path,
                options.ignore_snapshot_if_db_exists,
                options.ignore_missing_snapshot,
            )?;
        }

        std::fs::create_dir_all(&path)?;

        let uuid_resolver = uuid_resolver::UuidResolverHandleImpl::new(&path)?;
        let index_handle = index_actor::IndexActorHandleImpl::new(&path, index_size)?;
        let update_handle = update_actor::UpdateActorHandleImpl::new(
            index_handle.clone(),
            &path,
            update_store_size,
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
        })
    }

    pub async fn add_documents(
        &self,
        uid: String,
        method: milli::update::IndexDocumentsMethod,
        format: milli::update::UpdateFormat,
        payload: Payload,
        primary_key: Option<String>,
    ) -> anyhow::Result<UpdateStatus> {
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
                    .map(|bytes| {
                        bytes.map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Sync + Send + 'static>
                        })
                    })
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
            Err(UuidError::UnexistingIndex(name)) => {
                let uuid = Uuid::new_v4();
                let status = perform_update(uuid).await?;
                self.uuid_resolver.insert(name, uuid).await?;
                Ok(status)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn clear_documents(&self, uid: String) -> anyhow::Result<UpdateStatus> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let meta = UpdateMeta::ClearDocuments;
        let (_, receiver) = mpsc::channel(1);
        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn delete_documents(
        &self,
        uid: String,
        document_ids: Vec<String>,
    ) -> anyhow::Result<UpdateStatus> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let meta = UpdateMeta::DeleteDocuments;
        let (sender, receiver) = mpsc::channel(10);

        tokio::task::spawn(async move {
            let json = serde_json::to_vec(&document_ids).unwrap();
            let bytes = Bytes::from(json);
            let _ = sender.send(Ok(bytes)).await;
        });

        let status = self.update_handle.update(meta, receiver, uuid).await?;
        Ok(status)
    }

    pub async fn update_settings(
        &self,
        uid: String,
        settings: Settings,
        create: bool,
    ) -> anyhow::Result<UpdateStatus> {
        let perform_udpate = |uuid| async move {
            let meta = UpdateMeta::Settings(settings);
            // Nothing so send, drop the sender right away, as not to block the update actor.
            let (_, receiver) = mpsc::channel(1);
            self.update_handle.update(meta, receiver, uuid).await
        };

        match self.uuid_resolver.get(uid).await {
            Ok(uuid) => Ok(perform_udpate(uuid).await?),
            Err(UuidError::UnexistingIndex(name)) if create => {
                let uuid = Uuid::new_v4();
                let status = perform_udpate(uuid).await?;
                self.uuid_resolver.insert(name, uuid).await?;
                Ok(status)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_index(
        &self,
        index_settings: IndexSettings,
    ) -> anyhow::Result<IndexMetadata> {
        let IndexSettings { uid, primary_key } = index_settings;
        let uid = uid.ok_or_else(|| anyhow::anyhow!("Can't create an index without a uid."))?;
        let uuid = self.uuid_resolver.create(uid.clone()).await?;
        let meta = self.index_handle.create_index(uuid, primary_key).await?;
        let meta = IndexMetadata {
            name: uid.clone(),
            uid,
            meta,
        };

        Ok(meta)
    }

    pub async fn delete_index(&self, uid: String) -> anyhow::Result<()> {
        let uuid = self.uuid_resolver.delete(uid).await?;
        self.update_handle.delete(uuid).await?;
        self.index_handle.delete(uuid).await?;
        Ok(())
    }

    pub async fn update_status(&self, uid: String, id: u64) -> anyhow::Result<UpdateStatus> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let result = self.update_handle.update_status(uuid, id).await?;
        Ok(result)
    }

    pub async fn all_update_status(&self, uid: String) -> anyhow::Result<Vec<UpdateStatus>> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let result = self.update_handle.get_all_updates_status(uuid).await?;
        Ok(result)
    }

    pub async fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        let uuids = self.uuid_resolver.list().await?;

        let mut ret = Vec::new();

        for (uid, uuid) in uuids {
            let meta = self.index_handle.get_index_meta(uuid).await?;
            let meta = IndexMetadata {
                name: uid.clone(),
                uid,
                meta,
            };
            ret.push(meta);
        }

        Ok(ret)
    }

    pub async fn settings(&self, uid: String) -> anyhow::Result<Settings> {
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
    ) -> anyhow::Result<Vec<Document>> {
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
    ) -> anyhow::Result<Document> {
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
        index_settings: IndexSettings,
    ) -> anyhow::Result<IndexMetadata> {
        if index_settings.uid.is_some() {
            bail!("Can't change the index uid.")
        }

        let uuid = self.uuid_resolver.get(uid.clone()).await?;
        let meta = self.index_handle.update_index(uuid, index_settings).await?;
        let meta = IndexMetadata {
            name: uid.clone(),
            uid,
            meta,
        };
        Ok(meta)
    }

    pub async fn search(&self, uid: String, query: SearchQuery) -> anyhow::Result<SearchResult> {
        let uuid = self.uuid_resolver.get(uid).await?;
        let result = self.index_handle.search(uuid, query).await?;
        Ok(result)
    }

    pub async fn get_index(&self, uid: String) -> anyhow::Result<IndexMetadata> {
        let uuid = self.uuid_resolver.get(uid.clone()).await?;
        let meta = self.index_handle.get_index_meta(uuid).await?;
        let meta = IndexMetadata {
            name: uid.clone(),
            uid,
            meta,
        };
        Ok(meta)
    }

    pub async fn get_stats(&self, uid: String) -> anyhow::Result<IndexStats> {
        let uuid = self.uuid_resolver.get(uid.clone()).await?;

        Ok(self.index_handle.get_index_stats(uuid).await?)
    }

    pub async fn get_updates_size(&self) -> anyhow::Result<u64> {
        Ok(self.update_handle.get_size().await?)
    }

    pub async fn get_uuids_size(&self) -> anyhow::Result<u64> {
        Ok(self.uuid_resolver.get_size().await?)
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
