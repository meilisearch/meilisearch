use std::path::PathBuf;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::error::Result;
use crate::index::{Checked, Document, SearchQuery, SearchResult, Settings};
use crate::index_controller::updates::status::{Failed, Processed, Processing};
use crate::index_controller::{IndexSettings, IndexStats};

use super::IndexMeta;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum IndexMsg {
    CreateIndex {
        uuid: Uuid,
        primary_key: Option<String>,
        ret: oneshot::Sender<Result<IndexMeta>>,
    },
    Update {
        uuid: Uuid,
        meta: Processing,
        ret: oneshot::Sender<Result<std::result::Result<Processed, Failed>>>,
    },
    Search {
        uuid: Uuid,
        query: SearchQuery,
        ret: oneshot::Sender<Result<SearchResult>>,
    },
    Settings {
        uuid: Uuid,
        ret: oneshot::Sender<Result<Settings<Checked>>>,
    },
    Documents {
        uuid: Uuid,
        attributes_to_retrieve: Option<Vec<String>>,
        offset: usize,
        limit: usize,
        ret: oneshot::Sender<Result<Vec<Document>>>,
    },
    Document {
        uuid: Uuid,
        attributes_to_retrieve: Option<Vec<String>>,
        doc_id: String,
        ret: oneshot::Sender<Result<Document>>,
    },
    Delete {
        uuid: Uuid,
        ret: oneshot::Sender<Result<()>>,
    },
    GetMeta {
        uuid: Uuid,
        ret: oneshot::Sender<Result<IndexMeta>>,
    },
    UpdateIndex {
        uuid: Uuid,
        index_settings: IndexSettings,
        ret: oneshot::Sender<Result<IndexMeta>>,
    },
    Snapshot {
        uuid: Uuid,
        path: PathBuf,
        ret: oneshot::Sender<Result<()>>,
    },
    Dump {
        uuid: Uuid,
        path: PathBuf,
        ret: oneshot::Sender<Result<()>>,
    },
    GetStats {
        uuid: Uuid,
        ret: oneshot::Sender<Result<IndexStats>>,
    },
}

impl IndexMsg {
    pub async fn search(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
        query: SearchQuery,
    ) -> Result<SearchResult> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Search {
            ret,
            uuid,
            query,
        };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn update_index(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
        index_settings: IndexSettings,
    ) -> Result<IndexMeta> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::UpdateIndex {
            ret,
            uuid,
            index_settings,
        };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn create_index(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
        primary_key: Option<String>,
    ) -> Result<IndexMeta> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::CreateIndex {
            ret,
            uuid,
            primary_key,
        };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn index_meta(sender: &mpsc::Sender<Self>, uuid: Uuid) -> Result<IndexMeta> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::GetMeta { ret, uuid };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn index_stats(sender: &mpsc::Sender<Self>, uuid: Uuid) -> Result<IndexStats> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::GetStats { ret, uuid };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn settings(sender: &mpsc::Sender<Self>, uuid: Uuid) -> Result<Settings<Checked>> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Settings { ret, uuid };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn documents(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> Result<Vec<Document>> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Documents {
            ret,
            uuid,
            attributes_to_retrieve,
            offset,
            limit,
        };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn document(
        sender: &mpsc::Sender<Self>,
        uuid: Uuid,
        attributes_to_retrieve: Option<Vec<String>>,
        doc_id: String,
    ) -> Result<Document> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Document {
            ret,
            uuid,
            attributes_to_retrieve,
            doc_id,
        };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn update(sender: &mpsc::Sender<Self>, uuid: Uuid, meta: Processing) -> Result<std::result::Result<Processed, Failed>> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Update {
            ret,
            uuid,
            meta,
        };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn snapshot(sender: &mpsc::Sender<IndexMsg>, uuid: Uuid, path: PathBuf) -> Result<()> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Snapshot {
            uuid,
            path,
            ret,
        };
        sender.send(msg).await?;
        rcv.await?
    }

    pub async fn dump(sender: &mpsc::Sender<Self>, uuid: Uuid, path: PathBuf) -> Result<()> {
        let (ret, rcv) = oneshot::channel();
        let msg = Self::Dump {
            uuid,
            ret,
            path,
        };
        sender.send(msg).await?;
        rcv.await?
    }
}
