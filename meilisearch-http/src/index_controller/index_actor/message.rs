use std::path::PathBuf;

use tokio::sync::oneshot;
use uuid::Uuid;

use crate::index::{Document, SearchQuery, SearchResult, Settings};
use crate::index_controller::{
    updates::Processing,
    UpdateMeta,
};
use super::{IndexSettings, IndexMeta, UpdateResult, Result};

pub enum IndexMsg {
    CreateIndex {
        uuid: Uuid,
        primary_key: Option<String>,
        ret: oneshot::Sender<Result<IndexMeta>>,
    },
    Update {
        meta: Processing<UpdateMeta>,
        data: std::fs::File,
        ret: oneshot::Sender<Result<UpdateResult>>,
    },
    Search {
        uuid: Uuid,
        query: SearchQuery,
        ret: oneshot::Sender<anyhow::Result<SearchResult>>,
    },
    Settings {
        uuid: Uuid,
        ret: oneshot::Sender<Result<Settings>>,
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
}
