use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use heed::types::{Str, SerdeBincode};
use heed::{EnvOpenOptions, Env, Database};
use milli::{Index, FieldsIdsMap, SearchResult, FieldId, facet::FacetType};
use serde::{Serialize, Deserialize};
use ouroboros::self_referencing;

use crate::data::SearchQuery;

const CONTROLLER_META_FILENAME: &str = "index_controller_meta";
const INDEXES_CONTROLLER_FILENAME: &str = "indexes_db";
const INDEXES_DB_NAME: &str = "indexes_db";


#[derive(Debug, Serialize, Deserialize)]
struct IndexStoreMeta {
    open_options: EnvOpenOptions,
    created_at: DateTime<Utc>,
}

impl IndexStoreMeta {
    fn from_path(path: impl AsRef<Path>) -> Result<Option<IndexStoreMeta>> {
        let mut path = path.as_ref().to_path_buf();
        path.push(CONTROLLER_META_FILENAME);
        if path.exists() {
            let mut file = File::open(path)?;
            let mut buffer = Vec::new();
            let n = file.read_to_end(&mut buffer)?;
            let meta: IndexStoreMeta = serde_json::from_slice(&buffer[..n])?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    fn to_path(self, path: impl AsRef<Path>) -> Result<()> {
        let mut path = path.as_ref().to_path_buf();
        path.push(CONTROLLER_META_FILENAME);
        if path.exists() {
            Err(anyhow::anyhow!("Index controller metadata already exists"))
        } else {
            let mut file = File::create(path)?;
            let json = serde_json::to_vec(&self)?;
            file.write_all(&json)?;
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    created_at: DateTime<Utc>,
    open_options: EnvOpenOptions,
    uuid: String,
}

impl IndexMetadata {
    fn open_index(self, path: impl AsRef<Path>) -> Result<Index> {
        // create a path in the form "db_path/indexes/index_id"
        let mut path = path.as_ref().to_path_buf();
        path.push("indexes");
        path.push(&self.uuid);
        Ok(Index::new(self.open_options, path)?)
    }
}


#[self_referencing]
pub struct IndexView {
    pub index: Arc<Index>,
    #[borrows(index)]
    #[covariant]
    pub txn: heed::RoTxn<'this>,
    uuid: String,
}

impl IndexView {
    pub fn search(&self, search_query: &SearchQuery) -> Result<SearchResult> {
        self.with(|this| {
            let mut search = this.index.search(&this.txn);
            if let Some(query) = &search_query.q {
                search.query(query);
            }

            if let Some(offset) = search_query.offset {
                search.offset(offset);
            }

            let limit = search_query.limit;
            search.limit(limit);

            Ok(search.execute()?)
        })
    }

    #[inline]
    pub fn fields_ids_map(&self) -> Result<FieldsIdsMap> {
        self.with(|this| Ok(this.index.fields_ids_map(&this.txn)?))

    }

    #[inline]
    pub fn displayed_fields_ids(&self) -> Result<Option<Vec<FieldId>>> {
        self.with(|this| Ok(this.index.displayed_fields_ids(&this.txn)?))
    }

    #[inline]
    pub fn displayed_fields(&self) -> Result<Option<Vec<String>>> {
        self.with(|this| Ok(this.index
                .displayed_fields(&this.txn)?
                .map(|fields| fields.into_iter().map(String::from).collect())))
    }

    #[inline]
    pub fn searchable_fields(&self) -> Result<Option<Vec<String>>> {
        self.with(|this| Ok(this.index
                .searchable_fields(&this.txn)?
                .map(|fields| fields.into_iter().map(String::from).collect())))
    }

    #[inline]
    pub fn faceted_fields(&self) -> Result<HashMap<std::string::String, FacetType>> {
        self.with(|this| Ok(this.index.faceted_fields(&this.txn)?))
    }

    pub fn documents(&self, ids: &[u32]) -> Result<Vec<(u32, obkv::KvReader<'_>)>> {
        let txn = self.borrow_txn();
        let index = self.borrow_index();
        Ok(index.documents(txn, ids.into_iter().copied())?)
    }

    //pub async fn add_documents<B, E>(
        //&self,
        //method: IndexDocumentsMethod,
        //format: UpdateFormat,
        //mut stream: impl futures::Stream<Item=Result<B, E>> + Unpin,
        //) -> anyhow::Result<UpdateStatusResponse>
    //where
        //B: Deref<Target = [u8]>,
        //E: std::error::Error + Send + Sync + 'static,
    //{
        //let file = tokio::task::spawn_blocking(tempfile::tempfile).await?;
        //let file = tokio::fs::File::from_std(file?);
        //let mut encoder = GzipEncoder::new(file);

        //while let Some(result) = stream.next().await {
            //let bytes = &*result?;
            //encoder.write_all(&bytes[..]).await?;
        //}

        //encoder.shutdown().await?;
        //let mut file = encoder.into_inner();
        //file.sync_all().await?;
        //let file = file.into_std().await;
        //let mmap = unsafe { memmap::Mmap::map(&file)? };

        //let meta = UpdateMeta::DocumentsAddition { method, format };

        //let index = self.index.clone();
        //let queue = self.update_store.clone();
        //let update = tokio::task::spawn_blocking(move || queue.register_update(index, meta, &mmap[..])).await??;
        //Ok(update.into())
    //}
}

pub struct IndexStore {
    path: PathBuf,
    env: Env,
    indexes_db: Database<Str, SerdeBincode<IndexMetadata>>,
    indexes: DashMap<String, (String, Arc<Index>)>,
}

impl IndexStore {
    /// Open the index controller from meta found at path, and create a new one if no meta is
    /// found.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        // If index controller metadata is present, we return the env, otherwise, we create a new
        // metadata from scratch before returning a new env.
        let path = path.as_ref().to_path_buf();
        let env = match IndexStoreMeta::from_path(&path)? {
            Some(meta) =>  meta.open_options.open(INDEXES_CONTROLLER_FILENAME)?,
            None => {
                let mut open_options = EnvOpenOptions::new();
                open_options.map_size(page_size::get() * 1000);
                let env = open_options.open(INDEXES_CONTROLLER_FILENAME)?;
                let created_at = Utc::now();
                let meta = IndexStoreMeta { open_options: open_options.clone(), created_at };
                meta.to_path(&path)?;
                env
            }
        };
        let indexes = DashMap::new();
        let indexes_db = match env.open_database(Some(INDEXES_DB_NAME))? {
            Some(indexes_db) => indexes_db,
            None => env.create_database(Some(INDEXES_DB_NAME))?,
        };

        Ok(Self { env, indexes, indexes_db, path })
    }

    pub fn get_or_create<S: AsRef<str>>(&self, _name: S) -> Result<IndexView> {
        todo!()
    }

    /// Get an index with read access to the db. The index are lazily loaded, meaning that we first
    /// check for its exixtence in the indexes map, and if it doesn't exist, the index db is check
    /// for metadata to launch the index.
    pub fn get<S: AsRef<str>>(&self, name: S) -> Result<Option<IndexView>> {
        match self.indexes.get(name.as_ref()) {
            Some(entry) => {
                let index = entry.1.clone();
                let uuid = entry.0.clone();
                let view = IndexView::try_new(index, |index| index.read_txn(), uuid)?;
                Ok(Some(view))
            }
            None => {
                let txn = self.env.read_txn()?;
                match self.indexes_db.get(&txn, name.as_ref())? {
                    Some(meta) => {
                        let uuid = meta.uuid.clone();
                        let index = Arc::new(meta.open_index(&self.path)?);
                        self.indexes.insert(name.as_ref().to_owned(), (uuid.clone(), index.clone()));
                        let view = IndexView::try_new(index, |index| index.read_txn(), uuid)?;
                        Ok(Some(view))
                    }
                    None => Ok(None)
                }
            }
        }
    }

    pub fn get_mut<S: AsRef<str>>(&self, _name: S) -> Result<Option<IndexView>> {
        todo!()
    }

    pub async fn delete_index<S: AsRef<str>>(&self, _name:S) -> Result<()> {
        todo!()
    }

    pub async fn list_indices(&self) -> Result<Vec<(String, IndexMetadata)>> {
        todo!()
    }

    pub async fn rename_index(&self, _old: &str, _new: &str) -> Result<()> {
        todo!()
    }
}
