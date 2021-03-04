use std::collections::{HashMap, hash_map::Entry};
use std::fs::{File, create_dir_all};
use std::path::{PathBuf, Path};
use std::sync::Arc;
use std::time::Instant;

use anyhow::bail;
use either::Either;
use async_stream::stream;
use chrono::Utc;
use futures::stream::StreamExt;
use heed::{EnvOpenOptions, RoTxn};
use log::info;
use milli::{FacetCondition, Index};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

use super::update_handler::UpdateHandler;
use crate::data::{SearchQuery, SearchResult};
use crate::index_controller::{IndexMetadata, UpdateMeta, updates::{Processed, Failed, Processing}, UpdateResult as UResult};
use crate::option::IndexerOpts;

pub type Result<T> = std::result::Result<T, IndexError>;
type AsyncMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type UpdateResult = std::result::Result<Processed<UpdateMeta, UResult>, Failed<UpdateMeta, String>>;

enum IndexMsg {
    CreateIndex { uuid: Uuid, primary_key: Option<String>, ret: oneshot::Sender<Result<IndexMetadata>> },
    Update { meta: Processing<UpdateMeta>, data: std::fs::File, ret: oneshot::Sender<UpdateResult>},
    Search { uuid: Uuid, query: SearchQuery, ret: oneshot::Sender<anyhow::Result<SearchResult>> },
}

struct IndexActor<S> {
    inbox: Option<mpsc::Receiver<IndexMsg>>,
    update_handler: Arc<UpdateHandler>,
    store: S,
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("error with index: {0}")]
    Error(#[from] anyhow::Error),
    #[error("index already exists")]
    IndexAlreadyExists,
}

#[async_trait::async_trait]
trait IndexStore {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMetadata>;
    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<Index>>;
    async fn get(&self, uuid: Uuid) -> Result<Option<Arc<Index>>>;
}

impl<S: IndexStore + Sync + Send> IndexActor<S> {
    fn new(inbox: mpsc::Receiver<IndexMsg>, store: S) -> Self {
        let options = IndexerOpts::default();
        let update_handler = UpdateHandler::new(&options).unwrap();
        let update_handler = Arc::new(update_handler);
        let inbox = Some(inbox);
        Self { inbox, store, update_handler }
    }

    async fn run(mut self) {
        let mut inbox = self.inbox.take().expect("Index Actor must have a inbox at this point.");

        let stream = stream! {
            loop {
                match inbox.recv().await {
                    Some(msg) => yield msg,
                    None => break,
                }
            }
        };

        let fut = stream.for_each_concurrent(Some(10), |msg| async {
            match msg {
                IndexMsg::CreateIndex { uuid, primary_key, ret } => self.handle_create_index(uuid, primary_key, ret).await,
                IndexMsg::Update { ret, meta, data } => self.handle_update(meta, data, ret).await,
                IndexMsg::Search { ret, query, uuid } => self.handle_search(uuid, query, ret).await,
            }
        });

        fut.await;
    }

    async fn handle_search(&self, uuid: Uuid, query: SearchQuery, ret: oneshot::Sender<anyhow::Result<SearchResult>>) {
        let index = self.store.get(uuid).await.unwrap().unwrap();
        tokio::task::spawn_blocking(move || {
            let result = perform_search(&index, query);
            ret.send(result)
        });

    }

    async fn handle_create_index(&self, uuid: Uuid, primary_key: Option<String>, ret: oneshot::Sender<Result<IndexMetadata>>) {
        let result = self.store.create_index(uuid, primary_key).await;
        let _ = ret.send(result);
    }

    async fn handle_update(&self, meta: Processing<UpdateMeta>, data: File, ret: oneshot::Sender<UpdateResult>) {
        info!("Processing update {}", meta.id());
        let uuid = meta.index_uuid().clone();
        let index = self.store.get_or_create(uuid).await.unwrap();
        let update_handler = self.update_handler.clone();
        let result = tokio::task::spawn_blocking(move || update_handler.handle_update(meta, data, index.as_ref())).await;
        let result = result.unwrap();
        let _ = ret.send(result);
    }
}

fn perform_search(index: &Index, query: SearchQuery) -> anyhow::Result<SearchResult> {
    let before_search = Instant::now();
    let rtxn = index.read_txn()?;

    let mut search = index.search(&rtxn);

    if let Some(ref query) = query.q {
        search.query(query);
    }

    search.limit(query.limit);
    search.offset(query.offset.unwrap_or_default());

    if let Some(ref facets) = query.facet_filters {
        if let Some(facets) = parse_facets(facets, index, &rtxn)? {
            search.facet_condition(facets);
        }
    }

    let milli::SearchResult {
        documents_ids,
        found_words,
        candidates,
        ..
    } = search.execute()?;
    let mut documents = Vec::new();
    let fields_ids_map = index.fields_ids_map(&rtxn).unwrap();

    let displayed_fields_ids = index.displayed_fields_ids(&rtxn).unwrap();

    let attributes_to_retrieve_ids = match query.attributes_to_retrieve {
        Some(ref attrs) if attrs.iter().any(|f| f == "*") => None,
        Some(ref attrs) => attrs
            .iter()
            .filter_map(|f| fields_ids_map.id(f))
            .collect::<Vec<_>>()
            .into(),
        None => None,
    };

    let displayed_fields_ids = match (displayed_fields_ids, attributes_to_retrieve_ids) {
        (_, Some(ids)) => ids,
        (Some(ids), None) => ids,
        (None, None) => fields_ids_map.iter().map(|(id, _)| id).collect(),
    };

    let stop_words = fst::Set::default();
    let highlighter = crate::data::search::Highlighter::new(&stop_words);

    for (_id, obkv) in index.documents(&rtxn, documents_ids)? {
        let mut object = milli::obkv_to_json(&displayed_fields_ids, &fields_ids_map, obkv).unwrap();
        if let Some(ref attributes_to_highlight) = query.attributes_to_highlight {
            highlighter.highlight_record(&mut object, &found_words, attributes_to_highlight);
        }
        documents.push(object);
    }

    let nb_hits = candidates.len();

    let facet_distributions = match query.facet_distributions {
        Some(ref fields) => {
            let mut facet_distribution = index.facets_distribution(&rtxn);
            if fields.iter().all(|f| f != "*") {
                facet_distribution.facets(fields);
            }
            Some(facet_distribution.candidates(candidates).execute()?)
        }
        None => None,
    };

    let result = SearchResult {
        hits: documents,
        nb_hits,
        query: query.q.clone().unwrap_or_default(),
        limit: query.limit,
        offset: query.offset.unwrap_or_default(),
        processing_time_ms: before_search.elapsed().as_millis(),
        facet_distributions,
    };
    Ok(result)
}

fn parse_facets_array(
    txn: &RoTxn,
    index: &Index,
    arr: &Vec<Value>,
) -> anyhow::Result<Option<FacetCondition>> {
    let mut ands = Vec::new();
    for value in arr {
        match value {
            Value::String(s) => ands.push(Either::Right(s.clone())),
            Value::Array(arr) => {
                let mut ors = Vec::new();
                for value in arr {
                    match value {
                        Value::String(s) => ors.push(s.clone()),
                        v => bail!("Invalid facet expression, expected String, found: {:?}", v),
                    }
                }
                ands.push(Either::Left(ors));
            }
            v => bail!(
                "Invalid facet expression, expected String or [String], found: {:?}",
                v
            ),
        }
    }

    FacetCondition::from_array(txn, index, ands)
}

fn parse_facets(
    facets: &Value,
    index: &Index,
    txn: &RoTxn,
) -> anyhow::Result<Option<FacetCondition>> {
    match facets {
        // Disabled for now
        //Value::String(expr) => Ok(Some(FacetCondition::from_str(txn, index, expr)?)),
        Value::Array(arr) => parse_facets_array(txn, index, arr),
        v => bail!(
            "Invalid facet expression, expected Array, found: {:?}",
            v
        ),
    }
}
#[derive(Clone)]
pub struct IndexActorHandle {
    sender: mpsc::Sender<IndexMsg>,
}

impl IndexActorHandle {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let (sender, receiver) = mpsc::channel(100);

        let store = MapIndexStore::new(path);
        let actor = IndexActor::new(receiver, store);
        tokio::task::spawn(actor.run());
        Self { sender }
    }

    pub async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMetadata> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::CreateIndex { ret, uuid, primary_key };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }

    pub async fn update(&self, meta: Processing<UpdateMeta>, data: std::fs::File) -> UpdateResult {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Update { ret, meta, data };
        let _ = self.sender.send(msg).await;
        receiver.await.expect("IndexActor has been killed")
    }

    pub async fn search(&self, uuid: Uuid, query: SearchQuery) -> Result<SearchResult> {
        let (ret, receiver) = oneshot::channel();
        let msg = IndexMsg::Search { uuid, query, ret };
        let _ = self.sender.send(msg).await;
        Ok(receiver.await.expect("IndexActor has been killed")?)
    }
}

struct MapIndexStore {
    root: PathBuf,
    meta_store: AsyncMap<Uuid, IndexMetadata>,
    index_store: AsyncMap<Uuid, Arc<Index>>,
}

#[async_trait::async_trait]
impl IndexStore for MapIndexStore {
    async fn create_index(&self, uuid: Uuid, primary_key: Option<String>) -> Result<IndexMetadata> {
        let meta = match self.meta_store.write().await.entry(uuid.clone()) {
            Entry::Vacant(entry) => {
                let meta = IndexMetadata {
                    uuid,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    primary_key,
                };
                entry.insert(meta).clone()
            }
            Entry::Occupied(_) => return Err(IndexError::IndexAlreadyExists),
        };

        let db_path = self.root.join(format!("index-{}", meta.uuid));

        let index: Result<Index> = tokio::task::spawn_blocking(move || {
            create_dir_all(&db_path).expect("can't create db");
            let mut options = EnvOpenOptions::new();
            options.map_size(4096 * 100_000);
            let index = Index::new(options, &db_path)
                .map_err(|e| IndexError::Error(e))?;
            Ok(index)
        }).await.expect("thread died");

        self.index_store.write().await.insert(meta.uuid.clone(), Arc::new(index?));

        Ok(meta)
    }

    async fn get_or_create(&self, uuid: Uuid) -> Result<Arc<Index>> {
        match self.index_store.write().await.entry(uuid.clone()) {
            Entry::Vacant(entry) => {
                match self.meta_store.write().await.entry(uuid.clone()) {
                    Entry::Vacant(_) => {
                        todo!()
                    }
                    Entry::Occupied(entry) => {
                        todo!()
                    }
                }
            }
            Entry::Occupied(entry) => Ok(entry.get().clone()),
        }
    }

    async fn get(&self, uuid: Uuid) -> Result<Option<Arc<Index>>> {
        Ok(self.index_store.read().await.get(&uuid).cloned())
    }
}

impl MapIndexStore {
    fn new(root: impl AsRef<Path>) -> Self {
        let mut root = root.as_ref().to_owned();
        root.push("indexes/");
        let meta_store = Arc::new(RwLock::new(HashMap::new()));
        let index_store = Arc::new(RwLock::new(HashMap::new()));
        Self { meta_store, index_store, root }
    }
}
