use crate::data::{Data, IndexCreateRequest, IndexResponse};
use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState, RaftStorage};
use async_raft::{AppData, AppDataResponse, NodeId};
use heed::types::{OwnedType, Str};
use heed::{Database, Env, PolyDatabase};
use meilisearch_core::settings::Settings;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

const ERR_INCONSISTENT_LOG: &str =
    "a query was received which was expecting data to be in place which does not exist in the log";

const MEMBERSHIP_CONFIG_KEY: &str = "membership";
const HARD_STATE_KEY: &str = "hard_state";
const LAST_APPLIED_KEY: &str = "last_commited";
const SNAPSHOT_PATH_KEY: &str = "snapshot_path";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    CreateIndex(IndexCreateRequest),
    SettingChange(Settings),
    DocumentAddition {
        index_uid: String,
        addition: PathBuf,
        partial: bool,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    pub serial: u64,
    /// A string describing the status of the client. For a real application, this should probably
    /// be an enum representing all of the various types of requests / operations which a client
    /// can perform.
    pub message: Message,
}
///
/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponse {
    IndexCreation(std::result::Result<IndexResponse, String>),
}

impl AppDataResponse for ClientResponse {}

/// Error data response.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientError {
    /// This request has already been applied to the state machine, and the original response
    /// no longer exists.
    OldRequestReplayed,
}

impl AppData for ClientRequest {}

macro_rules! derive_bytes {
    ($type:ty, $name:ident) => {
        struct $name;

        impl<'a> heed::BytesDecode<'a> for $name {
            type DItem = $type;

            fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
                bincode::deserialize(bytes).ok()
            }
        }

        impl<'a> heed::BytesEncode<'a> for $name {
            type EItem = $type;

            fn bytes_encode(item: &Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
                let bytes = bincode::serialize(item).ok()?;
                Some(std::borrow::Cow::Owned(bytes))
            }
        }
    };
}

derive_bytes!(MembershipConfig, HeedMembershipConfig);
derive_bytes!(HardState, HeedHardState);
derive_bytes!(Entry<ClientRequest>, HeedEntry);

struct RaftStore {
    id: NodeId,
    db: PolyDatabase,
    logs: Database<OwnedType<u64>, HeedEntry>,
    env: Env,
    store: Arc<Data>,
    snapshot_dir: PathBuf,
    current_snapshot: RwLock<Option<tokio::fs::File>>,
}

struct RaftSnapshot {
    file: tokio::fs::File,
    index: u64,
    term: u64,
    membership: MembershipConfig,
}

impl AsyncRead for RaftSnapshot

impl RaftStore {
    fn hard_state(&self, txn: &heed::RoTxn) -> Result<Option<HardState>> {
        Ok(self.db.get::<_, Str, HeedHardState>(txn, HARD_STATE_KEY)?)
    }

    fn set_hard_state(&self, txn: &mut heed::RwTxn, hs: &HardState) -> Result<()> {
        Ok(self
            .db
            .put::<_, Str, HeedHardState>(txn, HARD_STATE_KEY, hs)?)
    }

    fn last_applied_log(&self, txn: &heed::RoTxn) -> Result<Option<u64>> {
        Ok(self
            .db
            .get::<_, Str, OwnedType<u64>>(txn, LAST_APPLIED_KEY)?)
    }

    fn set_last_applied_log(&self, txn: &mut heed::RwTxn, last_applied: u64) -> Result<()> {
        Ok(self
            .db
            .put::<_, Str, OwnedType<u64>>(txn, LAST_APPLIED_KEY, &last_applied)?)
    }

    fn membership_config(&self, txn: &heed::RoTxn) -> Result<Option<MembershipConfig>> {
        Ok(self
            .db
            .get::<_, Str, HeedMembershipConfig>(txn, MEMBERSHIP_CONFIG_KEY)?)
    }

    fn set_membership_config(&self, txn: &mut heed::RwTxn, cfg: &MembershipConfig) -> Result<()> {
        Ok(self
            .db
            .put::<_, Str, HeedMembershipConfig>(txn, MEMBERSHIP_CONFIG_KEY, cfg)?)
    }

    fn snapshot_id<'a>(&self, txn: &'a heed::RoTxn) -> Result<Option<&'a str>> {
        Ok(self.db.get::<_, Str, Str>(txn, SNAPSHOT_PATH_KEY)?)
    }

    fn snapshot_id_owned(&self) -> Result<Option<String>> {
        let txn = self.env.read_txn()?;
        Ok(self
            .db
            .get::<_, Str, Str>(&txn, SNAPSHOT_PATH_KEY)?
            .map(str::to_string))
    }

    fn set_snapshot_id(&self, txn: &mut heed::RwTxn, id: &str) -> Result<()> {
        Ok(self.db.put::<_, Str, Str>(txn, SNAPSHOT_PATH_KEY, id)?)
    }

    fn put_log(
        &self,
        txn: &mut heed::RwTxn,
        index: u64,
        entry: &Entry<ClientRequest>,
    ) -> Result<()> {
        // keep track of the latest membership config
        match entry.payload {
            EntryPayload::ConfigChange(ref cfg) => {
                self.set_membership_config(txn, &cfg.membership)?
            }
            _ => (),
        }
        self.logs.put(txn, &index, entry)?;
        Ok(())
    }

    fn apply_message(&self, message: &Message) -> ClientResponse {
        match message {
            Message::CreateIndex(ref index_info) => {
                let result = self
                    .store
                    .create_index(index_info)
                    .map_err(|e| e.to_string());
                ClientResponse::IndexCreation(result)
            }
            _ => todo!(),
        }
    }

    fn snapshot_path_from_id(&self, id: &str) -> PathBuf {
        self.snapshot_dir.join(format!("{}.snap", id))
    }

    fn create_snapshot_and_compact(
        &self,
        through: u64,
    ) -> Result<(u64, PathBuf, MembershipConfig)> {
        let mut txn = self.env.write_txn()?;

        // 1. get term
        let term = self
            .logs
            .get(&txn, &through)?
            .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?
            .term;
        // 2. snapshot_id is term-index
        let snapshot_id = format!("{}-{}", term, through);

        // 3. get current membership config
        let membership_config = self
            .membership_config(&txn)?
            .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

        // 4. create snapshot (_ means that the snapshot is not yet ready)
        let snapshot_path_temp = self.snapshot_dir.join("temp.snap");
        crate::snapshot::create_snapshot(&self.store, &snapshot_path_temp)?;
        // snapshot is finished, rename it:
        let snapshot_path = self.snapshot_path_from_id(&snapshot_id);
        std::fs::rename(snapshot_path_temp, snapshot_path.clone())?;

        // 5. compact logs
        self.logs.delete_range(&mut txn, &(0..=through))?;

        // 6. insert new snapshot entry
        let entry =
            Entry::new_snapshot_pointer(through, term, snapshot_id, membership_config.clone());
        self.put_log(&mut txn, through, &entry)?;

        // 7. set snapshot path, for later retrieve
        self.set_snapshot_id(&mut txn, &snapshot_id)?;

        txn.commit()?;
        Ok((term, snapshot_path, membership_config))
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for RaftStore {
    type Snapshot = tokio::fs::File;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let txn = self.env.read_txn()?;
        Ok(self
            .membership_config(&txn)?
            .expect("expected membership config"))
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut txn = self.env.write_txn()?;
        let hs = self.hard_state(&txn)?;
        let last_applied_log = self.last_applied_log(&txn)?.unwrap_or_default();
        let state = match hs {
            Some(inner) => {
                let last_entry = self.logs.last(&txn)?;
                let (last_log_index, last_log_term) = match last_entry {
                    Some((_, entry)) => (entry.index, entry.term),
                    None => (0, 0),
                };
                InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                }
            }
            None => {
                let new = InitialState::new_initial(self.id);
                self.set_hard_state(&mut txn, &new.hard_state)?;
                new
            }
        };
        txn.commit()?;
        Ok(state)
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        self.set_hard_state(&mut txn, hs)?;
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        let txn = self.env.read_txn()?;
        let entries = self
            .logs
            .range(&txn, &(start..stop))?
            .filter_map(|e| e.ok().map(|(_, e)| e))
            .collect();
        Ok(entries)
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        match stop {
            Some(stop) => self.logs.delete_range(&mut txn, &(start..stop))?,
            None => self.logs.delete_range(&mut txn, &(start..))?,
        };
        txn.commit()?;
        Ok(())
    }

    async fn append_entry_to_log(
        &self,
        entry: &async_raft::raft::Entry<ClientRequest>,
    ) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        let index = entry.index;
        self.put_log(&mut txn, index, &entry)?;
        txn.commit()?;
        Ok(())
    }

    async fn replicate_to_log(
        &self,
        entries: &[async_raft::raft::Entry<ClientRequest>],
    ) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        for entry in entries {
            let index = entry.index;
            self.put_log(&mut txn, index, &entry)?;
        }
        txn.commit()?;
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
        let mut txn = self.env.write_txn()?;
        let last_applied_log = *index;
        self.set_last_applied_log(&mut txn, last_applied_log)?;
        let response = self.apply_message(&data.message);
        txn.commit()?;
        Ok(response)
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        let mut last_applied_log = self.last_applied_log(&txn)?.unwrap_or_default();
        for (index, request) in entries {
            last_applied_log = **index;
            self.apply_message(&request.message);
        }
        self.set_last_applied_log(&mut txn, last_applied_log)?;
        txn.commit()?;
        Ok(())
    }

    async fn do_log_compaction(&self, through: u64) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        // it is necessary to do all the heed transation in a standalone function because heed
        // transations are not thread safe.
        let (term, snapshot_path, membership_config) = self.create_snapshot_and_compact(through)?;
        let snapshot_file = tokio::fs::File::open(snapshot_path).await?;

        Ok(CurrentSnapshotData {
            term,
            index: through,
            membership: membership_config.clone(),
            snapshot: Box::new(snapshot_file),
        })
    }

    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        let id = self.snapshot_id_owned()?.unwrap_or_default();
        let path = self.snapshot_path_from_id(&id);
        let file = tokio::fs::File::create(path).await?;
        Ok((id, Box::new(file)))
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        _snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        let mut txn = self.env.write_txn()?;
        match delete_through {
            Some(index) => {
                self.logs.delete_range(&mut txn, &(0..index))?;
            }
            None => self.logs.clear(&mut txn)?,
        }
        let membership_config = self
            .membership_config(&txn)?
            .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        let entry = Entry::new_snapshot_pointer(index, term, id, membership_config);
        self.put_log(&mut txn, index, &entry)?;
        txn.commit()?;

        //TODO:
        // I can't find a way at the moment to apply the snapshot,
        // maybe clear all the dbs, and clone it from the downloaded db? IDK
        Ok(())
    }

    async fn get_current_snapshot(
        &self,
    ) -> Result<Option<async_raft::storage::CurrentSnapshotData<Self::Snapshot>>> {
        todo!()
    }
}
