use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::path::Path;
use std::result::Result as StdResult;
use std::str;

use hmac::{Hmac, Mac};
use meilisearch_types::heed::{BoxedError, WithoutTls};
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::keys::KeyId;
use meilisearch_types::milli::heed;
use meilisearch_types::milli::heed::types::{Bytes, DecodeIgnore, SerdeJson};
use meilisearch_types::milli::heed::{Database, Env, EnvOpenOptions, RwTxn};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::fmt::Hyphenated;
use uuid::Uuid;

use super::error::Result;
use super::{Action, Key};

const AUTH_STORE_SIZE: usize = 1_073_741_824; //1GiB
const KEY_DB_NAME: &str = "api-keys";
const KEY_ACTIONS_DB_NAME: &str = "key-actions";

#[derive(Clone)]
pub struct HeedAuthStore {
    env: Env<WithoutTls>,
    keys: Database<Bytes, SerdeJson<Key>>,
    key_actions: Database<Bytes, SerdeJson<KeyActions>>,
}

pub fn open_auth_store_env(path: &Path) -> heed::Result<Env<WithoutTls>> {
    let options = EnvOpenOptions::new();
    let mut options = options.read_txn_without_tls();
    options.map_size(AUTH_STORE_SIZE); // 1GB
    options.max_dbs(2);
    unsafe { options.open(path) }
}

impl HeedAuthStore {
    pub fn new(env: Env<WithoutTls>) -> Result<Self> {
        let mut wtxn = env.write_txn()?;
        let keys = env.create_database(&mut wtxn, Some(KEY_DB_NAME))?;
        let key_actions = env.create_database(&mut wtxn, Some(KEY_ACTIONS_DB_NAME))?;
        wtxn.commit()?;
        Ok(Self { env, keys, key_actions })
    }

    /// Return `Ok(())` if the auth store is able to access one of its database.
    pub fn health(&self) -> Result<()> {
        let rtxn = self.env.read_txn()?;
        self.keys.first(&rtxn)?;
        Ok(())
    }

    /// Return the size in bytes of database
    pub fn size(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
    }

    /// Return the number of bytes actually used in the database
    pub fn used_size(&self) -> Result<u64> {
        Ok(self.env.non_free_pages_size()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        let rtxn = self.env.read_txn()?;

        Ok(self.keys.len(&rtxn)? == 0)
    }

    pub fn put_api_key(&self, key: Key) -> Result<Key> {
        let uid = key.uid;
        let mut wtxn = self.env.write_txn()?;

        self.keys.put(&mut wtxn, uid.as_bytes(), &key)?;

        // Delete existing actions
        self.delete_key_from_inverted_db(&mut wtxn, &uid)?;

        // Create new actions with bitflags
        let mut actions = HashSet::new();
        for action in &key.actions {
            match action {
                Action::All => {
                    actions.extend(enum_iterator::all::<Action>())
                },
                Action::DocumentsAll => {
                    actions.extend(
                        [Action::DocumentsGet, Action::DocumentsDelete, Action::DocumentsAdd]
                            .iter(),
                    );
                }
                Action::IndexesAll => {
                    actions.extend(
                        [
                            Action::IndexesAdd,
                            Action::IndexesDelete,
                            Action::IndexesGet,
                            Action::IndexesUpdate,
                            Action::IndexesSwap,
                        ]
                        .iter(),
                    );
                }
                Action::SettingsAll => {
                    actions.extend([Action::SettingsGet, Action::SettingsUpdate].iter());
                }
                Action::DumpsAll => {
                    actions.insert(Action::DumpsCreate);
                }
                Action::SnapshotsAll => {
                    actions.insert(Action::SnapshotsCreate);
                }
                Action::TasksAll => {
                    actions.extend([Action::TasksGet, Action::TasksDelete, Action::TasksCancel]);
                }
                Action::StatsAll => {
                    actions.insert(Action::StatsGet);
                }
                Action::MetricsAll => {
                    actions.insert(Action::MetricsGet);
                }
                other => {
                    actions.insert(*other);
                }
            }
        }

        // Store the actions with bitflags
        let key_actions = KeyActions::new(&actions, &key.indexes, key.expires_at);
        self.key_actions.put(&mut wtxn, uid.as_bytes(), &key_actions)?;

        wtxn.commit()?;
        Ok(key)
    }

    pub fn get_api_key(&self, uid: Uuid) -> Result<Option<Key>> {
        let rtxn = self.env.read_txn()?;
        self.keys.get(&rtxn, uid.as_bytes()).map_err(|e| e.into())
    }

    pub fn get_uid_from_encoded_key(
        &self,
        encoded_key: &[u8],
        master_key: &[u8],
    ) -> Result<Option<Uuid>> {
        let rtxn = self.env.read_txn()?;
        let uid = self
            .keys
            .remap_data_type::<DecodeIgnore>()
            .iter(&rtxn)?
            .filter_map(|res| match res {
                Ok((uid, _)) => {
                    let (uid, _) = try_split_array_at(uid)?;
                    let uid = Uuid::from_bytes(*uid);
                    if generate_key_as_hexa(uid, master_key).as_bytes() == encoded_key {
                        Some(uid)
                    } else {
                        None
                    }
                }
                Err(_) => None,
            })
            .next();

        Ok(uid)
    }

    pub fn delete_api_key(&self, uid: Uuid) -> Result<bool> {
        let mut wtxn = self.env.write_txn()?;
        let existing = self.keys.delete(&mut wtxn, uid.as_bytes())?;
        self.delete_key_from_inverted_db(&mut wtxn, &uid)?;
        wtxn.commit()?;

        Ok(existing)
    }

    pub fn delete_all_keys(&self) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.keys.clear(&mut wtxn)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn list_api_keys(&self) -> Result<Vec<Key>> {
        let mut list = Vec::new();
        let rtxn = self.env.read_txn()?;
        for result in self.keys.remap_key_type::<DecodeIgnore>().iter(&rtxn)? {
            let (_, content) = result?;
            list.push(content);
        }
        list.sort_unstable_by_key(|k| Reverse(k.created_at));
        Ok(list)
    }

    pub fn get_expiration_date(
        &self,
        uid: Uuid,
        action: Action,
        index: Option<&str>,
    ) -> Result<Option<Option<OffsetDateTime>>> {
        let rtxn = self.env.read_txn()?;
        
        // Get the key actions
        if let Some(key_actions) = self.key_actions.get(&rtxn, uid.as_bytes())? {
            // Check if the action is allowed
            if !key_actions.has_action(action) {
                return Ok(None);
            }

            // Check index restrictions
            let no_index_restriction = key_actions.indexes.iter().any(|p| p.matches_all());
            if no_index_restriction {
                return Ok(Some(key_actions.expires_at));
            }

            // Check specific index
            if let Some(index) = index {
                for pattern in &key_actions.indexes {
                    if pattern.matches_str(index) {
                        return Ok(Some(key_actions.expires_at));
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn prefix_first_expiration_date(
        &self,
        uid: Uuid,
        action: Action,
    ) -> Result<Option<Option<OffsetDateTime>>> {
        let rtxn = self.env.read_txn()?;
        
        if let Some(key_actions) = self.key_actions.get(&rtxn, uid.as_bytes())? {
            if key_actions.has_action(action) {
                return Ok(Some(key_actions.expires_at));
            }
        }
        
        Ok(None)
    }

    fn delete_key_from_inverted_db(&self, wtxn: &mut RwTxn, key: &KeyId) -> Result<()> {
        self.key_actions.delete(wtxn, key.as_bytes())?;
        Ok(())
    }
}

/// Codec allowing to retrieve the expiration date of an action,
/// optionally on a specific index, for a given key.
pub struct KeyIdActionCodec;

impl<'a> heed::BytesDecode<'a> for KeyIdActionCodec {
    type DItem = (KeyId, Action, Option<&'a [u8]>);

    fn bytes_decode(bytes: &'a [u8]) -> StdResult<Self::DItem, BoxedError> {
        let (key_id_bytes, action_bytes) = try_split_array_at(bytes).ok_or(SliceTooShortError)?;
        let (&action_byte, index) =
            match try_split_array_at(action_bytes).ok_or(SliceTooShortError)? {
                ([action], []) => (action, None),
                ([action], index) => (action, Some(index)),
            };
        let key_id = Uuid::from_bytes(*key_id_bytes);
        let action = Action::from_repr(action_byte).ok_or(InvalidActionError { action_byte })?;

        Ok((key_id, action, index))
    }
}

impl<'a> heed::BytesEncode<'a> for KeyIdActionCodec {
    type EItem = (&'a KeyId, &'a Action, Option<&'a [u8]>);

    fn bytes_encode((key_id, action, index): &Self::EItem) -> StdResult<Cow<[u8]>, BoxedError> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(key_id.as_bytes());
        let action_bytes = u8::to_be_bytes(action.repr());
        bytes.extend_from_slice(&action_bytes);
        if let Some(index) = index {
            bytes.extend_from_slice(index);
        }

        Ok(Cow::Owned(bytes))
    }
}

#[derive(Error, Debug)]
#[error("the slice is too short")]
pub struct SliceTooShortError;

#[derive(Error, Debug)]
#[error("cannot construct a valid Action from {action_byte}")]
pub struct InvalidActionError {
    pub action_byte: u8,
}

pub fn generate_key_as_hexa(uid: Uuid, master_key: &[u8]) -> String {
    // format uid as hyphenated allowing user to generate their own keys.
    let mut uid_buffer = [0; Hyphenated::LENGTH];
    let uid = uid.hyphenated().encode_lower(&mut uid_buffer);

    // new_from_slice function never fail.
    let mut mac = Hmac::<Sha256>::new_from_slice(master_key).unwrap();
    mac.update(uid.as_bytes());

    let result = mac.finalize();
    format!("{:x}", result.into_bytes())
}

/// Divides one slice into two at an index, returns `None` if mid is out of bounds.
pub fn try_split_at<T>(slice: &[T], mid: usize) -> Option<(&[T], &[T])> {
    if mid <= slice.len() {
        Some(slice.split_at(mid))
    } else {
        None
    }
}

/// Divides one slice into an array and the tail at an index,
/// returns `None` if `N` is out of bounds.
pub fn try_split_array_at<T, const N: usize>(slice: &[T]) -> Option<(&[T; N], &[T])>
where
    [T; N]: for<'a> TryFrom<&'a [T]>,
{
    let (head, tail) = try_split_at(slice, N)?;
    let head = head.try_into().ok()?;
    Some((head, tail))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeyActions {
    actions: u64, // Bitflags for actions
    indexes: Vec<IndexUidPattern>,
    expires_at: Option<OffsetDateTime>,
}

impl KeyActions {
    fn new(actions: &HashSet<Action>, indexes: &[IndexUidPattern], expires_at: Option<OffsetDateTime>) -> Self {
        let mut bitflags = 0u64;
        for action in actions {
            bitflags |= 1 << (*action as u8);
        }
        Self {
            actions: bitflags,
            indexes: indexes.to_vec(),
            expires_at,
        }
    }

    fn has_action(&self, action: Action) -> bool {
        let has = self.actions & (1 << (action as u8)) != 0;
        has
    }
}
