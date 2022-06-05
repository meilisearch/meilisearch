use std::borrow::Cow;
use std::cmp::Reverse;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::create_dir_all;
use std::path::Path;
use std::str;
use std::sync::Arc;

use enum_iterator::IntoEnumIterator;
use milli::heed::types::{ByteSlice, DecodeIgnore, SerdeJson};
use milli::heed::{Database, Env, EnvOpenOptions, RwTxn};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

use super::error::Result;
use super::{Action, Key};

const AUTH_STORE_SIZE: usize = 1_073_741_824; //1GiB
const AUTH_DB_PATH: &str = "auth";
const KEY_DB_NAME: &str = "api-keys";
const KEY_ID_ACTION_INDEX_EXPIRATION_DB_NAME: &str = "keyid-action-index-expiration";

pub type KeyId = Uuid;

#[derive(Clone)]
pub struct HeedAuthStore {
    env: Arc<Env>,
    keys: Database<ByteSlice, SerdeJson<Key>>,
    action_keyid_index_expiration: Database<KeyIdActionCodec, SerdeJson<Option<OffsetDateTime>>>,
    should_close_on_drop: bool,
}

impl Drop for HeedAuthStore {
    fn drop(&mut self) {
        if self.should_close_on_drop && Arc::strong_count(&self.env) == 1 {
            self.env.as_ref().clone().prepare_for_closing();
        }
    }
}

pub fn open_auth_store_env(path: &Path) -> milli::heed::Result<milli::heed::Env> {
    let mut options = EnvOpenOptions::new();
    options.map_size(AUTH_STORE_SIZE); // 1GB
    options.max_dbs(2);
    options.open(path)
}

impl HeedAuthStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().join(AUTH_DB_PATH);
        create_dir_all(&path)?;
        let env = Arc::new(open_auth_store_env(path.as_ref())?);
        let keys = env.create_database(Some(KEY_DB_NAME))?;
        let action_keyid_index_expiration =
            env.create_database(Some(KEY_ID_ACTION_INDEX_EXPIRATION_DB_NAME))?;
        Ok(Self {
            env,
            keys,
            action_keyid_index_expiration,
            should_close_on_drop: true,
        })
    }

    pub fn set_drop_on_close(&mut self, v: bool) {
        self.should_close_on_drop = v;
    }

    pub fn is_empty(&self) -> Result<bool> {
        let rtxn = self.env.read_txn()?;

        Ok(self.keys.len(&rtxn)? == 0)
    }

    pub fn put_api_key(&self, key: Key) -> Result<Key> {
        let uid = key.uid;
        let mut wtxn = self.env.write_txn()?;

        self.keys.put(&mut wtxn, uid.as_bytes(), &key)?;

        // delete key from inverted database before refilling it.
        self.delete_key_from_inverted_db(&mut wtxn, &uid)?;
        // create inverted database.
        let db = self.action_keyid_index_expiration;

        let actions = if key.actions.contains(&Action::All) {
            // if key.actions contains All, we iterate over all actions.
            Action::into_enum_iter().collect()
        } else {
            key.actions.clone()
        };

        let no_index_restriction = key.indexes.contains(&"*".to_owned());
        for action in actions {
            if no_index_restriction {
                // If there is no index restriction we put None.
                db.put(&mut wtxn, &(&uid, &action, None), &key.expires_at)?;
            } else {
                // else we create a key for each index.
                for index in key.indexes.iter() {
                    db.put(
                        &mut wtxn,
                        &(&uid, &action, Some(index.as_bytes())),
                        &key.expires_at,
                    )?;
                }
            }
        }

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
                Ok((uid, _))
                    if generate_key_as_base64(uid, master_key).as_bytes() == encoded_key =>
                {
                    let (uid, _) = try_split_array_at(uid)?;
                    Some(Uuid::from_bytes(*uid))
                }
                _ => None,
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
        index: Option<&[u8]>,
    ) -> Result<Option<Option<OffsetDateTime>>> {
        let rtxn = self.env.read_txn()?;
        let tuple = (&uid, &action, index);
        Ok(self.action_keyid_index_expiration.get(&rtxn, &tuple)?)
    }

    pub fn prefix_first_expiration_date(
        &self,
        uid: Uuid,
        action: Action,
    ) -> Result<Option<Option<OffsetDateTime>>> {
        let rtxn = self.env.read_txn()?;
        let tuple = (&uid, &action, None);
        let exp = self
            .action_keyid_index_expiration
            .prefix_iter(&rtxn, &tuple)?
            .next()
            .transpose()?
            .map(|(_, expiration)| expiration);

        Ok(exp)
    }

    fn delete_key_from_inverted_db(&self, wtxn: &mut RwTxn, key: &KeyId) -> Result<()> {
        let mut iter = self
            .action_keyid_index_expiration
            .remap_types::<ByteSlice, DecodeIgnore>()
            .prefix_iter_mut(wtxn, key.as_bytes())?;
        while iter.next().transpose()?.is_some() {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.del_current()? };
        }

        Ok(())
    }
}

/// Codec allowing to retrieve the expiration date of an action,
/// optionally on a specific index, for a given key.
pub struct KeyIdActionCodec;

impl<'a> milli::heed::BytesDecode<'a> for KeyIdActionCodec {
    type DItem = (KeyId, Action, Option<&'a [u8]>);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (key_id_bytes, action_bytes) = try_split_array_at(bytes)?;
        let (action_bytes, index) = match try_split_array_at(action_bytes)? {
            (action, []) => (action, None),
            (action, index) => (action, Some(index)),
        };
        let key_id = Uuid::from_bytes(*key_id_bytes);
        let action = Action::from_repr(u8::from_be_bytes(*action_bytes))?;

        Some((key_id, action, index))
    }
}

impl<'a> milli::heed::BytesEncode<'a> for KeyIdActionCodec {
    type EItem = (&'a KeyId, &'a Action, Option<&'a [u8]>);

    fn bytes_encode((key_id, action, index): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(key_id.as_bytes());
        let action_bytes = u8::to_be_bytes(action.repr());
        bytes.extend_from_slice(&action_bytes);
        if let Some(index) = index {
            bytes.extend_from_slice(index);
        }

        Some(Cow::Owned(bytes))
    }
}

pub fn generate_key_as_base64(uid: &[u8], master_key: &[u8]) -> String {
    let key = [uid, master_key].concat();
    let sha = Sha256::digest(&key);
    base64::encode_config(sha, base64::URL_SAFE_NO_PAD)
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
