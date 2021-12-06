use enum_iterator::IntoEnumIterator;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs::create_dir_all;
use std::path::Path;
use std::str;

use chrono::{DateTime, Utc};
use heed::types::{ByteSlice, DecodeIgnore, SerdeJson};
use heed::{Database, Env, EnvOpenOptions, RwTxn};

use super::error::Result;
use super::{Action, Key};

const AUTH_STORE_SIZE: usize = 1_073_741_824; //1GiB
pub const KEY_ID_LENGTH: usize = 8;
const AUTH_DB_PATH: &str = "auth";
const KEY_DB_NAME: &str = "api-keys";
const KEY_ID_ACTION_INDEX_EXPIRATION_DB_NAME: &str = "keyid-action-index-expiration";

pub type KeyId = [u8; KEY_ID_LENGTH];

#[derive(Clone)]
pub struct HeedAuthStore {
    env: Env,
    keys: Database<ByteSlice, SerdeJson<Key>>,
    action_keyid_index_expiration: Database<KeyIdActionCodec, SerdeJson<Option<DateTime<Utc>>>>,
}

impl HeedAuthStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().join(AUTH_DB_PATH);
        create_dir_all(&path)?;
        let mut options = EnvOpenOptions::new();
        options.map_size(AUTH_STORE_SIZE); // 1GB
        options.max_dbs(2);
        let env = options.open(path)?;
        let keys = env.create_database(Some(KEY_DB_NAME))?;
        let action_keyid_index_expiration =
            env.create_database(Some(KEY_ID_ACTION_INDEX_EXPIRATION_DB_NAME))?;
        Ok(Self {
            env,
            keys,
            action_keyid_index_expiration,
        })
    }

    pub fn is_empty(&self) -> Result<bool> {
        let rtxn = self.env.read_txn()?;

        Ok(self.keys.len(&rtxn)? == 0)
    }

    pub fn put_api_key(&self, key: Key) -> Result<Key> {
        let mut wtxn = self.env.write_txn()?;
        self.keys.put(&mut wtxn, &key.id, &key)?;

        let id = key.id;
        // delete key from inverted database before refilling it.
        self.delete_key_from_inverted_db(&mut wtxn, &id)?;
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
                db.put(&mut wtxn, &(&id, &action, None), &key.expires_at)?;
            } else {
                // else we create a key for each index.
                for index in key.indexes.iter() {
                    db.put(
                        &mut wtxn,
                        &(&id, &action, Some(index.as_bytes())),
                        &key.expires_at,
                    )?;
                }
            }
        }

        wtxn.commit()?;

        Ok(key)
    }

    pub fn get_api_key(&self, key: impl AsRef<str>) -> Result<Option<Key>> {
        let rtxn = self.env.read_txn()?;
        match try_split_array_at::<_, KEY_ID_LENGTH>(key.as_ref().as_bytes()) {
            Some((id, _)) => self.keys.get(&rtxn, id).map_err(|e| e.into()),
            None => Ok(None),
        }
    }

    pub fn delete_api_key(&self, key: impl AsRef<str>) -> Result<bool> {
        let mut wtxn = self.env.write_txn()?;
        let existing = match try_split_array_at(key.as_ref().as_bytes()) {
            Some((id, _)) => {
                let existing = self.keys.delete(&mut wtxn, id)?;
                self.delete_key_from_inverted_db(&mut wtxn, id)?;
                existing
            }
            None => false,
        };
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
        Ok(list)
    }

    pub fn get_expiration_date(
        &self,
        key: &[u8],
        action: Action,
        index: Option<&[u8]>,
    ) -> Result<Option<(KeyId, Option<DateTime<Utc>>)>> {
        let rtxn = self.env.read_txn()?;
        match try_split_array_at::<_, KEY_ID_LENGTH>(key) {
            Some((id, _)) => {
                let tuple = (id, &action, index);
                Ok(self
                    .action_keyid_index_expiration
                    .get(&rtxn, &tuple)?
                    .map(|expiration| (*id, expiration)))
            }
            None => Ok(None),
        }
    }

    pub fn prefix_first_expiration_date(
        &self,
        key: &[u8],
        action: Action,
    ) -> Result<Option<(KeyId, Option<DateTime<Utc>>)>> {
        let rtxn = self.env.read_txn()?;
        match try_split_array_at::<_, KEY_ID_LENGTH>(key) {
            Some((id, _)) => {
                let tuple = (id, &action, None);
                Ok(self
                    .action_keyid_index_expiration
                    .prefix_iter(&rtxn, &tuple)?
                    .next()
                    .transpose()?
                    .map(|(_, expiration)| (*id, expiration)))
            }
            None => Ok(None),
        }
    }

    fn delete_key_from_inverted_db(&self, wtxn: &mut RwTxn, key: &KeyId) -> Result<()> {
        let mut iter = self
            .action_keyid_index_expiration
            .remap_types::<ByteSlice, DecodeIgnore>()
            .prefix_iter_mut(wtxn, key)?;
        while iter.next().transpose()?.is_some() {
            // safety: we don't keep references from inside the LMDB database.
            unsafe { iter.del_current()? };
        }

        Ok(())
    }
}

/// Codec allowing to retrieve the expiration date of an action,
/// optionnally on a spcific index, for a given key.
pub struct KeyIdActionCodec;

impl<'a> heed::BytesDecode<'a> for KeyIdActionCodec {
    type DItem = (KeyId, Action, Option<&'a [u8]>);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let (key_id, action_bytes) = try_split_array_at(bytes)?;
        let (action_bytes, index) = match try_split_array_at(action_bytes)? {
            (action, []) => (action, None),
            (action, index) => (action, Some(index)),
        };
        let action = Action::from_repr(u8::from_be_bytes(*action_bytes))?;

        Some((*key_id, action, index))
    }
}

impl<'a> heed::BytesEncode<'a> for KeyIdActionCodec {
    type EItem = (&'a KeyId, &'a Action, Option<&'a [u8]>);

    fn bytes_encode((key_id, action, index): &Self::EItem) -> Option<Cow<[u8]>> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(*key_id);
        let action_bytes = u8::to_be_bytes(action.repr());
        bytes.extend_from_slice(&action_bytes);
        if let Some(index) = index {
            bytes.extend_from_slice(index);
        }

        Some(Cow::Owned(bytes))
    }
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
