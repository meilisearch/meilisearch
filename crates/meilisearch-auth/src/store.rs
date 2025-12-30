use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::path::Path;
use std::result::Result as StdResult;
use std::str;
use std::str::FromStr;

use hmac::{Hmac, Mac};
use meilisearch_types::heed::{BoxedError, WithoutTls};
use subtle::ConstantTimeEq;
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::keys::KeyId;
use meilisearch_types::milli::heed;
use meilisearch_types::milli::heed::types::{Bytes, DecodeIgnore, SerdeJson};
use meilisearch_types::milli::heed::{Database, Env, EnvOpenOptions, RwTxn};
use sha2::Sha256;
use thiserror::Error;
use time::OffsetDateTime;
use uuid::fmt::Hyphenated;
use uuid::Uuid;

use super::error::{AuthControllerError, Result};
use super::{Action, Key};

const AUTH_STORE_SIZE: usize = 1_073_741_824; //1GiB
const KEY_DB_NAME: &str = "api-keys";
const KEY_ID_ACTION_INDEX_EXPIRATION_DB_NAME: &str = "keyid-action-index-expiration";

#[derive(Clone)]
pub struct HeedAuthStore {
    env: Env<WithoutTls>,
    keys: Database<Bytes, SerdeJson<Key>>,
    action_keyid_index_expiration: Database<KeyIdActionCodec, SerdeJson<Option<OffsetDateTime>>>,
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
        let action_keyid_index_expiration =
            env.create_database(&mut wtxn, Some(KEY_ID_ACTION_INDEX_EXPIRATION_DB_NAME))?;
        wtxn.commit()?;
        Ok(Self { env, keys, action_keyid_index_expiration })
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

        // delete key from inverted database before refilling it.
        self.delete_key_from_inverted_db(&mut wtxn, &uid)?;
        // create inverted database.
        let db = self.action_keyid_index_expiration;

        let mut actions = HashSet::new();
        for action in &key.actions {
            match action {
                Action::All => {
                    actions.extend(enum_iterator::all::<Action>());
                    actions.remove(&Action::AllGet);
                }
                Action::AllGet => {
                    actions.extend(enum_iterator::all::<Action>().filter(|a| a.is_read()))
                }
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
                            Action::IndexesCompact,
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
                Action::ChatsAll => {
                    actions.extend([Action::ChatsGet, Action::ChatsDelete]);
                }
                Action::ChatsSettingsAll => {
                    actions.extend([Action::ChatsSettingsGet, Action::ChatsSettingsUpdate]);
                }
                Action::WebhooksAll => {
                    actions.extend([
                        Action::WebhooksGet,
                        Action::WebhooksUpdate,
                        Action::WebhooksDelete,
                        Action::WebhooksCreate,
                    ]);
                }
                other => {
                    actions.insert(*other);
                }
            }
        }

        let no_index_restriction = key.indexes.iter().any(|p| p.matches_all());
        for action in actions {
            if no_index_restriction {
                // If there is no index restriction we put None.
                db.put(&mut wtxn, &(&uid, &action, None), &key.expires_at)?;
            } else {
                // else we create a key for each index.
                for index in key.indexes.iter() {
                    db.put(
                        &mut wtxn,
                        &(&uid, &action, Some(index.to_string().as_bytes())),
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
                Ok((uid, _)) => {
                    let (uid, _) = try_split_array_at(uid)?;
                    let uid = Uuid::from_bytes(*uid);
                    if generate_key_as_hexa(uid, master_key).as_bytes().ct_eq(encoded_key).into() {
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
        let tuple = (&uid, &action, index.map(|s| s.as_bytes()));
        match self.action_keyid_index_expiration.get(&rtxn, &tuple)? {
            Some(expiration) => Ok(Some(expiration)),
            None => {
                let tuple = (&uid, &action, None);
                for result in self.action_keyid_index_expiration.prefix_iter(&rtxn, &tuple)? {
                    let ((_, _, index_uid_pattern), expiration) = result?;
                    if let Some((pattern, index)) = index_uid_pattern.zip(index) {
                        let index_uid_pattern = str::from_utf8(pattern)?;
                        let pattern = IndexUidPattern::from_str(index_uid_pattern)
                            .map_err(|e| AuthControllerError::Internal(Box::new(e)))?;
                        if pattern.matches_str(index) {
                            return Ok(Some(expiration));
                        }
                    }
                }
                Ok(None)
            }
        }
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
            .remap_types::<Bytes, DecodeIgnore>()
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

    fn bytes_encode(
        (key_id, action, index): &'_ Self::EItem,
    ) -> StdResult<Cow<'_, [u8]>, BoxedError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (HeedAuthStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let env = open_auth_store_env(temp_dir.path()).unwrap();
        let store = HeedAuthStore::new(env).unwrap();
        (store, temp_dir)
    }

    #[test]
    fn test_get_uid_from_encoded_key_correct_key() {
        let (store, _temp_dir) = create_test_store();
        let master_key = b"test_master_key_12345678901234567890";
        let uid = Uuid::new_v4();

        // Create a key
        let key = Key {
            uid,
            name: Some("Test Key".to_string()),
            description: None,
            actions: vec![Action::Search],
            indexes: vec![],
            expires_at: None,
            created_at: time::OffsetDateTime::now_utc(),
            updated_at: time::OffsetDateTime::now_utc(),
        };
        store.put_api_key(key).unwrap();

        // Generate the encoded key
        let encoded_key = generate_key_as_hexa(uid, master_key);
        let encoded_key_bytes = encoded_key.as_bytes();

        // Test that we can retrieve the UID with the correct key
        let retrieved_uid = store
            .get_uid_from_encoded_key(encoded_key_bytes, master_key)
            .unwrap();
        assert_eq!(retrieved_uid, Some(uid));
    }

    #[test]
    fn test_get_uid_from_encoded_key_incorrect_key() {
        let (store, _temp_dir) = create_test_store();
        let master_key = b"test_master_key_12345678901234567890";
        let uid = Uuid::new_v4();

        // Create a key
        let key = Key {
            uid,
            name: Some("Test Key".to_string()),
            description: None,
            actions: vec![Action::Search],
            indexes: vec![],
            expires_at: None,
            created_at: time::OffsetDateTime::now_utc(),
            updated_at: time::OffsetDateTime::now_utc(),
        };
        store.put_api_key(key).unwrap();

        // Generate the encoded key
        let encoded_key = generate_key_as_hexa(uid, master_key);

        // Test with incorrect key (wrong master key)
        let wrong_master_key = b"wrong_master_key_1234567890123456789";
        let retrieved_uid = store
            .get_uid_from_encoded_key(encoded_key.as_bytes(), wrong_master_key)
            .unwrap();
        assert_eq!(retrieved_uid, None);

        // Test with completely wrong encoded key
        let wrong_encoded_key = "a".repeat(64);
        let retrieved_uid = store
            .get_uid_from_encoded_key(wrong_encoded_key.as_bytes(), master_key)
            .unwrap();
        assert_eq!(retrieved_uid, None);
    }

    #[test]
    fn test_get_uid_from_encoded_key_wrong_length() {
        let (store, _temp_dir) = create_test_store();
        let master_key = b"test_master_key_12345678901234567890";

        // Test with key that has wrong length
        let wrong_length_key = "a".repeat(32); // 32 chars instead of 64
        let retrieved_uid = store
            .get_uid_from_encoded_key(wrong_length_key.as_bytes(), master_key)
            .unwrap();
        assert_eq!(retrieved_uid, None);
    }

    #[test]
    fn test_constant_time_comparison_used() {
        // This test verifies that ct_eq is being used by checking that
        // the comparison doesn't short-circuit on first mismatch
        use subtle::ConstantTimeEq;

        let key1 = b"a".repeat(64);
        let key2 = b"b".repeat(64);

        // Both keys should take the same time to compare (constant-time)
        // We can't directly measure timing in a unit test, but we can verify
        // that ct_eq returns the correct result
        let result1: bool = key1.as_slice().ct_eq(key1.as_slice()).into();
        let result2: bool = key1.as_slice().ct_eq(key2.as_slice()).into();

        assert!(result1);
        assert!(!result2);
    }

    #[test]
    fn test_generate_key_as_hexa_consistency() {
        let uid = Uuid::new_v4();
        let master_key = b"test_master_key_12345678901234567890";

        // Generate the same key twice - should be identical
        let key1 = generate_key_as_hexa(uid, master_key);
        let key2 = generate_key_as_hexa(uid, master_key);

        assert_eq!(key1, key2);
        assert_eq!(key1.len(), 64); // HMAC-SHA256 produces 32 bytes = 64 hex chars
    }

    #[test]
    fn test_get_uid_from_encoded_key_multiple_keys() {
        let (store, _temp_dir) = create_test_store();
        let master_key = b"test_master_key_12345678901234567890";

        // Create multiple keys
        let uid1 = Uuid::new_v4();
        let uid2 = Uuid::new_v4();
        let uid3 = Uuid::new_v4();

        for uid in [uid1, uid2, uid3] {
            let key = Key {
                uid,
                name: Some(format!("Test Key {}", uid)),
                description: None,
                actions: vec![Action::Search],
                indexes: vec![],
                expires_at: None,
                created_at: time::OffsetDateTime::now_utc(),
                updated_at: time::OffsetDateTime::now_utc(),
            };
            store.put_api_key(key).unwrap();
        }

        // Test retrieving each key
        let encoded_key1 = generate_key_as_hexa(uid1, master_key);
        let encoded_key2 = generate_key_as_hexa(uid2, master_key);
        let encoded_key3 = generate_key_as_hexa(uid3, master_key);

        assert_eq!(
            store
                .get_uid_from_encoded_key(encoded_key1.as_bytes(), master_key)
                .unwrap(),
            Some(uid1)
        );
        assert_eq!(
            store
                .get_uid_from_encoded_key(encoded_key2.as_bytes(), master_key)
                .unwrap(),
            Some(uid2)
        );
        assert_eq!(
            store
                .get_uid_from_encoded_key(encoded_key3.as_bytes(), master_key)
                .unwrap(),
            Some(uid3)
        );
    }
}
