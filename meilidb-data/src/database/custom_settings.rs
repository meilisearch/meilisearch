use rocksdb::DBVector;
use crate::database::raw_index::InnerRawIndex;

#[derive(Clone)]
pub struct CustomSettings(pub(crate) InnerRawIndex);

impl CustomSettings {
    pub fn set<K, V>(&self, key: K, value: V) -> Result<(), rocksdb::Error>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        self.0.set(key, value)
    }

    pub fn get<K, V>(&self, key: K) -> Result<Option<DBVector>, rocksdb::Error>
    where K: AsRef<[u8]>,
    {
        self.0.get(key)
    }
}
