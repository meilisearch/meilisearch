use std::sync::Arc;
use rocksdb::DBVector;

#[derive(Clone)]
pub struct CustomSettings(pub Arc<rocksdb::DB>, pub String);

impl CustomSettings {
    pub fn set<K, V>(&self, key: K, value: V) -> Result<(), rocksdb::Error>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        let cf = self.0.cf_handle(&self.1).unwrap();
        self.0.put_cf(cf, key, value)
    }

    pub fn get<K, V>(&self, key: K) -> Result<Option<DBVector>, rocksdb::Error>
    where K: AsRef<[u8]>,
    {
        let cf = self.0.cf_handle(&self.1).unwrap();
        self.0.get_cf(cf, key)
    }
}
