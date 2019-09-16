use serde::de::DeserializeOwned;
use serde::Serialize;
use super::Error;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct CommonIndex(pub crate::CfTree);

impl CommonIndex {
    pub fn get<T, K>(&self, key: K) -> Result<Option<T>, Error>
    where T: DeserializeOwned,
          K: AsRef<[u8]>,
    {
        let raw = match self.0.get(key)? {
            Some(raw) => raw,
            None => return Ok(None),
        };
        let data = bincode::deserialize(&raw)?;
        Ok(Some(data))
    }

    pub fn set<T, K>(&self, key: K, data: &T) -> Result<(), Error>
    where T: Serialize,
          K: AsRef<[u8]>,
    {
        let raw = bincode::serialize(data)?;
        self.0.insert(key, &raw)?;
        Ok(())
    }

    pub fn prefix_iterator<T, P>(&self, prefix: P) -> Result<SerializedIterator<T>, Error>
    where T: DeserializeOwned,
          P: AsRef<[u8]>,
    {
        let iter = self.0.prefix_iterator(prefix)?;
        Ok(SerializedIterator { iter, _marker: PhantomData })
    }
}

pub struct SerializedIterator<'a, T> {
    iter: rocksdb::DBIterator<'a>,
    _marker: PhantomData<T>,
}

impl<T> Iterator for SerializedIterator<'_, T>
where T: DeserializeOwned,
{
    type Item = (String, T);

    fn next(&mut self) -> Option<Self::Item> {
        let (raw_key, raw_value) = match self.iter.next() {
            Some((key, value)) => (key, value),
            None => return None,
        };

        let value: T = match bincode::deserialize(&raw_value) {
            Ok(data) => data,
            Err(_) => return None,
        };

        let key = match std::str::from_utf8(&raw_key) {
            Ok(key) => key.to_string(),
            Err(_) => return None,
        };

        Some((key, value))
    }
}
