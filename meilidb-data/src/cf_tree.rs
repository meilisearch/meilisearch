use std::sync::Arc;
use rocksdb::{DBVector, IteratorMode, Direction};
use crate::RocksDbResult;

#[derive(Clone)]
pub struct CfTree(Arc<CfTreeInner>);

struct CfTreeInner {
    db: rocksdb::DB,
    name: String,
}

impl CfTree {
    pub fn insert<K, V>(&self, key: K, value: V) -> RocksDbResult<()>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        let cf = self.0.db.cf_handle(&self.0.name).unwrap();
        self.0.db.put_cf(cf, key, value)
    }

    pub fn get<K>(&self, key: K) -> RocksDbResult<Option<DBVector>>
    where K: AsRef<[u8]>,
    {
        let cf = self.0.db.cf_handle(&self.0.name).unwrap();
        self.0.db.get_cf(cf, key)
    }

    pub fn remove<K>(&self, key: K) -> RocksDbResult<()>
    where K: AsRef<[u8]>
    {
        let cf = self.0.db.cf_handle(&self.0.name).unwrap();
        self.0.db.delete_cf(cf, key)
    }

    /// Start and end key range is inclusive on both bounds.
    pub fn range<KS, KE>(&self, start: KS, end: KE) -> RocksDbResult<CfIter>
    where KS: AsRef<[u8]>,
          KE: AsRef<[u8]>,
    {
        let cf = self.0.db.cf_handle(&self.0.name).unwrap();

        let mut iter = self.0.db.iterator_cf(cf, IteratorMode::Start)?;
        iter.set_mode(IteratorMode::From(start.as_ref(), Direction::Forward));

        let end_bound = Box::from(end.as_ref());
        Ok(CfIter { iter, end_bound: Some(end_bound) })
    }

    pub fn iter(&self) -> RocksDbResult<CfIter> {
        let cf = self.0.db.cf_handle(&self.0.name).unwrap();
        let iter = self.0.db.iterator_cf(cf, IteratorMode::Start)?;
        Ok(CfIter { iter, end_bound: None })
    }
}

pub struct CfIter<'a> {
    iter: rocksdb::DBIterator<'a>,
    end_bound: Option<Box<[u8]>>,
}

impl Iterator for CfIter<'_> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter.next(), self.end_bound) {
            (Some((key, _)), Some(end_bound)) if key > end_bound => None,
            (Some(entry), _) => Some(entry),
            (None, _) => None,
        }
    }
}
