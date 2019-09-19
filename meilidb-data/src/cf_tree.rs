use std::sync::Arc;
use crossbeam_channel::{unbounded, Sender, Receiver};
use rocksdb::{DBVector, IteratorMode, Direction};
use crate::RocksDbResult;

#[derive(Clone)]
pub struct CfTree {
    index: Arc<CfTreeInner>,
    sender: Option<Sender<()>>,
}

struct CfTreeInner {
    db: Arc<rocksdb::DB>,
    name: String,
}

impl CfTree {
    pub fn create(db: Arc<rocksdb::DB>, name: String) -> RocksDbResult<CfTree> {
        let mut options = rocksdb::Options::default();
        options.create_missing_column_families(true); // this doesn't work

        if db.cf_handle(&name).is_none() {
            let _cf = db.create_cf(&name, &options)?;
        }

        let index = Arc::new(CfTreeInner { db, name });

        Ok(CfTree { index, sender: None })
    }

    pub fn create_with_subcription(
        db: Arc<rocksdb::DB>,
        name: String,
    ) -> RocksDbResult<(CfTree, Receiver<()>)>
    {
        let mut options = rocksdb::Options::default();
        options.create_missing_column_families(true); // this doesn't work

        if db.cf_handle(&name).is_none() {
            let _cf = db.create_cf(&name, &options)?;
        }

        let index = Arc::new(CfTreeInner { db, name });
        let (sender, receiver) = unbounded();

        Ok((CfTree { index, sender: Some(sender) }, receiver))
    }

    pub fn insert<K, V>(&self, key: K, value: V) -> RocksDbResult<()>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        let cf = self.index.db.cf_handle(&self.index.name).unwrap();
        let result = self.index.db.put_cf(cf, key, value);

        if let Some(sender) = &self.sender {
            let _err = sender.send(());
        }

        result
    }

    pub fn get<K>(&self, key: K) -> RocksDbResult<Option<DBVector>>
    where K: AsRef<[u8]>,
    {
        let cf = self.index.db.cf_handle(&self.index.name).unwrap();
        self.index.db.get_cf(cf, key)
    }

    pub fn remove<K>(&self, key: K) -> RocksDbResult<()>
    where K: AsRef<[u8]>
    {
        let cf = self.index.db.cf_handle(&self.index.name).unwrap();
        self.index.db.delete_cf(cf, key)
    }

    /// Start and end key range is inclusive on both bounds.
    pub fn range<KS, KE>(&self, start: KS, end: KE) -> RocksDbResult<CfIter>
    where KS: AsRef<[u8]>,
          KE: AsRef<[u8]>,
    {
        let cf = self.index.db.cf_handle(&self.index.name).unwrap();

        let mut iter = self.index.db.iterator_cf(cf, IteratorMode::Start)?;
        iter.set_mode(IteratorMode::From(start.as_ref(), Direction::Forward));

        let end_bound = Box::from(end.as_ref());
        Ok(CfIter { iter, end_bound: Some(end_bound) })
    }

    pub fn iter(&self) -> RocksDbResult<CfIter> {
        let cf = self.index.db.cf_handle(&self.index.name).unwrap();
        let iter = self.index.db.iterator_cf(cf, IteratorMode::Start)?;
        Ok(CfIter { iter, end_bound: None })
    }

    pub fn last_key(&self) -> RocksDbResult<Option<Box<[u8]>>> {
        let cf = self.index.db.cf_handle(&self.index.name).unwrap();
        let mut iter = self.index.db.iterator_cf(cf, IteratorMode::End)?;
        Ok(iter.next().map(|(key, _)| key))
    }

    pub fn prefix_iterator<P>(&self, prefix: P) -> RocksDbResult<rocksdb::DBIterator>
    where P: AsRef<[u8]>,
    {
        let cf = self.index.db.cf_handle(&self.index.name).unwrap();
        self.index.db.prefix_iterator_cf(cf, prefix)
    }
}

pub struct CfIter<'a> {
    iter: rocksdb::DBIterator<'a>,
    end_bound: Option<Box<[u8]>>,
}

impl Iterator for CfIter<'_> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter.next(), &self.end_bound) {
            (Some((ref key, _)), Some(end_bound)) if key > end_bound => None,
            (Some(entry), _) => Some(entry),
            (None, _) => None,
        }
    }
}
