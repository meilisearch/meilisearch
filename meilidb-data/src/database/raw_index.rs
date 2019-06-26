use std::sync::Arc;
use super::{MainIndex, SynonymsIndex, WordsIndex, DocsWordsIndex, DocumentsIndex, CustomSettings};

#[derive(Clone)]
pub struct RawIndex {
    pub main: MainIndex,
    pub synonyms: SynonymsIndex,
    pub words: WordsIndex,
    pub docs_words: DocsWordsIndex,
    pub documents: DocumentsIndex,
    pub custom: CustomSettings,
}

impl RawIndex {
    pub(crate) fn compact(&self) {
        self.main.0.compact_range(None::<&[u8]>, None::<&[u8]>);
        self.synonyms.0.compact_range(None::<&[u8]>, None::<&[u8]>);
        self.words.0.compact_range(None::<&[u8]>, None::<&[u8]>);
        self.docs_words.0.compact_range(None::<&[u8]>, None::<&[u8]>);
        self.documents.0.compact_range(None::<&[u8]>, None::<&[u8]>);
        self.custom.0.compact_range(None::<&[u8]>, None::<&[u8]>);
    }
}

#[derive(Clone)]
pub struct InnerRawIndex {
    database: Arc<rocksdb::DB>,
    name: Arc<str>,
}

impl InnerRawIndex {
    pub fn new(database: Arc<rocksdb::DB>, name: Arc<str>) -> InnerRawIndex {
        InnerRawIndex { database, name }
    }

    pub fn get<K>(&self, key: K) -> Result<Option<rocksdb::DBVector>, rocksdb::Error>
    where K: AsRef<[u8]>,
    {
        let cf = self.database.cf_handle(&self.name).expect("cf not found");
        self.database.get_cf(cf, key)
    }

    pub fn get_pinned<K>(&self, key: K) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error>
    where K: AsRef<[u8]>,
    {
        let cf = self.database.cf_handle(&self.name).expect("cf not found");
        self.database.get_pinned_cf(cf, key)
    }

    pub fn iterator(&self, from: rocksdb::IteratorMode) -> Result<rocksdb::DBIterator, rocksdb::Error> {
        let cf = self.database.cf_handle(&self.name).expect("cf not found");
        self.database.iterator_cf(cf, from)
    }

    pub fn set<K, V>(&self, key: K, value: V) -> Result<(), rocksdb::Error>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        let cf = self.database.cf_handle(&self.name).expect("cf not found");
        self.database.put_cf(cf, key, value)
    }

    pub fn delete<K>(&self, key: K) -> Result<(), rocksdb::Error>
    where K: AsRef<[u8]>
    {
        let cf = self.database.cf_handle(&self.name).expect("cf not found");
        self.database.delete_cf(cf, key)
    }

    pub fn delete_range<K>(&self, start: K, end: K) -> Result<(), rocksdb::Error>
    where K: AsRef<[u8]>,
    {
        let mut batch = rocksdb::WriteBatch::default();

        let cf = self.database.cf_handle(&self.name).expect("cf not found");
        batch.delete_range_cf(cf, start, end)?;

        self.database.write(batch)
    }

    pub fn compact_range<S, E>(&self, start: Option<S>, end: Option<E>)
    where S: AsRef<[u8]>,
          E: AsRef<[u8]>,
    {
        let cf = self.database.cf_handle(&self.name).expect("cf not found");
        self.database.compact_range_cf(cf, start, end)
    }
}
