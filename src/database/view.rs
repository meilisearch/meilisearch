use std::error::Error;
use std::path::Path;
use std::ops::Deref;
use std::marker;

use rocksdb::rocksdb_options::{ReadOptions, EnvOptions, ColumnFamilyOptions};
use rocksdb::rocksdb::{DB, Snapshot, SeekKey, SstFileWriter, CFHandle};
use serde::de::DeserializeOwned;
use chashmap::ReadGuard;

use crate::database::{retrieve_data_schema, retrieve_data_index};
use crate::database::deserializer::Deserializer;
use crate::database::schema::Schema;
use crate::database::index::Index;
use crate::rank::{QueryBuilder, FilterFunc};
use crate::DocumentId;

pub struct DatabaseView<'h, D>
where D: Deref<Target=DB>
{
    snapshot: Snapshot<D>,
    handle: ReadGuard<'h, String, CFHandle>,
    index: Index,
    schema: Schema,
}

impl<'h, D> DatabaseView<'h, D>
where D: Deref<Target=DB>
{
    pub fn new(
        snapshot: Snapshot<D>,
        handle: ReadGuard<'h, String, CFHandle>
    ) -> Result<DatabaseView<D>, Box<Error>>
    {
        let schema = retrieve_data_schema(&snapshot, &handle)?;
        let index = retrieve_data_index(&snapshot, &handle)?;
        Ok(DatabaseView { snapshot, handle, index, schema })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn into_snapshot(self) -> Snapshot<D> {
        self.snapshot
    }

    pub fn snapshot(&self) -> &Snapshot<D> {
        &self.snapshot
    }

    pub fn dump_all<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let env_options = EnvOptions::new();
        let column_family_options = ColumnFamilyOptions::new();
        let mut file_writer = SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&path)?;

        let mut iter = self.snapshot.iter_cf(&self.handle, ReadOptions::new());
        iter.seek(SeekKey::Start);

        for (key, value) in &mut iter {
            file_writer.put(&key, &value)?;
        }

        file_writer.finish()?;
        Ok(())
    }

    pub fn query_builder(&self) -> Result<QueryBuilder<D, FilterFunc<D>>, Box<Error>> {
        QueryBuilder::new(self)
    }

    pub fn document_by_id<T>(&self, id: DocumentId) -> Result<T, Box<Error>>
    where T: DeserializeOwned
    {
        let mut deserializer = Deserializer {
            snapshot: &self.snapshot,
            handle: &self.handle,
            schema: &self.schema,
            document_id: id,
        };
        Ok(T::deserialize(&mut deserializer)?)
    }

    pub fn documents_by_id<T, I>(&self, ids: I) -> DocumentIter<D, T, I::IntoIter>
    where T: DeserializeOwned,
          I: IntoIterator<Item=DocumentId>,
    {
        DocumentIter {
            database_view: self,
            document_ids: ids.into_iter(),
            _phantom: marker::PhantomData,
        }
    }
}

// TODO this is just an iter::Map !!!
pub struct DocumentIter<'a, 'h, D, T, I>
where D: Deref<Target=DB>
{
    database_view: &'a DatabaseView<'h, D>,
    document_ids: I,
    _phantom: marker::PhantomData<T>,
}

impl<'a, 'h, D, T, I> Iterator for DocumentIter<'a, 'h, D, T, I>
where D: Deref<Target=DB>,
      T: DeserializeOwned,
      I: Iterator<Item=DocumentId>,
{
    type Item = Result<T, Box<Error>>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.document_ids.size_hint()
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.document_ids.next() {
            Some(id) => Some(self.database_view.document_by_id(id)),
            None => None
        }
    }
}

impl<'a, 'h, D, T, I> ExactSizeIterator for DocumentIter<'a, 'h, D, T, I>
where D: Deref<Target=DB>,
      T: DeserializeOwned,
      I: ExactSizeIterator + Iterator<Item=DocumentId>,
{ }

impl<'a, 'h, D, T, I> DoubleEndedIterator for DocumentIter<'a, 'h, D, T, I>
where D: Deref<Target=DB>,
      T: DeserializeOwned,
      I: DoubleEndedIterator + Iterator<Item=DocumentId>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.document_ids.next_back() {
            Some(id) => Some(self.database_view.document_by_id(id)),
            None => None
        }
    }
}
