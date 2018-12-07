use std::error::Error;
use std::{fmt, marker};

use rocksdb::rocksdb::{DB, DBVector, Snapshot, SeekKey};
use rocksdb::rocksdb_options::ReadOptions;
use serde::de::DeserializeOwned;

use crate::database::{DocumentKey, DocumentKeyAttr};
use crate::database::{retrieve_data_schema, retrieve_data_index};
use crate::database::blob::positive::PositiveBlob;
use crate::database::deserializer::Deserializer;
use crate::rank::criterion::Criterion;
use crate::database::schema::Schema;
use crate::rank::QueryBuilder;
use crate::DocumentId;

pub struct DatabaseView<'a> {
    snapshot: Snapshot<&'a DB>,
    blob: PositiveBlob,
    schema: Schema,
}

impl<'a> DatabaseView<'a> {
    pub fn new(snapshot: Snapshot<&'a DB>) -> Result<DatabaseView, Box<Error>> {
        let schema = retrieve_data_schema(&snapshot)?;
        let blob = retrieve_data_index(&snapshot)?;
        Ok(DatabaseView { snapshot, blob, schema })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn blob(&self) -> &PositiveBlob {
        &self.blob
    }

    pub fn into_snapshot(self) -> Snapshot<&'a DB> {
        self.snapshot
    }

    pub fn snapshot(&self) -> &Snapshot<&'a DB> {
        &self.snapshot
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Box<Error>> {
        Ok(self.snapshot.get(key)?)
    }

    pub fn query_builder(&self) -> Result<QueryBuilder<Box<dyn Criterion>>, Box<Error>> {
        QueryBuilder::new(self)
    }

    // TODO create an enum error type
    pub fn retrieve_document<D>(&self, id: DocumentId) -> Result<D, Box<Error>>
    where D: DeserializeOwned
    {
        let mut deserializer = Deserializer::new(&self.snapshot, &self.schema, id);
        Ok(D::deserialize(&mut deserializer)?)
    }

    pub fn retrieve_documents<D, I>(&self, ids: I) -> DocumentIter<D, I::IntoIter>
    where D: DeserializeOwned,
          I: IntoIterator<Item=DocumentId>,
    {
        DocumentIter {
            database_view: self,
            document_ids: ids.into_iter(),
            _phantom: marker::PhantomData,
        }
    }
}

impl<'a> fmt::Debug for DatabaseView<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut options = ReadOptions::new();
        let lower = DocumentKey::new(0);
        options.set_iterate_lower_bound(lower.as_ref());

        let mut iter = self.snapshot.iter_opt(options);
        iter.seek(SeekKey::Start);
        let iter = iter.map(|(key, _)| DocumentKeyAttr::from_bytes(&key));

        if f.alternate() {
            writeln!(f, "DatabaseView(")?;
        } else {
            write!(f, "DatabaseView(")?;
        }

        self.schema.fmt(f)?;

        if f.alternate() {
            writeln!(f, ",")?;
        } else {
            write!(f, ", ")?;
        }

        f.debug_list().entries(iter).finish()?;

        write!(f, ")")
    }
}

// TODO this is just an iter::Map !!!
pub struct DocumentIter<'a, D, I> {
    database_view: &'a DatabaseView<'a>,
    document_ids: I,
    _phantom: marker::PhantomData<D>,
}

impl<'a, D, I> Iterator for DocumentIter<'a, D, I>
where D: DeserializeOwned,
      I: Iterator<Item=DocumentId>,
{
    type Item = Result<D, Box<Error>>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.document_ids.size_hint()
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.document_ids.next() {
            Some(id) => Some(self.database_view.retrieve_document(id)),
            None => None
        }
    }
}

impl<'a, D, I> ExactSizeIterator for DocumentIter<'a, D, I>
where D: DeserializeOwned,
      I: ExactSizeIterator + Iterator<Item=DocumentId>,
{ }

impl<'a, D, I> DoubleEndedIterator for DocumentIter<'a, D, I>
where D: DeserializeOwned,
      I: DoubleEndedIterator + Iterator<Item=DocumentId>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.document_ids.next_back() {
            Some(id) => Some(self.database_view.retrieve_document(id)),
            None => None
        }
    }
}
