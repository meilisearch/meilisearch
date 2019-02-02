use std::cmp::Ordering;
use std::ops::Deref;
use std::marker;

use rocksdb::DB;
use serde::de::DeserializeOwned;

use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::rank::RawDocument;

/// An helper struct that permit to sort documents by
/// some of their stored attributes.
///
/// # Note
///
/// If a document cannot be deserialized it will be considered [`None`][].
///
/// Deserialized documents are compared like `Some(doc0).cmp(&Some(doc1))`,
/// so you must check the [`Ord`] of `Option` implementation.
///
/// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
/// [`Ord`]: https://doc.rust-lang.org/std/option/enum.Option.html#impl-Ord
///
/// # Example
///
/// ```ignore
/// use serde_derive::Deserialize;
/// use meilidb::rank::criterion::*;
///
/// #[derive(Deserialize, PartialOrd, Ord, PartialEq, Eq)]
/// struct TimeOnly {
///     time: String,
/// }
///
/// let builder = CriteriaBuilder::with_capacity(8)
///        .add(SumOfTypos)
///        .add(NumberOfWords)
///        .add(WordsProximity)
///        .add(SumOfWordsAttribute)
///        .add(SumOfWordsPosition)
///        .add(Exact)
///        .add(SortBy::<TimeOnly>::new(&view))
///        .add(DocumentId);
///
/// let criterion = builder.build();
///
/// ```
pub struct SortBy<'a, T, D>
where D: Deref<Target=DB> + Send + Sync,
      T: Send + Sync
{
    view: &'a DatabaseView<D>,
    _phantom: marker::PhantomData<T>,
}

impl<'a, T, D> SortBy<'a, T, D>
where D: Deref<Target=DB> + Send + Sync,
      T: Send + Sync
{
    pub fn new(view: &'a DatabaseView<D>) -> Self {
        SortBy { view, _phantom: marker::PhantomData }
    }
}

impl<'a, T, D> Criterion for SortBy<'a, T, D>
where D: Deref<Target=DB> + Send + Sync,
      T: DeserializeOwned + Ord + Send + Sync,
{
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = match self.view.document_by_id::<T>(lhs.id) {
            Ok(doc) => Some(doc),
            Err(e) => { eprintln!("{}", e); None },
        };

        let rhs = match self.view.document_by_id::<T>(rhs.id) {
            Ok(doc) => Some(doc),
            Err(e) => { eprintln!("{}", e); None },
        };

        lhs.cmp(&rhs)
    }
}
