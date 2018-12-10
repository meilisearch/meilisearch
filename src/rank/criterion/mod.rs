mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;

use std::cmp::Ordering;
use std::ops::Deref;
use std::marker;

use serde::de::DeserializeOwned;
use rocksdb::DB;

use crate::database::DatabaseView;
use crate::rank::Document;

pub use self::{
    sum_of_typos::SumOfTypos,
    number_of_words::NumberOfWords,
    words_proximity::WordsProximity,
    sum_of_words_attribute::SumOfWordsAttribute,
    sum_of_words_position::SumOfWordsPosition,
    exact::Exact,
};

pub trait Criterion<D>
where D: Deref<Target=DB>
{
    #[inline]
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering;

    #[inline]
    fn eq(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> bool {
        self.evaluate(lhs, rhs, view) == Ordering::Equal
    }
}

impl<'a, D, T: Criterion<D> + ?Sized> Criterion<D> for &'a T
where D: Deref<Target=DB>
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering {
        (**self).evaluate(lhs, rhs, view)
    }

    fn eq(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> bool {
        (**self).eq(lhs, rhs, view)
    }
}

impl<D, T: Criterion<D> + ?Sized> Criterion<D> for Box<T>
where D: Deref<Target=DB>
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering {
        (**self).evaluate(lhs, rhs, view)
    }

    fn eq(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> bool {
        (**self).eq(lhs, rhs, view)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DocumentId;

impl<D> Criterion<D> for DocumentId
where D: Deref<Target=DB>
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        lhs.id.cmp(&rhs.id)
    }
}

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
/// ```
/// use serde_derive::Deserialize;
/// use meilidb::rank::criterion::*;
///
/// #[derive(Deserialize, PartialOrd, Ord, PartialEq, Eq)]
/// struct TimeOnly {
///     time: String,
/// }
///
/// let builder = CriteriaBuilder::with_capacity(7)
///        .add(SumOfTypos)
///        .add(NumberOfWords)
///        .add(WordsProximity)
///        .add(SumOfWordsAttribute)
///        .add(SumOfWordsPosition)
///        .add(Exact)
///        .add(SortBy::<TimeOnly>::new())
///        .add(DocumentId);
///
/// let criterion = builder.build();
///
/// ```
#[derive(Default)]
pub struct SortBy<T> {
    _phantom: marker::PhantomData<T>,
}

impl<T> SortBy<T> {
    pub fn new() -> Self {
        SortBy { _phantom: marker::PhantomData }
    }
}

impl<T, D> Criterion<D> for SortBy<T>
where D: Deref<Target=DB>,
      T: DeserializeOwned + Ord,
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering {
        let lhs = match view.retrieve_document::<T>(lhs.id) {
            Ok(doc) => Some(doc),
            Err(e) => { eprintln!("{}", e); None },
        };

        let rhs = match view.retrieve_document::<T>(rhs.id) {
            Ok(doc) => Some(doc),
            Err(e) => { eprintln!("{}", e); None },
        };

        lhs.cmp(&rhs)
    }
}

pub struct CriteriaBuilder<D>
where D: Deref<Target=DB>
{
    inner: Vec<Box<dyn Criterion<D>>>
}

impl<D> CriteriaBuilder<D>
where D: Deref<Target=DB>
{
    pub fn new() -> CriteriaBuilder<D> {
        CriteriaBuilder { inner: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> CriteriaBuilder<D> {
        CriteriaBuilder { inner: Vec::with_capacity(capacity) }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional)
    }

    pub fn add<C>(mut self, criterion: C) -> CriteriaBuilder<D>
    where C: 'static + Criterion<D>,
    {
        self.push(criterion);
        self
    }

    pub fn push<C>(&mut self, criterion: C)
    where C: 'static + Criterion<D>,
    {
        self.inner.push(Box::new(criterion));
    }

    pub fn build(self) -> Vec<Box<dyn Criterion<D>>> {
        self.inner
    }
}

pub fn default<D>() -> Vec<Box<dyn Criterion<D>>>
where D: Deref<Target=DB>
{
    CriteriaBuilder::with_capacity(7)
        .add(SumOfTypos)
        .add(NumberOfWords)
        .add(WordsProximity)
        .add(SumOfWordsAttribute)
        .add(SumOfWordsPosition)
        .add(Exact)
        .add(DocumentId)
        .build()
}
