//! Everything related to criteria and criterion rules.
//!
//! The default [Criteria][1] implementation is: [SumOfTypos],
//! [NumberOfWords], [WordsProximity], [SumOfWordsAttribute], [SumOfWordsPosition],
//! [Exact] and a last sort by [DocumentId].
//!
//! [1]: struct.Criteria.html
//! [SumOfTypos]: struct.SumOfTypos.html
//! [NumberOfWords]: struct.NumberOfWords.html
//! [WordsProximity]: struct.WordsProximity.html
//! [SumOfWordsAttribute]: struct.SumOfWordsAttribute.html
//! [SumOfWordsPosition]: struct.SumOfWordsPosition.html
//! [Exact]: struct.Exact.html
//! [DocumentId]: struct.DocumentId.html

mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;
mod sort_by;
mod document_id;

use std::cmp::Ordering;
use std::ops::Deref;

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
    sort_by::SortBy,
    document_id::DocumentId,
};

/// A trait specifying how to sort documents.
///
/// Criterion are stored into the [Criteria][1] type and used by the [QueryBuilder][2].
///
/// [1]: struct.Criteria.html
/// [2]: ../struct.QueryBuilder.html
pub trait Criterion<D>
where D: Deref<Target=DB>
{
    /// Giving two [DocumentIds][1] and a [DatabaseView][2], this function returns
    /// what is their ordering.
    ///
    /// [1]: ../../type.DocumentId.html
    /// [2]: ../../database/struct.DatabaseView.html
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering;

    #[inline]
    /// Giving two [DocumentIds][1] and a [DatabaseView][2], this function returns `true`
    /// if the two documents can be considered equal.
    ///
    /// [1]: ../../type.DocumentId.html
    /// [2]: ../../database/struct.DatabaseView.html
    fn eq(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> bool {
        self.evaluate(lhs, rhs, view) == Ordering::Equal
    }
}

impl<'a, D, T: Criterion<D> + ?Sized> Criterion<D> for &'a T
where D: Deref<Target=DB>
{
    #[inline]
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering {
        (**self).evaluate(lhs, rhs, view)
    }

    #[inline]
    fn eq(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> bool {
        (**self).eq(lhs, rhs, view)
    }
}

impl<D, T: Criterion<D> + ?Sized> Criterion<D> for Box<T>
where D: Deref<Target=DB>
{
    #[inline]
    fn evaluate(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> Ordering {
        (**self).evaluate(lhs, rhs, view)
    }

    #[inline]
    fn eq(&self, lhs: &Document, rhs: &Document, view: &DatabaseView<D>) -> bool {
        (**self).eq(lhs, rhs, view)
    }
}

/// A builder of [Criteria][1], this is basically a [Vec][2] of [Trait objects][3].
///
/// [1]: struct.Criteria.html
/// [2]: https://doc.rust-lang.org/std/vec/struct.Vec.html
/// [3]: https://doc.rust-lang.org/book/ch17-02-trait-objects.html
///
/// # Examples
///
/// ```
/// use meilidb::rank::criterion::*;
/// use meilidb::rocksdb::DB;
/// use meilidb::rank::*;
///
/// CriteriaBuilder::<&DB>::new()
///     .add(SumOfTypos)
///     .add(NumberOfWords)
///     .add(WordsProximity)
///     .add(SumOfWordsAttribute)
///     .add(SumOfWordsPosition)
///     .add(Exact)
///     .add(DocumentId)
///     .build();
/// ```
pub struct CriteriaBuilder<D>
where D: Deref<Target=DB>
{
    inner: Vec<Box<dyn Criterion<D>>>
}

impl<D> CriteriaBuilder<D>
where D: Deref<Target=DB>
{
    /// Create an empty builder without any allocation.
    pub fn new() -> CriteriaBuilder<D> {
        CriteriaBuilder { inner: Vec::new() }
    }

    /// Create an empty builder which will be able to store `capacity` criterion
    /// before needing a reallocation.
    pub fn with_capacity(capacity: usize) -> CriteriaBuilder<D> {
        CriteriaBuilder { inner: Vec::with_capacity(capacity) }
    }

    /// Reserve for a given number of `additional` criterion.
    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional)
    }

    /// Add a criterion at the end of the list of criteria, this method returns
    /// the builder, useful to chain create a [Criteria][1].
    ///
    /// [1]: struct.Criteria.html
    ///
    /// # Examples
    ///
    /// ```
    /// use meilidb::rank::criterion::*;
    /// use meilidb::rocksdb::DB;
    /// use meilidb::rank::*;
    ///
    /// CriteriaBuilder::<&DB>::new()
    ///     .add(SumOfTypos)
    ///     .add(NumberOfWords)
    ///     .add(WordsProximity)
    ///     .add(SumOfWordsAttribute)
    ///     .add(SumOfWordsPosition)
    ///     .add(Exact)
    ///     .add(DocumentId)
    ///     .build();
    /// ```
    pub fn add<C>(mut self, criterion: C) -> CriteriaBuilder<D>
    where C: 'static + Criterion<D>,
    {
        self.push(criterion);
        self
    }

    /// Pushes a criterion add the end of criteria.
    pub fn push<C>(&mut self, criterion: C)
    where C: 'static + Criterion<D>,
    {
        self.inner.push(Box::new(criterion));
    }

    /// Build the final [Criteria][1].
    ///
    /// [1]: struct.Criteria.html
    pub fn build(self) -> Criteria<D> {
        Criteria { inner: self.inner }
    }
}

/// Represents a list of [Criterion][1] which will be applied in order.
///
/// For more informaition on the default criteria see [the module documentation][2].
///
/// [1]: trait.Criterion.html
/// [2]: index.html
pub struct Criteria<D>
where D: Deref<Target=DB>
{
    inner: Vec<Box<dyn Criterion<D>>>,
}

impl<D> Default for Criteria<D>
where D: Deref<Target=DB>
{
    fn default() -> Self {
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
}

impl<D> AsRef<[Box<dyn Criterion<D>>]> for Criteria<D>
where D: Deref<Target=DB>
{
    fn as_ref(&self) -> &[Box<dyn Criterion<D>>] {
        &self.inner
    }
}
