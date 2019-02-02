mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;
// mod sort_by;
mod document_id;

use std::cmp::Ordering;

use crate::database::DatabaseView;
use crate::rank::RawDocument;

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

pub trait Criterion<D>
where D: Deref<Target=DB>
{
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

#[derive(Default)]
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

    pub fn build(self) -> Criteria<D> {
        Criteria { inner: self.inner }
    }
}

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
