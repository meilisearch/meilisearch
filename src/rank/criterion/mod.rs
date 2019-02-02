mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;
mod sort_by;
mod document_id;

use std::cmp::Ordering;
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

pub trait Criterion: Send + Sync {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering;

    #[inline]
    fn eq(&self, lhs: &RawDocument, rhs: &RawDocument) -> bool {
        self.evaluate(lhs, rhs) == Ordering::Equal
    }
}

impl<'a, T: Criterion + ?Sized + Send + Sync> Criterion for &'a T {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        (**self).evaluate(lhs, rhs)
    }

    fn eq(&self, lhs: &RawDocument, rhs: &RawDocument) -> bool {
        (**self).eq(lhs, rhs)
    }
}

impl<T: Criterion + ?Sized> Criterion for Box<T> {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        (**self).evaluate(lhs, rhs)
    }

    fn eq(&self, lhs: &RawDocument, rhs: &RawDocument) -> bool {
        (**self).eq(lhs, rhs)
    }
}

#[derive(Default)]
pub struct CriteriaBuilder {
    inner: Vec<Box<dyn Criterion>>
}

impl CriteriaBuilder
{
    pub fn new() -> CriteriaBuilder {
        CriteriaBuilder { inner: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> CriteriaBuilder {
        CriteriaBuilder { inner: Vec::with_capacity(capacity) }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional)
    }

    pub fn add<C>(mut self, criterion: C) -> CriteriaBuilder
    where C: 'static + Criterion,
    {
        self.push(criterion);
        self
    }

    pub fn push<C>(&mut self, criterion: C)
    where C: 'static + Criterion,
    {
        self.inner.push(Box::new(criterion));
    }

    pub fn build(self) -> Criteria {
        Criteria { inner: self.inner }
    }
}

pub struct Criteria {
    inner: Vec<Box<dyn Criterion>>,
}

impl Default for Criteria {
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

impl AsRef<[Box<dyn Criterion>]> for Criteria {
    fn as_ref(&self) -> &[Box<dyn Criterion>] {
        &self.inner
    }
}
