mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;

use std::cmp::Ordering;
use std::ops::Deref;
use std::vec;

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

// TODO there is too much Box here, can we use
//      static references or static closures
pub fn default<D>() -> Vec<Box<dyn Criterion<D>>>
where D: Deref<Target=DB>
{
    vec![
        Box::new(SumOfTypos),
        Box::new(NumberOfWords),
        Box::new(WordsProximity),
        Box::new(SumOfWordsAttribute),
        Box::new(SumOfWordsPosition),
        Box::new(Exact),
        Box::new(DocumentId),
    ]
}
