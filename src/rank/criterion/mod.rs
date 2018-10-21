mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;

use std::vec;
use std::cmp::Ordering;
use crate::rank::Document;

pub use self::{
    sum_of_typos::SumOfTypos,
    number_of_words::NumberOfWords,
    words_proximity::WordsProximity,
    sum_of_words_attribute::SumOfWordsAttribute,
    sum_of_words_position::SumOfWordsPosition,
    exact::Exact,
};

pub trait Criterion {
    #[inline]
    fn evaluate(&self, lhs: &Document, rhs: &Document) -> Ordering;

    #[inline]
    fn eq(&self, lhs: &Document, rhs: &Document) -> bool {
        self.evaluate(lhs, rhs) == Ordering::Equal
    }
}

impl<'a, T: Criterion + ?Sized> Criterion for &'a T {
    fn evaluate(&self, lhs: &Document, rhs: &Document) -> Ordering {
        (**self).evaluate(lhs, rhs)
    }

    fn eq(&self, lhs: &Document, rhs: &Document) -> bool {
        (**self).eq(lhs, rhs)
    }
}

impl<T: Criterion + ?Sized> Criterion for Box<T> {
    fn evaluate(&self, lhs: &Document, rhs: &Document) -> Ordering {
        (**self).evaluate(lhs, rhs)
    }

    fn eq(&self, lhs: &Document, rhs: &Document) -> bool {
        (**self).eq(lhs, rhs)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DocumentId;

impl Criterion for DocumentId {
    fn evaluate(&self, lhs: &Document, rhs: &Document) -> Ordering {
        lhs.id.cmp(&rhs.id)
    }
}

// TODO there is too much Box here, can we use
//      static references or static closures
pub fn default() -> Vec<Box<dyn Criterion>> {
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
