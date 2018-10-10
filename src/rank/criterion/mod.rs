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
    sum_of_typos::sum_of_typos,
    number_of_words::number_of_words,
    words_proximity::words_proximity,
    sum_of_words_attribute::sum_of_words_attribute,
    sum_of_words_position::sum_of_words_position,
    exact::exact,
};

#[inline]
pub fn document_id(lhs: &Document, rhs: &Document) -> Ordering {
    lhs.id.cmp(&rhs.id)
}

#[derive(Debug)]
pub struct Criteria<F>(Vec<F>);

impl<F> Criteria<F> {
    pub fn new() -> Self {
        Criteria(Vec::new())
    }

    pub fn with_capacity(cap: usize) -> Self {
        Criteria(Vec::with_capacity(cap))
    }

    pub fn push(&mut self, criterion: F) {
        self.0.push(criterion)
    }

    pub fn add(mut self, criterion: F) -> Self {
        self.push(criterion);
        self
    }
}

impl<F> IntoIterator for Criteria<F> {
    type Item = F;
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub fn default() -> Criteria<impl Fn(&Document, &Document) -> Ordering + Copy> {
    let functions = &[
        sum_of_typos,
        number_of_words,
        words_proximity,
        sum_of_words_attribute,
        sum_of_words_position,
        exact,
        document_id,
    ];

    let mut criteria = Criteria::with_capacity(functions.len());
    for f in functions { criteria.push(f) }
    criteria
}
