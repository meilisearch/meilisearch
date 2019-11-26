use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use crate::criterion::Criterion;
use crate::{RankedMap, RawDocument};
use meilisearch_schema::{Schema, SchemaAttr};

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
/// use meilisearch::rank::criterion::*;
///
/// let custom_ranking = SortByAttr::lower_is_better(&ranked_map, &schema, "published_at")?;
///
/// let builder = CriteriaBuilder::with_capacity(8)
///        .add(SumOfTypos)
///        .add(NumberOfWords)
///        .add(WordsProximity)
///        .add(SumOfWordsAttribute)
///        .add(SumOfWordsPosition)
///        .add(Exact)
///        .add(custom_ranking)
///        .add(DocumentId);
///
/// let criterion = builder.build();
///
/// ```
pub struct SortByAttr<'a> {
    ranked_map: &'a RankedMap,
    attr: SchemaAttr,
    reversed: bool,
}

impl<'a> SortByAttr<'a> {
    pub fn lower_is_better(
        ranked_map: &'a RankedMap,
        schema: &Schema,
        attr_name: &str,
    ) -> Result<SortByAttr<'a>, SortByAttrError> {
        SortByAttr::new(ranked_map, schema, attr_name, false)
    }

    pub fn higher_is_better(
        ranked_map: &'a RankedMap,
        schema: &Schema,
        attr_name: &str,
    ) -> Result<SortByAttr<'a>, SortByAttrError> {
        SortByAttr::new(ranked_map, schema, attr_name, true)
    }

    fn new(
        ranked_map: &'a RankedMap,
        schema: &Schema,
        attr_name: &str,
        reversed: bool,
    ) -> Result<SortByAttr<'a>, SortByAttrError> {
        let attr = match schema.attribute(attr_name) {
            Some(attr) => attr,
            None => return Err(SortByAttrError::AttributeNotFound),
        };

        if !schema.props(attr).is_ranked() {
            return Err(SortByAttrError::AttributeNotRegisteredForRanking);
        }

        Ok(SortByAttr {
            ranked_map,
            attr,
            reversed,
        })
    }
}

impl<'a> Criterion for SortByAttr<'a> {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = self.ranked_map.get(lhs.id, self.attr);
        let rhs = self.ranked_map.get(rhs.id, self.attr);

        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => {
                let order = lhs.cmp(&rhs);
                if self.reversed {
                    order.reverse()
                } else {
                    order
                }
            }
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }

    fn name(&self) -> &str {
        "SortByAttr"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortByAttrError {
    AttributeNotFound,
    AttributeNotRegisteredForRanking,
}

impl fmt::Display for SortByAttrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use SortByAttrError::*;
        match self {
            AttributeNotFound => f.write_str("attribute not found in the schema"),
            AttributeNotRegisteredForRanking => f.write_str("attribute not registered for ranking"),
        }
    }
}

impl Error for SortByAttrError {}
