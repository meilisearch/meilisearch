use super::{FilterParser, Rule, PREC_CLIMBER};
use pest::{
    iterators::{Pair, Pairs},
    Parser,
};
use std::convert::From;
use std::fmt;
use meilisearch_core::Schema;

pub enum Query {
    Contains { field: String, value: String },
    IsEqual { field: String, value: String },
    IsLower { field: String, value: String },
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Contains { field, value } => write!(f, "{}:{}", field, value),
            _ => todo!(),
        }
    }
}

impl From<Pair<'_, Rule>> for Query {
    fn from(item: Pair<Rule>) -> Self {
        let mut items = item.into_inner();
        let key = items.next().unwrap();
        // do additional parsing here and get the correct query type
        let value = items.next().unwrap();
        Self::Contains {
            field: key.as_str().to_owned(),
            value: value.as_str().to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct Span(usize, usize);

impl Span {
    pub fn merge(&self, other: &Span) -> Self {
        let start = if self.0 > other.0 { other.0 } else { self.0 };

        let end = if self.0 < other.0 { other.0 } else { self.0 };
        Span(start, end)
    }
}

impl From<pest::Span<'_>> for Span {
    fn from(other: pest::Span<'_>) -> Self {
        Span(other.start(), other.end())
    }
}

#[derive(Debug)]
pub enum Operation {
    Query(Query, Span),
    Or(Box<Operation>, Box<Operation>, Span),
    And(Box<Operation>, Box<Operation>, Span),
    Not(Box<Operation>, Span),
}

impl Operation {
    pub fn as_span<'a>(&'a self) -> &'a Span {
        use Operation::*;
        match self {
            Query(_, span) | Or(_, _, span) | And(_, _, span) | Not(_, span) => span,
        }
    }
}

fn eval(expression: Pairs<Rule>) -> Operation {
    PREC_CLIMBER.climb(
        expression,
        |pair: Pair<Rule>| {
            let span = Span::from(pair.as_span());
            match pair.as_rule() {
                Rule::query =>  Operation::Query(Query::from(pair), span),
                Rule::prgm  => eval(pair.into_inner()),
                Rule::not => Operation::Not(Box::new(eval(pair.into_inner())), span),
                _ => unreachable!(),
            }
        },
        |lhs: Operation, op: Pair<Rule>, rhs: Operation| {
            let span = lhs.as_span().merge(rhs.as_span());
            match op.as_rule() {
                Rule::or => Operation::Or(Box::new(lhs), Box::new(rhs), span),
                Rule::and => Operation::And(Box::new(lhs), Box::new(rhs), span),
                _ => unreachable!(),
            }
        },
    )
}

impl Operation {
    pub fn parse_with_schema<T: AsRef<str>>(_expr: T, _schema: &Schema) -> Result<Self, Box<dyn std::error::Error>> {
        unimplemented!()
    }
}
