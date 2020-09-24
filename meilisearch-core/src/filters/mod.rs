mod parser;
mod condition;

pub(crate) use parser::Rule;

use std::ops::Not;

use condition::Condition;
use crate::error::Error;
use crate::{DocumentId, MainT, store::Index};
use heed::RoTxn;
use meilisearch_schema::Schema;
use parser::{PREC_CLIMBER, FilterParser};
use pest::iterators::{Pair, Pairs};
use pest::Parser;

type FilterResult<'a> = Result<Filter<'a>, Error>;

#[derive(Debug)]
pub enum Filter<'a> {
    Condition(Condition<'a>),
    Or(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

impl<'a> Filter<'a> {
    pub fn parse(expr: &'a str, schema: &'a Schema) -> FilterResult<'a> {
        let mut lexed = FilterParser::parse(Rule::prgm, expr)?;
        Self::build(lexed.next().unwrap().into_inner(), schema)
    }

    pub fn test(
        &self,
        reader: &RoTxn<MainT>,
        index: &Index,
        document_id: DocumentId,
    ) -> Result<bool, Error> {
        use Filter::*;
        match self {
            Condition(c) => c.test(reader, index, document_id),
            Or(lhs, rhs) => Ok(
                lhs.test(reader, index, document_id)? || rhs.test(reader, index, document_id)?
            ),
            And(lhs, rhs) => Ok(
                lhs.test(reader, index, document_id)? && rhs.test(reader, index, document_id)?
            ),
            Not(op) => op.test(reader, index, document_id).map(bool::not),
        }
    }

    fn build(expression: Pairs<'a, Rule>, schema: &'a Schema) -> FilterResult<'a> {
        PREC_CLIMBER.climb(
            expression,
            |pair: Pair<Rule>| match pair.as_rule() {
                Rule::eq => Ok(Filter::Condition(Condition::eq(pair, schema)?)),
                Rule::greater => Ok(Filter::Condition(Condition::greater(pair, schema)?)),
                Rule::less => Ok(Filter::Condition(Condition::less(pair, schema)?)),
                Rule::neq => Ok(Filter::Condition(Condition::neq(pair, schema)?)),
                Rule::geq => Ok(Filter::Condition(Condition::geq(pair, schema)?)),
                Rule::leq => Ok(Filter::Condition(Condition::leq(pair, schema)?)),
                Rule::prgm => Self::build(pair.into_inner(), schema),
                Rule::term => Self::build(pair.into_inner(), schema),
                Rule::not => Ok(Filter::Not(Box::new(Self::build(
                    pair.into_inner(),
                    schema,
                )?))),
                _ => unreachable!(),
            },
            |lhs: FilterResult, op: Pair<Rule>, rhs: FilterResult| match op.as_rule() {
                Rule::or => Ok(Filter::Or(Box::new(lhs?), Box::new(rhs?))),
                Rule::and => Ok(Filter::And(Box::new(lhs?), Box::new(rhs?))),
                _ => unreachable!(),
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn invalid_syntax() {
        assert!(FilterParser::parse(Rule::prgm, "field : id").is_err());
        assert!(FilterParser::parse(Rule::prgm, "field=hello hello").is_err());
        assert!(FilterParser::parse(Rule::prgm, "field=hello OR OR").is_err());
        assert!(FilterParser::parse(Rule::prgm, "OR field:hello").is_err());
        assert!(FilterParser::parse(Rule::prgm, r#"field="hello world"#).is_err());
        assert!(FilterParser::parse(Rule::prgm, r#"field='hello world"#).is_err());
        assert!(FilterParser::parse(Rule::prgm, "NOT field=").is_err());
        assert!(FilterParser::parse(Rule::prgm, "N").is_err());
        assert!(FilterParser::parse(Rule::prgm, "(field=1").is_err());
        assert!(FilterParser::parse(Rule::prgm, "(field=1))").is_err());
        assert!(FilterParser::parse(Rule::prgm, "field=1ORfield=2").is_err());
        assert!(FilterParser::parse(Rule::prgm, "field=1 ( OR field=2)").is_err());
        assert!(FilterParser::parse(Rule::prgm, "hello world=1").is_err());
        assert!(FilterParser::parse(Rule::prgm, "").is_err());
        assert!(FilterParser::parse(Rule::prgm, r#"((((((hello=world)))))"#).is_err());
    }

    #[test]
    fn valid_syntax() {
        assert!(FilterParser::parse(Rule::prgm, "field = id").is_ok());
        assert!(FilterParser::parse(Rule::prgm, "field=id").is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field >= 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field <= 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field="hello world""#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field='hello world'"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field > 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field < 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field < 10 AND NOT field=5"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field < 10 AND NOT field > 7.5"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field=true OR NOT field=5"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"NOT field=true OR NOT field=5"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field='hello world' OR ( NOT field=true OR NOT field=5 )"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field='hello \'worl\'d' OR ( NOT field=true OR NOT field=5 )"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"field="hello \"worl\"d" OR ( NOT field=true OR NOT field=5 )"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"((((((hello=world))))))"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#""foo bar" > 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#""foo bar" = 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"'foo bar' = 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"'foo bar' <= 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"'foo bar' != 10"#).is_ok());
        assert!(FilterParser::parse(Rule::prgm, r#"bar != 10"#).is_ok());
    }
}
