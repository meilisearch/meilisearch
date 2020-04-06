use lazy_static::lazy_static;
use pest::prec_climber::{Operator, Assoc, PrecClimber};

lazy_static! {
    pub static ref PREC_CLIMBER: PrecClimber<Rule> = {
        use Assoc::*;
        use Rule::*;
        pest::prec_climber::PrecClimber::new(vec![Operator::new(or, Left), Operator::new(and, Left)])
    };
}

#[derive(Parser)]
#[grammar = "filters/parser/grammar.pest"]
pub struct FilterParser;
