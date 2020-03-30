pub mod operation;

use lazy_static::lazy_static;
use pest::prec_climber::{Operator, Assoc, PrecClimber};

pub use operation::Operation;

lazy_static! {
    static ref PREC_CLIMBER: PrecClimber<Rule> = {
        use Assoc::*;
        use Rule::*;
        pest::prec_climber::PrecClimber::new(vec![Operator::new(or, Left), Operator::new(and, Left)])
    };
}

#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct FilterParser;
