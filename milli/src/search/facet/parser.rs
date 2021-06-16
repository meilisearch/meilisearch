use once_cell::sync::Lazy;
use pest::prec_climber::{Assoc, Operator, PrecClimber};

pub static PREC_CLIMBER: Lazy<PrecClimber<Rule>> = Lazy::new(|| {
    use Assoc::*;
    use Rule::*;
    pest::prec_climber::PrecClimber::new(vec![Operator::new(or, Left), Operator::new(and, Left)])
});

#[derive(Parser)]
#[grammar = "search/facet/grammar.pest"]
pub struct FilterParser;
