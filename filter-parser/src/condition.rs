//! BNF grammar:
//!
//! ```text
//! condition      = value ("==" | ">" ...) value
//! to             = value value TO value
//! ```

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::multispace1;
use nom::combinator::cut;
use nom::sequence::{terminated, tuple};
use Condition::*;

use crate::{parse_value, FilterCondition, IResult, Span, Token};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Condition<'a> {
    GreaterThan(Token<'a>),
    GreaterThanOrEqual(Token<'a>),
    Equal(Token<'a>),
    NotEqual(Token<'a>),
    Exists,
    LowerThan(Token<'a>),
    LowerThanOrEqual(Token<'a>),
    Between { from: Token<'a>, to: Token<'a> },
}

/// condition      = value ("==" | ">" ...) value
pub fn parse_condition(input: Span) -> IResult<FilterCondition> {
    let operator = alt((tag("<="), tag(">="), tag("!="), tag("<"), tag(">"), tag("=")));
    let (input, (fid, op, value)) = tuple((parse_value, operator, cut(parse_value)))(input)?;

    let condition = match *op.fragment() {
        "<=" => FilterCondition::Condition { fid, op: LowerThanOrEqual(value) },
        ">=" => FilterCondition::Condition { fid, op: GreaterThanOrEqual(value) },
        "!=" => FilterCondition::Condition { fid, op: NotEqual(value) },
        "<" => FilterCondition::Condition { fid, op: LowerThan(value) },
        ">" => FilterCondition::Condition { fid, op: GreaterThan(value) },
        "=" => FilterCondition::Condition { fid, op: Equal(value) },
        _ => unreachable!(),
    };

    Ok((input, condition))
}

/// exist          = value "EXISTS"
pub fn parse_exists(input: Span) -> IResult<FilterCondition> {
    let (input, key) = terminated(parse_value, tag("EXISTS"))(input)?;

    Ok((input, FilterCondition::Condition { fid: key.into(), op: Exists }))
}
/// exist          = value "NOT" WS+ "EXISTS"
pub fn parse_not_exists(input: Span) -> IResult<FilterCondition> {
    let (input, key) = parse_value(input)?;

    let (input, _) = tuple((tag("NOT"), multispace1, tag("EXISTS")))(input)?;
    Ok((
        input,
        FilterCondition::Not(Box::new(FilterCondition::Condition { fid: key.into(), op: Exists })),
    ))
}

/// to             = value value "TO" WS+ value
pub fn parse_to(input: Span) -> IResult<FilterCondition> {
    let (input, (key, from, _, _, to)) =
        tuple((parse_value, parse_value, tag("TO"), multispace1, cut(parse_value)))(input)?;

    Ok((input, FilterCondition::Condition { fid: key, op: Between { from, to } }))
}
