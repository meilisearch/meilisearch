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
    Null,
    Empty,
    Exists,
    LowerThan(Token<'a>),
    LowerThanOrEqual(Token<'a>),
    Between { from: Token<'a>, to: Token<'a> },
    Contains { keyword: Token<'a>, word: Token<'a> },
    StartsWith { keyword: Token<'a>, word: Token<'a> },
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

/// null          = value "IS" WS+ "NULL"
pub fn parse_is_null(input: Span) -> IResult<FilterCondition> {
    let (input, key) = parse_value(input)?;

    let (input, _) = tuple((tag("IS"), multispace1, tag("NULL")))(input)?;
    Ok((input, FilterCondition::Condition { fid: key, op: Null }))
}

/// null          = value "IS" WS+ "NOT" WS+ "NULL"
pub fn parse_is_not_null(input: Span) -> IResult<FilterCondition> {
    let (input, key) = parse_value(input)?;

    let (input, _) = tuple((tag("IS"), multispace1, tag("NOT"), multispace1, tag("NULL")))(input)?;
    Ok((input, FilterCondition::Not(Box::new(FilterCondition::Condition { fid: key, op: Null }))))
}

/// empty          = value "IS" WS+ "EMPTY"
pub fn parse_is_empty(input: Span) -> IResult<FilterCondition> {
    let (input, key) = parse_value(input)?;

    let (input, _) = tuple((tag("IS"), multispace1, tag("EMPTY")))(input)?;
    Ok((input, FilterCondition::Condition { fid: key, op: Empty }))
}

/// empty          = value "IS" WS+ "NOT" WS+ "EMPTY"
pub fn parse_is_not_empty(input: Span) -> IResult<FilterCondition> {
    let (input, key) = parse_value(input)?;

    let (input, _) = tuple((tag("IS"), multispace1, tag("NOT"), multispace1, tag("EMPTY")))(input)?;
    Ok((input, FilterCondition::Not(Box::new(FilterCondition::Condition { fid: key, op: Empty }))))
}

/// exist          = value "EXISTS"
pub fn parse_exists(input: Span) -> IResult<FilterCondition> {
    let (input, key) = terminated(parse_value, tag("EXISTS"))(input)?;

    Ok((input, FilterCondition::Condition { fid: key, op: Exists }))
}
/// exist          = value "NOT" WS+ "EXISTS"
pub fn parse_not_exists(input: Span) -> IResult<FilterCondition> {
    let (input, key) = parse_value(input)?;

    let (input, _) = tuple((tag("NOT"), multispace1, tag("EXISTS")))(input)?;
    Ok((input, FilterCondition::Not(Box::new(FilterCondition::Condition { fid: key, op: Exists }))))
}

/// contains        = value "CONTAINS" value
pub fn parse_contains(input: Span) -> IResult<FilterCondition> {
    let (input, (fid, contains, value)) =
        tuple((parse_value, tag("CONTAINS"), cut(parse_value)))(input)?;
    Ok((
        input,
        FilterCondition::Condition {
            fid,
            op: Contains { keyword: Token { span: contains, value: None }, word: value },
        },
    ))
}

/// contains        = value "NOT" WS+ "CONTAINS" value
pub fn parse_not_contains(input: Span) -> IResult<FilterCondition> {
    let keyword = tuple((tag("NOT"), multispace1, tag("CONTAINS")));
    let (input, (fid, (_not, _spaces, contains), value)) =
        tuple((parse_value, keyword, cut(parse_value)))(input)?;

    Ok((
        input,
        FilterCondition::Not(Box::new(FilterCondition::Condition {
            fid,
            op: Contains { keyword: Token { span: contains, value: None }, word: value },
        })),
    ))
}

/// starts with        = value "CONTAINS" value
pub fn parse_starts_with(input: Span) -> IResult<FilterCondition> {
    let (input, (fid, starts_with, value)) =
        tuple((parse_value, tag("STARTS WITH"), cut(parse_value)))(input)?;
    Ok((
        input,
        FilterCondition::Condition {
            fid,
            op: StartsWith { keyword: Token { span: starts_with, value: None }, word: value },
        },
    ))
}

/// starts with        = value "NOT" WS+ "CONTAINS" value
pub fn parse_not_starts_with(input: Span) -> IResult<FilterCondition> {
    let keyword = tuple((tag("NOT"), multispace1, tag("STARTS WITH")));
    let (input, (fid, (_not, _spaces, starts_with), value)) =
        tuple((parse_value, keyword, cut(parse_value)))(input)?;

    Ok((
        input,
        FilterCondition::Not(Box::new(FilterCondition::Condition {
            fid,
            op: StartsWith { keyword: Token { span: starts_with, value: None }, word: value },
        })),
    ))
}

/// to             = value value "TO" WS+ value
pub fn parse_to(input: Span) -> IResult<FilterCondition> {
    let (input, (key, from, _, _, to)) =
        tuple((parse_value, parse_value, tag("TO"), multispace1, cut(parse_value)))(input)?;

    Ok((input, FilterCondition::Condition { fid: key, op: Between { from, to } }))
}
