//! BNF grammar:
//!
//! ```text
//! condition      = value ("==" | ">" ...) value
//! to             = value value TO value
//! ```

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::{cut, map, value};
use nom::sequence::{preceded, terminated, tuple};
use Condition::*;

use crate::error::IResultExt;
use crate::value::{parse_vector_value, parse_vector_value_cut};
use crate::{parse_value, Error, ErrorKind, FilterCondition, IResult, Span, Token, VectorFilter};

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

impl Condition<'_> {
    pub fn operator(&self) -> &str {
        match self {
            Condition::GreaterThan(_) => ">",
            Condition::GreaterThanOrEqual(_) => ">=",
            Condition::Equal(_) => "=",
            Condition::NotEqual(_) => "!=",
            Condition::Null => "IS NULL",
            Condition::Empty => "IS EMPTY",
            Condition::Exists => "EXISTS",
            Condition::LowerThan(_) => "<",
            Condition::LowerThanOrEqual(_) => "<=",
            Condition::Between { .. } => "TO",
            Condition::Contains { .. } => "CONTAINS",
            Condition::StartsWith { .. } => "STARTS WITH",
        }
    }
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

fn parse_vectors(input: Span) -> IResult<(Token, Option<Token>, VectorFilter)> {
    let (input, _) = multispace0(input)?;
    let (input, fid) = tag("_vectors")(input)?;

    if let Ok((input, _)) = multispace1::<_, crate::Error>(input) {
        return Ok((input, (Token::from(fid), None, VectorFilter::None)));
    }

    let (input, _) = char('.')(input)?;

    // From this point, we are certain this is a vector filter, so our errors must be final.
    // We could use nom's `cut` but it's better to be explicit about the errors

    if let Ok((_, space)) = tag::<_, _, ()>(" ")(input) {
        return Err(crate::Error::failure_from_kind(space, ErrorKind::VectorFilterMissingEmbedder));
    }

    let (input, embedder_name) =
        parse_vector_value_cut(input, ErrorKind::VectorFilterInvalidEmbedder)?;

    let (input, filter) = alt((
        map(
            preceded(tag(".fragments"), |input| {
                let (input, _) = tag(".")(input).map_cut(ErrorKind::VectorFilterMissingFragment)?;
                parse_vector_value_cut(input, ErrorKind::VectorFilterInvalidFragment)
            }),
            VectorFilter::Fragment,
        ),
        value(VectorFilter::UserProvided, tag(".userProvided")),
        value(VectorFilter::DocumentTemplate, tag(".documentTemplate")),
        value(VectorFilter::Regenerate, tag(".regenerate")),
        value(VectorFilter::None, nom::combinator::success("")),
    ))(input)?;

    if let Ok((input, point)) = tag::<_, _, ()>(".")(input) {
        let opt_value = parse_vector_value(input).ok().map(|(_, v)| v);
        let value =
            opt_value.as_ref().map(|v| v.value().to_owned()).unwrap_or_else(|| point.to_string());
        let context = opt_value.map(|v| v.original_span()).unwrap_or(point);
        let previous_kind = match filter {
            VectorFilter::Fragment(_) => Some("fragments"),
            VectorFilter::DocumentTemplate => Some("documentTemplate"),
            VectorFilter::UserProvided => Some("userProvided"),
            VectorFilter::Regenerate => Some("regenerate"),
            VectorFilter::None => None,
        };
        return Err(Error::failure_from_kind(
            context,
            ErrorKind::VectorFilterUnknownSuffix(previous_kind, value),
        ));
    }

    let (input, _) = multispace1(input).map_cut(ErrorKind::VectorFilterLeftover)?;

    Ok((input, (Token::from(fid), Some(embedder_name), filter)))
}

/// vectors_exists          = vectors ("EXISTS" | ("NOT" WS+ "EXISTS"))
pub fn parse_vectors_exists(input: Span) -> IResult<FilterCondition> {
    let (input, (fid, embedder, filter)) = parse_vectors(input)?;

    // Try parsing "EXISTS" first
    if let Ok((input, _)) = tag::<_, _, ()>("EXISTS")(input) {
        return Ok((input, FilterCondition::VectorExists { fid, embedder, filter }));
    }

    // Try parsing "NOT EXISTS"
    if let Ok((input, _)) = tuple::<_, _, (), _>((tag("NOT"), multispace1, tag("EXISTS")))(input) {
        return Ok((
            input,
            FilterCondition::Not(Box::new(FilterCondition::VectorExists { fid, embedder, filter })),
        ));
    }

    Err(crate::Error::failure_from_kind(input, ErrorKind::VectorFilterOperation))
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
