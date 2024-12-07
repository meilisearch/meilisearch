//! BNF grammar:
//!
//! ```text
//! filter         = expression EOF
//! expression     = or
//! or             = and ("OR" WS+ and)*
//! and            = not ("AND" WS+ not)*
//! not            = ("NOT" WS+ not) | primary
//! primary        = (WS* "(" WS* expression WS* ")" WS*) | geoRadius | in | condition | exists | not_exists | to
//! in             = value "IN" WS* "[" value_list "]"
//! condition      = value ("=" | "!=" | ">" | ">=" | "<" | "<=") value
//! exists         = value "EXISTS"
//! not_exists     = value "NOT" WS+ "EXISTS"
//! to             = value value "TO" WS+ value
//! value          = WS* ( word | singleQuoted | doubleQuoted) WS+
//! value_list     = (value ("," value)* ","?)?
//! singleQuoted   = "'" .* all but quotes "'"
//! doubleQuoted   = "\"" .* all but double quotes "\""
//! word           = (alphanumeric | _ | - | .)+
//! geoRadius      = "_geoRadius(" WS* float WS* "," WS* float WS* "," float WS* ")"
//! geoBoundingBox = "_geoBoundingBox([" WS * float WS* "," WS* float WS* "], [" WS* float WS* "," WS* float WS* "]")
//! ```
//!
//! Other BNF grammar used to handle some specific errors:
//! ```text
//! geoPoint       = WS* "_geoPoint(" (float ",")* ")"
//! ```
//!
//! Specific errors:
//! ================
//! - If a user try to use a geoPoint, as a primary OR as a value we must throw an error.
//! ```text
//! field = _geoPoint(12, 13, 14)
//! field < 12 AND _geoPoint(1, 2)
//! ```
//!
//! - If a user try to use a geoRadius as a value we must throw an error.
//! ```text
//! field = _geoRadius(12, 13, 14)
//! ```
//!

mod condition;
mod error;
mod value;

use std::fmt::Debug;

pub use condition::{parse_condition, parse_to, Condition};
use condition::{
    parse_contains, parse_exists, parse_is_empty, parse_is_not_empty, parse_is_not_null,
    parse_is_null, parse_not_contains, parse_not_exists, parse_not_starts_with, parse_starts_with,
};
use error::{cut_with_err, ExpectedValueKind, NomErrorExt};
pub use error::{Error, ErrorKind};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, multispace0};
use nom::combinator::{cut, eof, map, opt};
use nom::multi::{many0, separated_list1};
use nom::number::complete::recognize_float;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::Finish;
use nom_locate::LocatedSpan;
pub(crate) use value::parse_value;
use value::word_exact;

pub type Span<'a> = LocatedSpan<&'a str, &'a str>;

type IResult<'a, Ret> = nom::IResult<Span<'a>, Ret, Error<'a>>;

const MAX_FILTER_DEPTH: usize = 200;

#[derive(Debug, Clone, Eq)]
pub struct Token<'a> {
    /// The token in the original input, it should be used when possible.
    span: Span<'a>,
    /// If you need to modify the original input you can use the `value` field
    /// to store your modified input.
    value: Option<String>,
}

impl<'a> PartialEq for Token<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.span.fragment() == other.span.fragment()
    }
}

impl<'a> Token<'a> {
    pub fn new(span: Span<'a>, value: Option<String>) -> Self {
        Self { span, value }
    }

    /// Returns the string contained in the span of the `Token`.
    /// This is only useful in the tests. You should always use
    /// the value.
    #[cfg(test)]
    pub fn lexeme(&self) -> &str {
        &self.span
    }

    /// Return the string contained in the token.
    pub fn value(&self) -> &str {
        self.value.as_ref().map_or(&self.span, |value| value)
    }

    pub fn as_external_error(&self, error: impl std::error::Error) -> Error<'a> {
        Error::new_from_external(self.span, error)
    }

    /// Returns a copy of the span this token was created with.
    pub fn original_span(&self) -> Span<'a> {
        self.span
    }

    pub fn parse_finite_float(&self) -> Result<f64, Error> {
        let value: f64 = self.value().parse().map_err(|e| self.as_external_error(e))?;
        if value.is_finite() {
            Ok(value)
        } else {
            Err(Error::new_from_kind(self.span, ErrorKind::NonFiniteFloat))
        }
    }
}

impl<'a> From<Span<'a>> for Token<'a> {
    fn from(span: Span<'a>) -> Self {
        Self { span, value: None }
    }
}

/// Allow [Token] to be constructed from &[str]
impl<'a> From<&'a str> for Token<'a> {
    fn from(s: &'a str) -> Self {
        Token::from(Span::new_extra(s, s))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterCondition<'a> {
    Not(Box<Self>),
    Condition { fid: Token<'a>, op: Condition<'a> },
    In { fid: Token<'a>, els: Vec<Token<'a>> },
    Or(Vec<Self>),
    And(Vec<Self>),
    GeoLowerThan { point: [Token<'a>; 2], radius: Token<'a> },
    GeoBoundingBox { top_right_point: [Token<'a>; 2], bottom_left_point: [Token<'a>; 2] },
}

pub enum TraversedElement<'a> {
    FilterCondition(&'a FilterCondition<'a>),
    Condition(&'a Condition<'a>),
}

impl<'a> FilterCondition<'a> {
    pub fn use_contains_operator(&self) -> Option<&Token> {
        match self {
            FilterCondition::Condition { fid: _, op } => match op {
                Condition::GreaterThan(_)
                | Condition::GreaterThanOrEqual(_)
                | Condition::Equal(_)
                | Condition::NotEqual(_)
                | Condition::Null
                | Condition::Empty
                | Condition::Exists
                | Condition::LowerThan(_)
                | Condition::LowerThanOrEqual(_)
                | Condition::Between { .. } => None,
                Condition::Contains { keyword, word: _ }
                | Condition::StartsWith { keyword, word: _ } => Some(keyword),
            },
            FilterCondition::Not(this) => this.use_contains_operator(),
            FilterCondition::Or(seq) | FilterCondition::And(seq) => {
                seq.iter().find_map(|filter| filter.use_contains_operator())
            }
            FilterCondition::GeoLowerThan { .. }
            | FilterCondition::GeoBoundingBox { .. }
            | FilterCondition::In { .. } => None,
        }
    }

    pub fn fids(&self, depth: usize) -> Box<dyn Iterator<Item = &Token> + '_> {
        if depth == 0 {
            return Box::new(std::iter::empty());
        }
        match self {
            FilterCondition::Condition { fid, .. } | FilterCondition::In { fid, .. } => {
                Box::new(std::iter::once(fid))
            }
            FilterCondition::Not(filter) => {
                let depth = depth.saturating_sub(1);
                filter.fids(depth)
            }
            FilterCondition::And(subfilters) | FilterCondition::Or(subfilters) => {
                let depth = depth.saturating_sub(1);
                Box::new(subfilters.iter().flat_map(move |f| f.fids(depth)))
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    /// Returns the first token found at the specified depth, `None` if no token at this depth.
    pub fn token_at_depth(&self, depth: usize) -> Option<&Token> {
        match self {
            FilterCondition::Condition { fid, .. } if depth == 0 => Some(fid),
            FilterCondition::Or(subfilters) => {
                let depth = depth.saturating_sub(1);
                for f in subfilters.iter() {
                    if let Some(t) = f.token_at_depth(depth) {
                        return Some(t);
                    }
                }
                None
            }
            FilterCondition::And(subfilters) => {
                let depth = depth.saturating_sub(1);
                for f in subfilters.iter() {
                    if let Some(t) = f.token_at_depth(depth) {
                        return Some(t);
                    }
                }
                None
            }
            FilterCondition::GeoLowerThan { point: [point, _], .. } if depth == 0 => Some(point),
            _ => None,
        }
    }

    pub fn parse(input: &'a str) -> Result<Option<Self>, Error> {
        if input.trim().is_empty() {
            return Ok(None);
        }
        let span = Span::new_extra(input, input);
        parse_filter(span).finish().map(|(_rem, output)| Some(output))
    }
}

/// remove OPTIONAL whitespaces before AND after the provided parser.
fn ws<'a, O>(
    inner: impl FnMut(Span<'a>) -> IResult<'a, O>,
) -> impl FnMut(Span<'a>) -> IResult<'a, O> {
    delimited(multispace0, inner, multispace0)
}

/// value_list = (value ("," value)* ","?)?
fn parse_value_list(input: Span) -> IResult<Vec<Token>> {
    let (input, first_value) = opt(parse_value)(input)?;
    if let Some(first_value) = first_value {
        let value_list_el_parser = preceded(ws(tag(",")), parse_value);

        let (input, mut values) = many0(value_list_el_parser)(input)?;
        let (input, _) = opt(ws(tag(",")))(input)?;
        values.insert(0, first_value);

        Ok((input, values))
    } else {
        Ok((input, vec![]))
    }
}

/// "IN" WS* "[" value_list "]"
fn parse_in_body(input: Span) -> IResult<Vec<Token>> {
    let (input, _) = ws(word_exact("IN"))(input)?;

    // everything after `IN` can be a failure
    let (input, _) =
        cut_with_err(tag("["), |_| Error::new_from_kind(input, ErrorKind::InOpeningBracket))(
            input,
        )?;

    let (input, content) = cut(parse_value_list)(input)?;

    // everything after `IN` can be a failure
    let (input, _) = cut_with_err(ws(tag("]")), |_| {
        if eof::<_, ()>(input).is_ok() {
            Error::new_from_kind(input, ErrorKind::InClosingBracket)
        } else {
            let expected_value_kind = match parse_value(input) {
                Err(nom::Err::Error(e)) => match e.kind() {
                    ErrorKind::ReservedKeyword(_) => ExpectedValueKind::ReservedKeyword,
                    _ => ExpectedValueKind::Other,
                },
                _ => ExpectedValueKind::Other,
            };
            Error::new_from_kind(input, ErrorKind::InExpectedValue(expected_value_kind))
        }
    })(input)?;

    Ok((input, content))
}

/// in = value "IN" "[" value_list "]"
fn parse_in(input: Span) -> IResult<FilterCondition> {
    let (input, value) = parse_value(input)?;
    let (input, content) = parse_in_body(input)?;

    let filter = FilterCondition::In { fid: value, els: content };
    Ok((input, filter))
}

/// in = value "NOT" WS* "IN" "[" value_list "]"
fn parse_not_in(input: Span) -> IResult<FilterCondition> {
    let (input, value) = parse_value(input)?;
    let (input, _) = word_exact("NOT")(input)?;
    let (input, content) = parse_in_body(input)?;

    let filter = FilterCondition::Not(Box::new(FilterCondition::In { fid: value, els: content }));
    Ok((input, filter))
}

/// or             = and ("OR" and)
fn parse_or(input: Span, depth: usize) -> IResult<FilterCondition> {
    if depth > MAX_FILTER_DEPTH {
        return Err(nom::Err::Error(Error::new_from_kind(input, ErrorKind::DepthLimitReached)));
    }
    let (input, first_filter) = parse_and(input, depth + 1)?;
    // if we found a `OR` then we MUST find something next
    let (input, mut ors) =
        many0(preceded(ws(word_exact("OR")), cut(|input| parse_and(input, depth + 1))))(input)?;

    let filter = if ors.is_empty() {
        first_filter
    } else {
        ors.insert(0, first_filter);
        FilterCondition::Or(ors)
    };

    Ok((input, filter))
}

/// and            = not ("AND" not)*
fn parse_and(input: Span, depth: usize) -> IResult<FilterCondition> {
    if depth > MAX_FILTER_DEPTH {
        return Err(nom::Err::Error(Error::new_from_kind(input, ErrorKind::DepthLimitReached)));
    }
    let (input, first_filter) = parse_not(input, depth + 1)?;
    // if we found a `AND` then we MUST find something next
    let (input, mut ands) =
        many0(preceded(ws(word_exact("AND")), cut(|input| parse_not(input, depth + 1))))(input)?;

    let filter = if ands.is_empty() {
        first_filter
    } else {
        ands.insert(0, first_filter);
        FilterCondition::And(ands)
    };

    Ok((input, filter))
}

/// not            = ("NOT" WS+ not) | primary
/// We can have multiple consecutive not, eg: `NOT NOT channel = mv`.
/// If we parse a `NOT` we MUST parse something behind.
fn parse_not(input: Span, depth: usize) -> IResult<FilterCondition> {
    if depth > MAX_FILTER_DEPTH {
        return Err(nom::Err::Error(Error::new_from_kind(input, ErrorKind::DepthLimitReached)));
    }
    alt((
        map(
            preceded(ws(word_exact("NOT")), cut(|input| parse_not(input, depth + 1))),
            |e| match e {
                FilterCondition::Not(e) => *e,
                _ => FilterCondition::Not(Box::new(e)),
            },
        ),
        |input| parse_primary(input, depth + 1),
    ))(input)
}

/// geoRadius      = WS* "_geoRadius(float WS* "," WS* float WS* "," WS* float)
/// If we parse `_geoRadius` we MUST parse the rest of the expression.
fn parse_geo_radius(input: Span) -> IResult<FilterCondition> {
    // we want to allow space BEFORE the _geoRadius but not after
    let parsed = preceded(
        tuple((multispace0, word_exact("_geoRadius"))),
        // if we were able to parse `_geoRadius` and can't parse the rest of the input we return a failure
        cut(delimited(char('('), separated_list1(tag(","), ws(recognize_float)), char(')'))),
    )(input)
    .map_err(|e| e.map(|_| Error::new_from_kind(input, ErrorKind::GeoRadius)));

    let (input, args) = parsed?;

    if args.len() != 3 {
        return Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::GeoRadius)));
    }

    let res = FilterCondition::GeoLowerThan {
        point: [args[0].into(), args[1].into()],
        radius: args[2].into(),
    };
    Ok((input, res))
}

/// geoBoundingBox      = WS* "_geoBoundingBox([float WS* "," WS* float WS* "], [float WS* "," WS* float WS* "]")
/// If we parse `_geoBoundingBox` we MUST parse the rest of the expression.
fn parse_geo_bounding_box(input: Span) -> IResult<FilterCondition> {
    // we want to allow space BEFORE the _geoBoundingBox but not after
    let parsed = preceded(
        tuple((multispace0, word_exact("_geoBoundingBox"))),
        // if we were able to parse `_geoBoundingBox` and can't parse the rest of the input we return a failure
        cut(delimited(
            char('('),
            separated_list1(
                tag(","),
                ws(delimited(char('['), separated_list1(tag(","), ws(recognize_float)), char(']'))),
            ),
            char(')'),
        )),
    )(input)
    .map_err(|e| e.map(|_| Error::new_from_kind(input, ErrorKind::GeoBoundingBox)));

    let (input, args) = parsed?;

    if args.len() != 2 || args[0].len() != 2 || args[1].len() != 2 {
        return Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::GeoBoundingBox)));
    }

    let res = FilterCondition::GeoBoundingBox {
        top_right_point: [args[0][0].into(), args[0][1].into()],
        bottom_left_point: [args[1][0].into(), args[1][1].into()],
    };
    Ok((input, res))
}

/// geoPoint      = WS* "_geoPoint(float WS* "," WS* float WS* "," WS* float)
fn parse_geo_point(input: Span) -> IResult<FilterCondition> {
    // we want to forbid space BEFORE the _geoPoint but not after
    tuple((
        multispace0,
        tag("_geoPoint"),
        // if we were able to parse `_geoPoint` we are going to return a Failure whatever happens next.
        cut(delimited(char('('), separated_list1(tag(","), ws(recognize_float)), char(')'))),
    ))(input)
    .map_err(|e| e.map(|_| Error::new_from_kind(input, ErrorKind::ReservedGeo("_geoPoint"))))?;
    // if we succeeded we still return a `Failure` because geoPoints are not allowed
    Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::ReservedGeo("_geoPoint"))))
}

/// geoPoint      = WS* "_geoDistance(float WS* "," WS* float WS* "," WS* float)
fn parse_geo_distance(input: Span) -> IResult<FilterCondition> {
    // we want to forbid space BEFORE the _geoDistance but not after
    tuple((
        multispace0,
        tag("_geoDistance"),
        // if we were able to parse `_geoDistance` we are going to return a Failure whatever happens next.
        cut(delimited(char('('), separated_list1(tag(","), ws(recognize_float)), char(')'))),
    ))(input)
    .map_err(|e| e.map(|_| Error::new_from_kind(input, ErrorKind::ReservedGeo("_geoDistance"))))?;
    // if we succeeded we still return a `Failure` because `geoDistance` filters are not allowed
    Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::ReservedGeo("_geoDistance"))))
}

/// geo      = WS* "_geo(float WS* "," WS* float WS* "," WS* float)
fn parse_geo(input: Span) -> IResult<FilterCondition> {
    // we want to forbid space BEFORE the _geo but not after
    tuple((
        multispace0,
        word_exact("_geo"),
        // if we were able to parse `_geo` we are going to return a Failure whatever happens next.
        cut(delimited(char('('), separated_list1(tag(","), ws(recognize_float)), char(')'))),
    ))(input)
    .map_err(|e| e.map(|_| Error::new_from_kind(input, ErrorKind::ReservedGeo("_geo"))))?;
    // if we succeeded we still return a `Failure` because `_geo` filter is not allowed
    Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::ReservedGeo("_geo"))))
}

fn parse_error_reserved_keyword(input: Span) -> IResult<FilterCondition> {
    match parse_condition(input) {
        Ok(result) => Ok(result),
        Err(nom::Err::Error(inner) | nom::Err::Failure(inner)) => match inner.kind() {
            ErrorKind::ExpectedValue(ExpectedValueKind::ReservedKeyword) => {
                Err(nom::Err::Failure(inner))
            }
            _ => Err(nom::Err::Error(inner)),
        },
        Err(e) => Err(e),
    }
}

/// primary        = (WS* "(" WS* expression WS* ")" WS*) | geoRadius | condition | exists | not_exists | to
fn parse_primary(input: Span, depth: usize) -> IResult<FilterCondition> {
    if depth > MAX_FILTER_DEPTH {
        return Err(nom::Err::Error(Error::new_from_kind(input, ErrorKind::DepthLimitReached)));
    }
    alt((
        // if we find a first parenthesis, then we must parse an expression and find the closing parenthesis
        delimited(
            ws(char('(')),
            cut(|input| parse_expression(input, depth + 1)),
            cut_with_err(ws(char(')')), |c| {
                Error::new_from_kind(input, ErrorKind::MissingClosingDelimiter(c.char()))
            }),
        ),
        parse_geo_radius,
        parse_geo_bounding_box,
        parse_in,
        parse_not_in,
        parse_condition,
        parse_is_null,
        parse_is_not_null,
        parse_is_empty,
        parse_is_not_empty,
        parse_exists,
        parse_not_exists,
        parse_to,
        parse_contains,
        parse_not_contains,
        parse_starts_with,
        parse_not_starts_with,
        // the next lines are only for error handling and are written at the end to have the less possible performance impact
        parse_geo,
        parse_geo_distance,
        parse_geo_point,
        parse_error_reserved_keyword,
    ))(input)
    // if the inner parsers did not match enough information to return an accurate error
    .map_err(|e| e.map_err(|_| Error::new_from_kind(input, ErrorKind::InvalidPrimary)))
}

/// expression     = or
pub fn parse_expression(input: Span, depth: usize) -> IResult<FilterCondition> {
    parse_or(input, depth)
}

/// filter     = expression EOF
pub fn parse_filter(input: Span) -> IResult<FilterCondition> {
    terminated(|input| parse_expression(input, 0), eof)(input)
}

impl<'a> std::fmt::Display for FilterCondition<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterCondition::Not(filter) => {
                write!(f, "NOT ({filter})")
            }
            FilterCondition::Condition { fid, op } => {
                write!(f, "{fid} {op}")
            }
            FilterCondition::In { fid, els } => {
                write!(f, "{fid} IN[")?;
                for el in els {
                    write!(f, "{el}, ")?;
                }
                write!(f, "]")
            }
            FilterCondition::Or(els) => {
                write!(f, "OR[")?;
                for el in els {
                    write!(f, "{el}, ")?;
                }
                write!(f, "]")
            }
            FilterCondition::And(els) => {
                write!(f, "AND[")?;
                for el in els {
                    write!(f, "{el}, ")?;
                }
                write!(f, "]")
            }
            FilterCondition::GeoLowerThan { point, radius } => {
                write!(f, "_geoRadius({}, {}, {})", point[0], point[1], radius)
            }
            FilterCondition::GeoBoundingBox {
                top_right_point: top_left_point,
                bottom_left_point: bottom_right_point,
            } => {
                write!(
                    f,
                    "_geoBoundingBox([{}, {}], [{}, {}])",
                    top_left_point[0],
                    top_left_point[1],
                    bottom_right_point[0],
                    bottom_right_point[1]
                )
            }
        }
    }
}
impl<'a> std::fmt::Display for Condition<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Condition::GreaterThan(token) => write!(f, "> {token}"),
            Condition::GreaterThanOrEqual(token) => write!(f, ">= {token}"),
            Condition::Equal(token) => write!(f, "= {token}"),
            Condition::NotEqual(token) => write!(f, "!= {token}"),
            Condition::Null => write!(f, "IS NULL"),
            Condition::Empty => write!(f, "IS EMPTY"),
            Condition::Exists => write!(f, "EXISTS"),
            Condition::LowerThan(token) => write!(f, "< {token}"),
            Condition::LowerThanOrEqual(token) => write!(f, "<= {token}"),
            Condition::Between { from, to } => write!(f, "{from} TO {to}"),
            Condition::Contains { word, keyword: _ } => write!(f, "CONTAINS {word}"),
            Condition::StartsWith { word, keyword: _ } => write!(f, "STARTS WITH {word}"),
        }
    }
}
impl<'a> std::fmt::Display for Token<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}}}", self.value())
    }
}

#[cfg(test)]
pub mod tests {
    use FilterCondition as Fc;

    use super::*;

    /// Create a raw [Token]. You must specify the string that appear BEFORE your element followed by your element
    pub fn rtok<'a>(before: &'a str, value: &'a str) -> Token<'a> {
        // if the string is empty we still need to return 1 for the line number
        let lines = before.is_empty().then_some(1).unwrap_or_else(|| before.lines().count());
        let offset = before.chars().count();
        // the extra field is not checked in the tests so we can set it to nothing
        unsafe { Span::new_from_raw_offset(offset, lines as u32, value, "") }.into()
    }

    #[track_caller]
    fn p(s: &str) -> impl std::fmt::Display + '_ {
        Fc::parse(s).unwrap().unwrap()
    }

    #[test]
    fn parse_escaped() {
        insta::assert_snapshot!(p(r"title = 'foo\\'"), @r#"{title} = {foo\}"#);
        insta::assert_snapshot!(p(r"title = 'foo\\\\'"), @r#"{title} = {foo\\}"#);
        insta::assert_snapshot!(p(r"title = 'foo\\\\\\'"), @r#"{title} = {foo\\\}"#);
        insta::assert_snapshot!(p(r"title = 'foo\\\\\\\\'"), @r#"{title} = {foo\\\\}"#);
        // but it also works with other sequences
        insta::assert_snapshot!(p(r#"title = 'foo\x20\n\t\"\'"'"#), @"{title} = {foo \n\t\"\'\"}");
    }

    #[test]
    fn parse() {
        // Test equal
        insta::assert_snapshot!(p("channel = Ponce"), @"{channel} = {Ponce}");
        insta::assert_snapshot!(p("subscribers = 12"), @"{subscribers} = {12}");
        insta::assert_snapshot!(p("channel = 'Mister Mv'"), @"{channel} = {Mister Mv}");
        insta::assert_snapshot!(p("channel = \"Mister Mv\""), @"{channel} = {Mister Mv}");
        insta::assert_snapshot!(p("'dog race' = Borzoi"), @"{dog race} = {Borzoi}");
        insta::assert_snapshot!(p("\"dog race\" = Chusky"), @"{dog race} = {Chusky}");
        insta::assert_snapshot!(p("\"dog race\" = \"Bernese Mountain\""), @"{dog race} = {Bernese Mountain}");
        insta::assert_snapshot!(p("'dog race' = 'Bernese Mountain'"), @"{dog race} = {Bernese Mountain}");
        insta::assert_snapshot!(p("\"dog race\" = 'Bernese Mountain'"), @"{dog race} = {Bernese Mountain}");

        // Test IN
        insta::assert_snapshot!(p("colour IN[]"), @"{colour} IN[]");
        insta::assert_snapshot!(p("colour IN[green]"), @"{colour} IN[{green}, ]");
        insta::assert_snapshot!(p("colour IN[green,]"), @"{colour} IN[{green}, ]");
        insta::assert_snapshot!(p("colour NOT IN[green,blue]"), @"NOT ({colour} IN[{green}, {blue}, ])");
        insta::assert_snapshot!(p(" colour IN [  green , blue , ]"), @"{colour} IN[{green}, {blue}, ]");

        // Test IN + OR/AND/()
        insta::assert_snapshot!(p(" colour IN [green, blue]  AND color = green "), @"AND[{colour} IN[{green}, {blue}, ], {color} = {green}, ]");
        insta::assert_snapshot!(p("NOT (colour IN [green, blue])  AND color = green "), @"AND[NOT ({colour} IN[{green}, {blue}, ]), {color} = {green}, ]");
        insta::assert_snapshot!(p("x = 1 OR NOT (colour IN [green, blue]  OR color = green) "), @"OR[{x} = {1}, NOT (OR[{colour} IN[{green}, {blue}, ], {color} = {green}, ]), ]");

        // Test whitespace start/end
        insta::assert_snapshot!(p(" colour = green "), @"{colour} = {green}");
        insta::assert_snapshot!(p(" (colour = green OR colour = red) "), @"OR[{colour} = {green}, {colour} = {red}, ]");
        insta::assert_snapshot!(p(" colour IN [green, blue]  AND color = green "), @"AND[{colour} IN[{green}, {blue}, ], {color} = {green}, ]");
        insta::assert_snapshot!(p(" colour NOT  IN [green, blue] "), @"NOT ({colour} IN[{green}, {blue}, ])");
        insta::assert_snapshot!(p(" colour IN [green, blue] "), @"{colour} IN[{green}, {blue}, ]");

        // Test conditions
        insta::assert_snapshot!(p("channel != ponce"), @"{channel} != {ponce}");
        insta::assert_snapshot!(p("NOT channel = ponce"), @"NOT ({channel} = {ponce})");
        insta::assert_snapshot!(p("subscribers < 1000"), @"{subscribers} < {1000}");
        insta::assert_snapshot!(p("subscribers > 1000"), @"{subscribers} > {1000}");
        insta::assert_snapshot!(p("subscribers <= 1000"), @"{subscribers} <= {1000}");
        insta::assert_snapshot!(p("subscribers >= 1000"), @"{subscribers} >= {1000}");
        insta::assert_snapshot!(p("subscribers <= 1000"), @"{subscribers} <= {1000}");
        insta::assert_snapshot!(p("subscribers 100 TO 1000"), @"{subscribers} {100} TO {1000}");

        // Test NOT
        insta::assert_snapshot!(p("NOT subscribers < 1000"), @"NOT ({subscribers} < {1000})");
        insta::assert_snapshot!(p("NOT subscribers 100 TO 1000"), @"NOT ({subscribers} {100} TO {1000})");

        // Test NULL + NOT NULL
        insta::assert_snapshot!(p("subscribers IS NULL"), @"{subscribers} IS NULL");
        insta::assert_snapshot!(p("NOT subscribers IS NULL"), @"NOT ({subscribers} IS NULL)");
        insta::assert_snapshot!(p("subscribers IS NOT NULL"), @"NOT ({subscribers} IS NULL)");
        insta::assert_snapshot!(p("NOT subscribers IS NOT NULL"), @"{subscribers} IS NULL");
        insta::assert_snapshot!(p("subscribers  IS   NOT   NULL"), @"NOT ({subscribers} IS NULL)");

        // Test EMPTY + NOT EMPTY
        insta::assert_snapshot!(p("subscribers IS EMPTY"), @"{subscribers} IS EMPTY");
        insta::assert_snapshot!(p("NOT subscribers IS EMPTY"), @"NOT ({subscribers} IS EMPTY)");
        insta::assert_snapshot!(p("subscribers IS NOT EMPTY"), @"NOT ({subscribers} IS EMPTY)");
        insta::assert_snapshot!(p("NOT subscribers IS NOT EMPTY"), @"{subscribers} IS EMPTY");
        insta::assert_snapshot!(p("subscribers  IS   NOT   EMPTY"), @"NOT ({subscribers} IS EMPTY)");

        // Test EXISTS + NOT EXITS
        insta::assert_snapshot!(p("subscribers EXISTS"), @"{subscribers} EXISTS");
        insta::assert_snapshot!(p("NOT subscribers EXISTS"), @"NOT ({subscribers} EXISTS)");
        insta::assert_snapshot!(p("subscribers NOT EXISTS"), @"NOT ({subscribers} EXISTS)");
        insta::assert_snapshot!(p("NOT subscribers NOT EXISTS"), @"{subscribers} EXISTS");
        insta::assert_snapshot!(p("subscribers NOT   EXISTS"), @"NOT ({subscribers} EXISTS)");

        // Test CONTAINS + NOT CONTAINS
        insta::assert_snapshot!(p("subscribers CONTAINS 'hello'"), @"{subscribers} CONTAINS {hello}");
        insta::assert_snapshot!(p("NOT subscribers CONTAINS 'hello'"), @"NOT ({subscribers} CONTAINS {hello})");
        insta::assert_snapshot!(p("subscribers NOT CONTAINS hello"), @"NOT ({subscribers} CONTAINS {hello})");
        insta::assert_snapshot!(p("NOT subscribers NOT CONTAINS 'hello'"), @"{subscribers} CONTAINS {hello}");
        insta::assert_snapshot!(p("subscribers NOT   CONTAINS 'hello'"), @"NOT ({subscribers} CONTAINS {hello})");

        // Test STARTS WITH + NOT STARTS WITH
        insta::assert_snapshot!(p("subscribers STARTS WITH 'hel'"), @"{subscribers} STARTS WITH {hel}");
        insta::assert_snapshot!(p("NOT subscribers STARTS WITH 'hel'"), @"NOT ({subscribers} STARTS WITH {hel})");
        insta::assert_snapshot!(p("subscribers NOT STARTS WITH hel"), @"NOT ({subscribers} STARTS WITH {hel})");
        insta::assert_snapshot!(p("NOT subscribers NOT STARTS WITH 'hel'"), @"{subscribers} STARTS WITH {hel}");
        insta::assert_snapshot!(p("subscribers NOT   STARTS WITH 'hel'"), @"NOT ({subscribers} STARTS WITH {hel})");

        // Test nested NOT
        insta::assert_snapshot!(p("NOT NOT NOT NOT x = 5"), @"{x} = {5}");
        insta::assert_snapshot!(p("NOT NOT (NOT NOT x = 5)"), @"{x} = {5}");

        // Test geo radius
        insta::assert_snapshot!(p("_geoRadius(12, 13, 14)"), @"_geoRadius({12}, {13}, {14})");
        insta::assert_snapshot!(p("NOT _geoRadius(12, 13, 14)"), @"NOT (_geoRadius({12}, {13}, {14}))");
        insta::assert_snapshot!(p("_geoRadius(12,13,14)"), @"_geoRadius({12}, {13}, {14})");

        // Test geo bounding box
        insta::assert_snapshot!(p("_geoBoundingBox([12, 13], [14, 15])"), @"_geoBoundingBox([{12}, {13}], [{14}, {15}])");
        insta::assert_snapshot!(p("NOT _geoBoundingBox([12, 13], [14, 15])"), @"NOT (_geoBoundingBox([{12}, {13}], [{14}, {15}]))");
        insta::assert_snapshot!(p("_geoBoundingBox([12,13],[14,15])"), @"_geoBoundingBox([{12}, {13}], [{14}, {15}])");

        // Test OR + AND
        insta::assert_snapshot!(p("channel = ponce AND 'dog race' != 'bernese mountain'"), @"AND[{channel} = {ponce}, {dog race} != {bernese mountain}, ]");
        insta::assert_snapshot!(p("channel = ponce OR 'dog race' != 'bernese mountain'"), @"OR[{channel} = {ponce}, {dog race} != {bernese mountain}, ]");
        insta::assert_snapshot!(p("channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000"), @"OR[AND[{channel} = {ponce}, {dog race} != {bernese mountain}, ], {subscribers} > {1000}, ]");
        insta::assert_snapshot!(
        p("channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000 OR colour = red OR colour = blue AND size = 7"),
        @"OR[AND[{channel} = {ponce}, {dog race} != {bernese mountain}, ], {subscribers} > {1000}, {colour} = {red}, AND[{colour} = {blue}, {size} = {7}, ], ]"
        );

        // Test parentheses
        insta::assert_snapshot!(p("channel = ponce AND ( 'dog race' != 'bernese mountain' OR subscribers > 1000 )"), @"AND[{channel} = {ponce}, OR[{dog race} != {bernese mountain}, {subscribers} > {1000}, ], ]");
        insta::assert_snapshot!(p("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, 13, 14)"), @"AND[OR[AND[{channel} = {ponce}, {dog race} != {bernese mountain}, ], {subscribers} > {1000}, ], _geoRadius({12}, {13}, {14}), ]");

        // Test recursion
        // This is the most that is allowed
        insta::assert_snapshot!(
            p("(((((((((((((((((((((((((((((((((((((((((((((((((x = 1)))))))))))))))))))))))))))))))))))))))))))))))))"),
            @"{x} = {1}"
        );
        insta::assert_snapshot!(
            p("NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT x = 1"),
            @"NOT ({x} = {1})"
        );

        // Confusing keywords
        insta::assert_snapshot!(p(r#"NOT "OR" EXISTS AND "EXISTS" NOT EXISTS"#), @"AND[NOT ({OR} EXISTS), NOT ({EXISTS} EXISTS), ]");
    }

    #[test]
    fn error() {
        use FilterCondition as Fc;

        fn p(s: &str) -> impl std::fmt::Display + '_ {
            Fc::parse(s).unwrap_err().to_string()
        }

        insta::assert_snapshot!(p("channel = Ponce = 12"), @r###"
        Found unexpected characters at the end of the filter: `= 12`. You probably forgot an `OR` or an `AND` rule.
        17:21 channel = Ponce = 12
        "###);

        insta::assert_snapshot!(p("channel =    "), @r###"
        Was expecting a value but instead got nothing.
        14:14 channel =
        "###);

        insta::assert_snapshot!(p("channel = 🐻"), @r###"
        Was expecting a value but instead got `🐻`.
        11:12 channel = 🐻
        "###);

        insta::assert_snapshot!(p("channel = 🐻 AND followers < 100"), @r###"
        Was expecting a value but instead got `🐻`.
        11:12 channel = 🐻 AND followers < 100
        "###);

        insta::assert_snapshot!(p("'OR'"), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `\'OR\'`.
        1:5 'OR'
        "###);

        insta::assert_snapshot!(p("OR"), @r###"
        Was expecting a value but instead got `OR`, which is a reserved keyword. To use `OR` as a field name or a value, surround it by quotes.
        1:3 OR
        "###);

        insta::assert_snapshot!(p("channel Ponce"), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `channel Ponce`.
        1:14 channel Ponce
        "###);

        insta::assert_snapshot!(p("channel = Ponce OR"), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` but instead got nothing.
        19:19 channel = Ponce OR
        "###);

        insta::assert_snapshot!(p("_geoRadius"), @r###"
        The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`.
        1:11 _geoRadius
        "###);

        insta::assert_snapshot!(p("_geoRadius = 12"), @r###"
        The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`.
        1:16 _geoRadius = 12
        "###);

        insta::assert_snapshot!(p("_geoBoundingBox"), @r###"
        The `_geoBoundingBox` filter expects two pairs of arguments: `_geoBoundingBox([latitude, longitude], [latitude, longitude])`.
        1:16 _geoBoundingBox
        "###);

        insta::assert_snapshot!(p("_geoBoundingBox = 12"), @r###"
        The `_geoBoundingBox` filter expects two pairs of arguments: `_geoBoundingBox([latitude, longitude], [latitude, longitude])`.
        1:21 _geoBoundingBox = 12
        "###);

        insta::assert_snapshot!(p("_geoBoundingBox(1.0, 1.0)"), @r###"
        The `_geoBoundingBox` filter expects two pairs of arguments: `_geoBoundingBox([latitude, longitude], [latitude, longitude])`.
        1:26 _geoBoundingBox(1.0, 1.0)
        "###);

        insta::assert_snapshot!(p("_geoPoint(12, 13, 14)"), @r###"
        `_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.
        1:22 _geoPoint(12, 13, 14)
        "###);

        insta::assert_snapshot!(p("position <= _geoPoint(12, 13, 14)"), @r###"
        `_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.
        13:34 position <= _geoPoint(12, 13, 14)
        "###);

        insta::assert_snapshot!(p("_geoDistance(12, 13, 14)"), @r###"
        `_geoDistance` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.
        1:25 _geoDistance(12, 13, 14)
        "###);

        insta::assert_snapshot!(p("position <= _geoDistance(12, 13, 14)"), @r###"
        `_geoDistance` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.
        13:37 position <= _geoDistance(12, 13, 14)
        "###);

        insta::assert_snapshot!(p("_geo(12, 13, 14)"), @r###"
        `_geo` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.
        1:17 _geo(12, 13, 14)
        "###);

        insta::assert_snapshot!(p("position <= _geo(12, 13, 14)"), @r###"
        `_geo` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.
        13:29 position <= _geo(12, 13, 14)
        "###);

        insta::assert_snapshot!(p("position <= _geoRadius(12, 13, 14)"), @r###"
        The `_geoRadius` filter is an operation and can't be used as a value.
        13:35 position <= _geoRadius(12, 13, 14)
        "###);

        insta::assert_snapshot!(p("channel = 'ponce"), @r###"
        Expression `\'ponce` is missing the following closing delimiter: `'`.
        11:17 channel = 'ponce
        "###);

        insta::assert_snapshot!(p("channel = \"ponce"), @r###"
        Expression `\"ponce` is missing the following closing delimiter: `"`.
        11:17 channel = "ponce
        "###);

        insta::assert_snapshot!(p("channel = mv OR (followers >= 1000"), @r###"
        Expression `(followers >= 1000` is missing the following closing delimiter: `)`.
        17:35 channel = mv OR (followers >= 1000
        "###);

        insta::assert_snapshot!(p("channel = mv OR followers >= 1000)"), @r###"
        Found unexpected characters at the end of the filter: `)`. You probably forgot an `OR` or an `AND` rule.
        34:35 channel = mv OR followers >= 1000)
        "###);

        insta::assert_snapshot!(p("colour NOT EXIST"), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `colour NOT EXIST`.
        1:17 colour NOT EXIST
        "###);

        insta::assert_snapshot!(p("subscribers 100 TO1000"), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `subscribers 100 TO1000`.
        1:23 subscribers 100 TO1000
        "###);

        insta::assert_snapshot!(p("channel = ponce ORdog != 'bernese mountain'"), @r###"
        Found unexpected characters at the end of the filter: `ORdog != \'bernese mountain\'`. You probably forgot an `OR` or an `AND` rule.
        17:44 channel = ponce ORdog != 'bernese mountain'
        "###);

        insta::assert_snapshot!(p("colour IN blue, green]"), @r###"
        Expected `[` after `IN` keyword.
        11:23 colour IN blue, green]
        "###);

        insta::assert_snapshot!(p("colour IN [blue, green, 'blue' > 2]"), @r###"
        Expected only comma-separated field names inside `IN[..]` but instead found `> 2]`.
        32:36 colour IN [blue, green, 'blue' > 2]
        "###);

        insta::assert_snapshot!(p("colour IN [blue, green, AND]"), @r###"
        Expected only comma-separated field names inside `IN[..]` but instead found `AND]`.
        25:29 colour IN [blue, green, AND]
        "###);

        insta::assert_snapshot!(p("colour IN [blue, green"), @r###"
        Expected matching `]` after the list of field names given to `IN[`
        23:23 colour IN [blue, green
        "###);

        insta::assert_snapshot!(p("colour IN ['blue, green"), @r###"
        Expression `\'blue, green` is missing the following closing delimiter: `'`.
        12:24 colour IN ['blue, green
        "###);

        insta::assert_snapshot!(p("x = EXISTS"), @r###"
        Was expecting a value but instead got `EXISTS`, which is a reserved keyword. To use `EXISTS` as a field name or a value, surround it by quotes.
        5:11 x = EXISTS
        "###);

        insta::assert_snapshot!(p("AND = 8"), @r###"
        Was expecting a value but instead got `AND`, which is a reserved keyword. To use `AND` as a field name or a value, surround it by quotes.
        1:4 AND = 8
        "###);

        insta::assert_snapshot!(p("((((((((((((((((((((((((((((((((((((((((((((((((((x = 1))))))))))))))))))))))))))))))))))))))))))))))))))"), @r###"
        The filter exceeded the maximum depth limit. Try rewriting the filter so that it contains fewer nested conditions.
        51:106 ((((((((((((((((((((((((((((((((((((((((((((((((((x = 1))))))))))))))))))))))))))))))))))))))))))))))))))
        "###);

        insta::assert_snapshot!(
            p("NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT x = 1"),
            @r###"
        The filter exceeded the maximum depth limit. Try rewriting the filter so that it contains fewer nested conditions.
        797:802 NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT NOT x = 1
        "###
        );

        insta::assert_snapshot!(p(r#"NOT OR EXISTS AND EXISTS NOT EXISTS"#), @r###"
        Was expecting a value but instead got `OR`, which is a reserved keyword. To use `OR` as a field name or a value, surround it by quotes.
        5:7 NOT OR EXISTS AND EXISTS NOT EXISTS
        "###);

        insta::assert_snapshot!(p(r#"value NULL"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value NULL`.
        1:11 value NULL
        "###);
        insta::assert_snapshot!(p(r#"value NOT NULL"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value NOT NULL`.
        1:15 value NOT NULL
        "###);
        insta::assert_snapshot!(p(r#"value EMPTY"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value EMPTY`.
        1:12 value EMPTY
        "###);
        insta::assert_snapshot!(p(r#"value NOT EMPTY"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value NOT EMPTY`.
        1:16 value NOT EMPTY
        "###);
        insta::assert_snapshot!(p(r#"value IS"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value IS`.
        1:9 value IS
        "###);
        insta::assert_snapshot!(p(r#"value IS NOT"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value IS NOT`.
        1:13 value IS NOT
        "###);
        insta::assert_snapshot!(p(r#"value IS EXISTS"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value IS EXISTS`.
        1:16 value IS EXISTS
        "###);
        insta::assert_snapshot!(p(r#"value IS NOT EXISTS"#), @r###"
        Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `value IS NOT EXISTS`.
        1:20 value IS NOT EXISTS
        "###);
    }

    #[test]
    fn depth() {
        let filter = FilterCondition::parse("account_ids=1 OR account_ids=2 OR account_ids=3 OR account_ids=4 OR account_ids=5 OR account_ids=6").unwrap().unwrap();
        assert!(filter.token_at_depth(1).is_some());
        assert!(filter.token_at_depth(2).is_none());

        let filter = FilterCondition::parse("(account_ids=1 OR (account_ids=2 AND account_ids=3) OR (account_ids=4 AND account_ids=5) OR account_ids=6)").unwrap().unwrap();
        assert!(filter.token_at_depth(2).is_some());
        assert!(filter.token_at_depth(3).is_none());

        let filter = FilterCondition::parse("account_ids=1 OR account_ids=2 AND account_ids=3 OR account_ids=4 AND account_ids=5 OR account_ids=6").unwrap().unwrap();
        assert!(filter.token_at_depth(2).is_some());
        assert!(filter.token_at_depth(3).is_none());
    }

    #[test]
    fn fids() {
        let filter = Fc::parse("field = value").unwrap().unwrap();
        let fids: Vec<_> = filter.fids(MAX_FILTER_DEPTH).collect();
        assert_eq!(fids.len(), 1);
        assert_eq!(fids[0].value(), "field");

        let filter = Fc::parse("field IN [1, 2, 3]").unwrap().unwrap();
        let fids: Vec<_> = filter.fids(MAX_FILTER_DEPTH).collect();
        assert_eq!(fids.len(), 1);
        assert_eq!(fids[0].value(), "field");

        let filter = Fc::parse("field != value").unwrap().unwrap();
        let fids: Vec<_> = filter.fids(MAX_FILTER_DEPTH).collect();
        assert_eq!(fids.len(), 1);
        assert_eq!(fids[0].value(), "field");

        let filter = Fc::parse("field1 = value1 AND field2 = value2").unwrap().unwrap();
        let fids: Vec<_> = filter.fids(MAX_FILTER_DEPTH).collect();
        assert_eq!(fids.len(), 2);
        assert!(fids[0].value() == "field1");
        assert!(fids[1].value() == "field2");

        let filter = Fc::parse("field1 = value1 OR field2 = value2").unwrap().unwrap();
        let fids: Vec<_> = filter.fids(MAX_FILTER_DEPTH).collect();
        assert_eq!(fids.len(), 2);
        assert!(fids[0].value() == "field1");
        assert!(fids[1].value() == "field2");

        let depth = 2;
        let filter =
            Fc::parse("field1 = value1 AND (field2 = value2 OR field3 = value3)").unwrap().unwrap();
        let fids: Vec<_> = filter.fids(depth).collect();
        assert_eq!(fids.len(), 1);
        assert_eq!(fids[0].value(), "field1");
    }

    #[test]
    fn token_from_str() {
        let s = "test string that should not be parsed";
        let token: Token = s.into();
        assert_eq!(token.value(), s);
    }
}
