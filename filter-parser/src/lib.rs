//! BNF grammar:
//!
//! ```text
//! filter         = expression ~ EOF
//! expression     = or
//! or             = and ("OR" and)
//! and            = not ("AND" not)*
//! not            = ("NOT" not) | primary
//! primary        = (WS* "("  expression ")" WS*) | geoRadius | condition | exists | not_exists | to
//! condition      = value ("==" | ">" ...) value
//! exists         = value EXISTS
//! not_exists     = value NOT EXISTS
//! to             = value value TO value
//! value          = WS* ( word | singleQuoted | doubleQuoted) ~ WS*
//! singleQuoted   = "'" .* all but quotes "'"
//! doubleQuoted   = "\"" .* all but double quotes "\""
//! word           = (alphanumeric | _ | - | .)+
//! geoRadius      = WS* ~ "_geoRadius(" WS* float WS* "," WS* float WS* "," float WS* ")"
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
//! - If a user try to use a geoRadius as a value we must throw an error.
//! ```text
//! field = _geoRadius(12, 13, 14)
//! ```
//!

mod condition;
mod error;
mod value;

use std::fmt::Debug;
use std::str::FromStr;

pub use condition::{parse_condition, parse_to, Condition};
use condition::{parse_exists, parse_not_exists};
use error::{cut_with_err, NomErrorExt};
pub use error::{Error, ErrorKind};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, multispace0};
use nom::combinator::{cut, eof, map};
use nom::multi::{many0, separated_list1};
use nom::number::complete::recognize_float;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::Finish;
use nom_locate::LocatedSpan;
pub(crate) use value::parse_value;

pub type Span<'a> = LocatedSpan<&'a str, &'a str>;

type IResult<'a, Ret> = nom::IResult<Span<'a>, Ret, Error<'a>>;

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

    pub fn lexeme(&self) -> &str {
        &self.span
    }

    pub fn value(&self) -> &str {
        self.value.as_ref().map_or(&self.span, |value| value)
    }

    pub fn as_external_error(&self, error: impl std::error::Error) -> Error<'a> {
        Error::new_from_external(self.span, error)
    }

    pub fn parse<T>(&self) -> Result<T, Error>
    where
        T: FromStr,
        T::Err: std::error::Error,
    {
        self.span.parse().map_err(|e| self.as_external_error(e))
    }
}

impl<'a> From<Span<'a>> for Token<'a> {
    fn from(span: Span<'a>) -> Self {
        Self { span, value: None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterCondition<'a> {
    Condition { fid: Token<'a>, op: Condition<'a> },
    Or(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
    GeoLowerThan { point: [Token<'a>; 2], radius: Token<'a> },
    GeoGreaterThan { point: [Token<'a>; 2], radius: Token<'a> },
}

impl<'a> FilterCondition<'a> {
    /// Returns the first token found at the specified depth, `None` if no token at this depth.
    pub fn token_at_depth(&self, depth: usize) -> Option<&Token> {
        match self {
            FilterCondition::Condition { fid, .. } if depth == 0 => Some(fid),
            FilterCondition::Or(left, right) => {
                let depth = depth.saturating_sub(1);
                right.token_at_depth(depth).or_else(|| left.token_at_depth(depth))
            }
            FilterCondition::And(left, right) => {
                let depth = depth.saturating_sub(1);
                right.token_at_depth(depth).or_else(|| left.token_at_depth(depth))
            }
            FilterCondition::GeoLowerThan { point: [point, _], .. } if depth == 0 => Some(point),
            FilterCondition::GeoGreaterThan { point: [point, _], .. } if depth == 0 => Some(point),
            _ => None,
        }
    }

    pub fn negate(self) -> FilterCondition<'a> {
        use FilterCondition::*;

        match self {
            Condition { fid, op } => match op.negate() {
                (op, None) => Condition { fid, op },
                (a, Some(b)) => Or(
                    Condition { fid: fid.clone(), op: a }.into(),
                    Condition { fid, op: b }.into(),
                ),
            },
            Or(a, b) => And(a.negate().into(), b.negate().into()),
            And(a, b) => Or(a.negate().into(), b.negate().into()),
            GeoLowerThan { point, radius } => GeoGreaterThan { point, radius },
            GeoGreaterThan { point, radius } => GeoLowerThan { point, radius },
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
fn ws<'a, O>(inner: impl FnMut(Span<'a>) -> IResult<O>) -> impl FnMut(Span<'a>) -> IResult<O> {
    delimited(multispace0, inner, multispace0)
}

/// or             = and (~ "OR" ~ and)
fn parse_or(input: Span) -> IResult<FilterCondition> {
    let (input, lhs) = parse_and(input)?;
    // if we found a `OR` then we MUST find something next
    let (input, ors) = many0(preceded(ws(tag("OR")), cut(parse_and)))(input)?;

    let expr = ors
        .into_iter()
        .fold(lhs, |acc, branch| FilterCondition::Or(Box::new(acc), Box::new(branch)));
    Ok((input, expr))
}

/// and            = not (~ "AND" not)*
fn parse_and(input: Span) -> IResult<FilterCondition> {
    let (input, lhs) = parse_not(input)?;
    // if we found a `AND` then we MUST find something next
    let (input, ors) = many0(preceded(ws(tag("AND")), cut(parse_not)))(input)?;
    let expr = ors
        .into_iter()
        .fold(lhs, |acc, branch| FilterCondition::And(Box::new(acc), Box::new(branch)));
    Ok((input, expr))
}

/// not            = ("NOT" ~ not) | primary
/// We can have multiple consecutive not, eg: `NOT NOT channel = mv`.
/// If we parse a `NOT` we MUST parse something behind.
fn parse_not(input: Span) -> IResult<FilterCondition> {
    alt((map(preceded(tag("NOT"), cut(parse_not)), |e| e.negate()), parse_primary))(input)
}

/// geoRadius      = WS* ~ "_geoRadius(float ~ "," ~ float ~ "," float)
/// If we parse `_geoRadius` we MUST parse the rest of the expression.
fn parse_geo_radius(input: Span) -> IResult<FilterCondition> {
    // we want to forbid space BEFORE the _geoRadius but not after
    let parsed = preceded(
        tuple((multispace0, tag("_geoRadius"))),
        // if we were able to parse `_geoRadius` and can't parse the rest of the input we return a failure
        cut(delimited(char('('), separated_list1(tag(","), ws(recognize_float)), char(')'))),
    )(input)
    .map_err(|e| e.map(|_| Error::new_from_kind(input, ErrorKind::Geo)));

    let (input, args) = parsed?;

    if args.len() != 3 {
        return Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::Geo)));
    }

    let res = FilterCondition::GeoLowerThan {
        point: [args[0].into(), args[1].into()],
        radius: args[2].into(),
    };
    Ok((input, res))
}

/// geoPoint      = WS* ~ "_geoPoint(float ~ "," ~ float ~ "," float)
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

/// primary        = (WS* ~ "("  expression ")" ~ WS*) | geoRadius | condition | to
fn parse_primary(input: Span) -> IResult<FilterCondition> {
    alt((
        // if we find a first parenthesis, then we must parse an expression and find the closing parenthesis
        delimited(
            ws(char('(')),
            cut(parse_expression),
            cut_with_err(ws(char(')')), |c| {
                Error::new_from_kind(input, ErrorKind::MissingClosingDelimiter(c.char()))
            }),
        ),
        parse_geo_radius,
        parse_condition,
        parse_exists,
        parse_not_exists,
        parse_to,
        // the next lines are only for error handling and are written at the end to have the less possible performance impact
        parse_geo_point,
    ))(input)
    // if the inner parsers did not match enough information to return an accurate error
    .map_err(|e| e.map_err(|_| Error::new_from_kind(input, ErrorKind::InvalidPrimary)))
}

/// expression     = or
pub fn parse_expression(input: Span) -> IResult<FilterCondition> {
    parse_or(input)
}

/// filter     = expression ~ EOF
pub fn parse_filter(input: Span) -> IResult<FilterCondition> {
    terminated(parse_expression, eof)(input)
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Create a raw [Token]. You must specify the string that appear BEFORE your element followed by your element
    pub fn rtok<'a>(before: &'a str, value: &'a str) -> Token<'a> {
        // if the string is empty we still need to return 1 for the line number
        let lines = before.is_empty().then(|| 1).unwrap_or_else(|| before.lines().count());
        let offset = before.chars().count();
        // the extra field is not checked in the tests so we can set it to nothing
        unsafe { Span::new_from_raw_offset(offset, lines as u32, value, "") }.into()
    }

    #[test]
    fn parse() {
        use FilterCondition as Fc;

        let test_case = [
            // simple test
            (
                "channel = Ponce",
                Fc::Condition {
                    fid: rtok("", "channel"),
                    op: Condition::Equal(rtok("channel = ", "Ponce")),
                },
            ),
            (
                "subscribers = 12",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::Equal(rtok("subscribers = ", "12")),
                },
            ),
            // test all the quotes and simple quotes
            (
                "channel = 'Mister Mv'",
                Fc::Condition {
                    fid: rtok("", "channel"),
                    op: Condition::Equal(rtok("channel = '", "Mister Mv")),
                },
            ),
            (
                "channel = \"Mister Mv\"",
                Fc::Condition {
                    fid: rtok("", "channel"),
                    op: Condition::Equal(rtok("channel = \"", "Mister Mv")),
                },
            ),
            (
                "'dog race' = Borzoi",
                Fc::Condition {
                    fid: rtok("'", "dog race"),
                    op: Condition::Equal(rtok("'dog race' = ", "Borzoi")),
                },
            ),
            (
                "\"dog race\" = Chusky",
                Fc::Condition {
                    fid: rtok("\"", "dog race"),
                    op: Condition::Equal(rtok("\"dog race\" = ", "Chusky")),
                },
            ),
            (
                "\"dog race\" = \"Bernese Mountain\"",
                Fc::Condition {
                    fid: rtok("\"", "dog race"),
                    op: Condition::Equal(rtok("\"dog race\" = \"", "Bernese Mountain")),
                },
            ),
            (
                "'dog race' = 'Bernese Mountain'",
                Fc::Condition {
                    fid: rtok("'", "dog race"),
                    op: Condition::Equal(rtok("'dog race' = '", "Bernese Mountain")),
                },
            ),
            (
                "\"dog race\" = 'Bernese Mountain'",
                Fc::Condition {
                    fid: rtok("\"", "dog race"),
                    op: Condition::Equal(rtok("\"dog race\" = \"", "Bernese Mountain")),
                },
            ),
            // test all the operators
            (
                "channel != ponce",
                Fc::Condition {
                    fid: rtok("", "channel"),
                    op: Condition::NotEqual(rtok("channel != ", "ponce")),
                },
            ),
            (
                "NOT channel = ponce",
                Fc::Condition {
                    fid: rtok("NOT ", "channel"),
                    op: Condition::NotEqual(rtok("NOT channel = ", "ponce")),
                },
            ),
            (
                "subscribers < 1000",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::LowerThan(rtok("subscribers < ", "1000")),
                },
            ),
            (
                "subscribers > 1000",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::GreaterThan(rtok("subscribers > ", "1000")),
                },
            ),
            (
                "subscribers <= 1000",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::LowerThanOrEqual(rtok("subscribers <= ", "1000")),
                },
            ),
            (
                "subscribers >= 1000",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::GreaterThanOrEqual(rtok("subscribers >= ", "1000")),
                },
            ),
            (
                "NOT subscribers < 1000",
                Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::GreaterThanOrEqual(rtok("NOT subscribers < ", "1000")),
                },
            ),
            (
                "NOT subscribers > 1000",
                Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::LowerThanOrEqual(rtok("NOT subscribers > ", "1000")),
                },
            ),
            (
                "NOT subscribers <= 1000",
                Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::GreaterThan(rtok("NOT subscribers <= ", "1000")),
                },
            ),
            (
                "NOT subscribers >= 1000",
                Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::LowerThan(rtok("NOT subscribers >= ", "1000")),
                },
            ),
            (
                "subscribers EXISTS",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::Exists,
                },
            ),
            (
                "NOT subscribers EXISTS",
                Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::NotExists,
                },
            ),
            (
                "subscribers NOT EXISTS",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::NotExists,
                },
            ),
            (
                "subscribers 100 TO 1000",
                Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::Between {
                        from: rtok("subscribers ", "100"),
                        to: rtok("subscribers 100 TO ", "1000"),
                    },
                },
            ),
            (
                "NOT subscribers 100 TO 1000",
                Fc::Or(
                    Fc::Condition {
                        fid: rtok("NOT ", "subscribers"),
                        op: Condition::LowerThan(rtok("NOT subscribers ", "100")),
                    }
                    .into(),
                    Fc::Condition {
                        fid: rtok("NOT ", "subscribers"),
                        op: Condition::GreaterThan(rtok("NOT subscribers 100 TO ", "1000")),
                    }
                    .into(),
                ),
            ),
            (
                "_geoRadius(12, 13, 14)",
                Fc::GeoLowerThan {
                    point: [rtok("_geoRadius(", "12"), rtok("_geoRadius(12, ", "13")],
                    radius: rtok("_geoRadius(12, 13, ", "14"),
                },
            ),
            (
                "NOT _geoRadius(12, 13, 14)",
                Fc::GeoGreaterThan {
                    point: [rtok("NOT _geoRadius(", "12"), rtok("NOT _geoRadius(12, ", "13")],
                    radius: rtok("NOT _geoRadius(12, 13, ", "14"),
                },
            ),
            // test simple `or` and `and`
            (
                "channel = ponce AND 'dog race' != 'bernese mountain'",
                Fc::And(
                    Fc::Condition {
                        fid: rtok("", "channel"),
                        op: Condition::Equal(rtok("channel = ", "ponce")),
                    }
                    .into(),
                    Fc::Condition {
                        fid: rtok("channel = ponce AND '", "dog race"),
                        op: Condition::NotEqual(rtok(
                            "channel = ponce AND 'dog race' != '",
                            "bernese mountain",
                        )),
                    }
                    .into(),
                ),
            ),
            (
                "channel = ponce OR 'dog race' != 'bernese mountain'",
                Fc::Or(
                    Fc::Condition {
                        fid: rtok("", "channel"),
                        op: Condition::Equal(rtok("channel = ", "ponce")),
                    }
                    .into(),
                    Fc::Condition {
                        fid: rtok("channel = ponce OR '", "dog race"),
                        op: Condition::NotEqual(rtok(
                            "channel = ponce OR 'dog race' != '",
                            "bernese mountain",
                        )),
                    }
                    .into(),
                ),
            ),
            (
                "channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000",
                Fc::Or(
                    Fc::And(
                        Fc::Condition {
                            fid: rtok("", "channel"),
                            op: Condition::Equal(rtok("channel = ", "ponce")),
                        }
                        .into(),
                        Fc::Condition {
                            fid: rtok("channel = ponce AND '", "dog race"),
                            op: Condition::NotEqual(rtok(
                                "channel = ponce AND 'dog race' != '",
                                "bernese mountain",
                            )),
                        }
                        .into(),
                    )
                    .into(),
                    Fc::Condition {
                        fid: rtok(
                            "channel = ponce AND 'dog race' != 'bernese mountain' OR ",
                            "subscribers",
                        ),
                        op: Condition::GreaterThan(rtok(
                            "channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > ",
                            "1000",
                        )),
                    }
                    .into(),
                ),
            ),
            // test parenthesis
            (
                    "channel = ponce AND ( 'dog race' != 'bernese mountain' OR subscribers > 1000 )",
                    Fc::And(
                        Fc::Condition { fid: rtok("", "channel"), op: Condition::Equal(rtok("channel = ", "ponce")) }.into(),
                        Fc::Or(
                            Fc::Condition { fid: rtok("channel = ponce AND ( '", "dog race"), op: Condition::NotEqual(rtok("channel = ponce AND ( 'dog race' != '", "bernese mountain"))}.into(),
                            Fc::Condition { fid: rtok("channel = ponce AND ( 'dog race' != 'bernese mountain' OR ", "subscribers"), op: Condition::GreaterThan(rtok("channel = ponce AND ( 'dog race' != 'bernese mountain' OR subscribers > ", "1000")) }.into(),
                    ).into()),
            ),
            (
                "(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, 13, 14)",
                Fc::And(
                    Fc::Or(
                        Fc::And(
                            Fc::Condition { fid: rtok("(", "channel"), op: Condition::Equal(rtok("(channel = ", "ponce")) }.into(),
                            Fc::Condition { fid: rtok("(channel = ponce AND '", "dog race"), op: Condition::NotEqual(rtok("(channel = ponce AND 'dog race' != '", "bernese mountain")) }.into(),
                        ).into(),
                        Fc::Condition { fid: rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR ", "subscribers"), op: Condition::GreaterThan(rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > ", "1000")) }.into(),
                    ).into(),
                    Fc::GeoLowerThan { point: [rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(", "12"), rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, ", "13")], radius: rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, 13, ", "14") }.into()
                )
            )
        ];

        for (input, expected) in test_case {
            let result = Fc::parse(input);

            assert!(
                result.is_ok(),
                "Filter `{:?}` was supposed to be parsed but failed with the following error: `{}`",
                expected,
                result.unwrap_err()
            );
            let filter = result.unwrap();
            assert_eq!(filter, Some(expected), "Filter `{}` failed.", input);
        }
    }

    #[test]
    fn error() {
        use FilterCondition as Fc;

        let test_case = [
            // simple test
            ("channel = Ponce = 12", "Found unexpected characters at the end of the filter: `= 12`. You probably forgot an `OR` or an `AND` rule."),
            ("channel =    ", "Was expecting a value but instead got nothing."),
            ("channel = 🐻", "Was expecting a value but instead got `🐻`."),
            ("channel = 🐻 AND followers < 100", "Was expecting a value but instead got `🐻`."),
            ("OR", "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `OR`."),
            ("AND", "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `AND`."),
            ("channel Ponce", "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `channel Ponce`."),
            ("channel = Ponce OR", "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` but instead got nothing."),
            ("_geoRadius", "The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`."),
            ("_geoRadius = 12", "The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`."),
            ("_geoPoint(12, 13, 14)", "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance) built-in rule to filter on `_geo` coordinates."),
            ("position <= _geoPoint(12, 13, 14)", "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance) built-in rule to filter on `_geo` coordinates."),
            ("position <= _geoRadius(12, 13, 14)", "The `_geoRadius` filter is an operation and can't be used as a value."),
            ("channel = 'ponce", "Expression `\\'ponce` is missing the following closing delimiter: `'`."),
            ("channel = \"ponce", "Expression `\\\"ponce` is missing the following closing delimiter: `\"`."),
            ("channel = mv OR (followers >= 1000", "Expression `(followers >= 1000` is missing the following closing delimiter: `)`."),
            ("channel = mv OR followers >= 1000)", "Found unexpected characters at the end of the filter: `)`. You probably forgot an `OR` or an `AND` rule."),
        ];

        for (input, expected) in test_case {
            let result = Fc::parse(input);

            assert!(
                result.is_err(),
                "Filter `{}` wasn't supposed to be parsed but it did with the following result: `{:?}`",
                input,
                result.unwrap()
            );
            let filter = result.unwrap_err().to_string();
            assert!(filter.starts_with(expected), "Filter `{:?}` was supposed to return the following error:\n{}\n, but instead returned\n{}\n.", input, expected, filter);
        }
    }

    #[test]
    fn depth() {
        let filter = FilterCondition::parse("account_ids=1 OR account_ids=2 OR account_ids=3 OR account_ids=4 OR account_ids=5 OR account_ids=6").unwrap().unwrap();
        assert!(filter.token_at_depth(5).is_some());
    }
}
