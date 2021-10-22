//! BNF grammar:
//!
//! ```text
//! expression     = or
//! or             = and (~ "OR" ~ and)
//! and            = not (~ "AND" not)*
//! not            = ("NOT" | "!") not | primary
//! primary        = (WS* ~ "("  expression ")" ~ WS*) | condition | to | geoRadius
//! condition      = value ("==" | ">" ...) value
//! to             = value value TO value
//! value          = WS* ~ ( word | singleQuoted | doubleQuoted) ~ WS*
//! singleQuoted   = "'" .* all but quotes "'"
//! doubleQuoted   = "\"" (word | spaces)* "\""
//! word           = (alphanumeric | _ | - | .)+
//! geoRadius      = WS* ~ "_geoRadius(float ~ "," ~ float ~ "," float)
//! ```

mod condition;
mod value;
use std::fmt::Debug;

pub use condition::{parse_condition, parse_to, Condition};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, multispace0};
use nom::combinator::map;
use nom::error::{ContextError, Error, ErrorKind, VerboseError};
use nom::multi::{many0, separated_list1};
use nom::number::complete::recognize_float;
use nom::sequence::{delimited, preceded, tuple};
use nom::{Finish, IResult};
use nom_greedyerror::GreedyError;
use nom_locate::LocatedSpan;
pub(crate) use value::parse_value;

pub type Span<'a> = LocatedSpan<&'a str>;

pub trait FilterParserError<'a>: nom::error::ParseError<Span<'a>> + ContextError<Span<'a>> {}
impl<'a> FilterParserError<'a> for GreedyError<Span<'a>, ErrorKind> {}
impl<'a> FilterParserError<'a> for VerboseError<Span<'a>> {}
impl<'a> FilterParserError<'a> for Error<Span<'a>> {}

use FilterParserError as FPError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token<'a> {
    pub position: Span<'a>,
    pub inner: &'a str,
}

impl<'a> Token<'a> {
    pub fn new(position: Span<'a>) -> Self {
        Self { position, inner: &position }
    }
}

impl<'a> From<Span<'a>> for Token<'a> {
    fn from(span: Span<'a>) -> Self {
        Self { inner: &span, position: span }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterCondition<'a> {
    Condition { fid: Token<'a>, op: Condition<'a> },
    Or(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
    GeoLowerThan { point: [Token<'a>; 2], radius: Token<'a> },
    GeoGreaterThan { point: [Token<'a>; 2], radius: Token<'a> },
    Empty,
}

impl<'a> FilterCondition<'a> {
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
            Empty => Empty,
            GeoLowerThan { point, radius } => GeoGreaterThan { point, radius },
            GeoGreaterThan { point, radius } => GeoLowerThan { point, radius },
        }
    }

    pub fn parse<E: FPError<'a>>(input: &'a str) -> Result<Self, E> {
        if input.trim().is_empty() {
            return Ok(Self::Empty);
        }
        let span = Span::new(input);
        parse_expression::<'a, E>(span).finish().map(|(_rem, output)| output)
    }
}

// remove OPTIONAL whitespaces before AND after the the provided parser
fn ws<'a, O, E: FPError<'a>>(
    inner: impl FnMut(Span<'a>) -> IResult<Span, O, E>,
) -> impl FnMut(Span<'a>) -> IResult<Span, O, E> {
    delimited(multispace0, inner, multispace0)
}

/// and            = not (~ "AND" not)*
fn parse_or<'a, E: FPError<'a>>(input: Span<'a>) -> IResult<Span, FilterCondition, E> {
    let (input, lhs) = parse_and(input)?;
    let (input, ors) = many0(preceded(ws(tag("OR")), |c| parse_and(c)))(input)?;

    let expr = ors
        .into_iter()
        .fold(lhs, |acc, branch| FilterCondition::Or(Box::new(acc), Box::new(branch)));
    Ok((input, expr))
}

fn parse_and<'a, E: FPError<'a>>(input: Span<'a>) -> IResult<Span, FilterCondition, E> {
    let (input, lhs) = parse_not(input)?;
    let (input, ors) = many0(preceded(ws(tag("AND")), |c| parse_not(c)))(input)?;
    let expr = ors
        .into_iter()
        .fold(lhs, |acc, branch| FilterCondition::And(Box::new(acc), Box::new(branch)));
    Ok((input, expr))
}

/// not            = ("NOT" | "!") not | primary
fn parse_not<'a, E: FPError<'a>>(input: Span<'a>) -> IResult<Span, FilterCondition, E> {
    alt((map(preceded(alt((tag("!"), tag("NOT"))), |c| parse_not(c)), |e| e.negate()), |c| {
        parse_primary(c)
    }))(input)
}

/// geoRadius      = WS* ~ "_geoRadius(float ~ "," ~ float ~ "," float)
fn parse_geo_radius<'a, E: FPError<'a>>(input: Span<'a>) -> IResult<Span<'a>, FilterCondition, E> {
    let err_msg_args_incomplete = "_geoRadius. The `_geoRadius` filter expect three arguments: `_geoRadius(latitude, longitude, radius)`";

    // we want to forbid space BEFORE the _geoRadius but not after
    let parsed = preceded::<_, _, _, _, _, _>(
        tuple((multispace0, tag("_geoRadius"))),
        delimited(char('('), separated_list1(tag(","), ws(|c| recognize_float(c))), char(')')),
    )(input);

    let (input, args): (Span, Vec<Span>) = parsed?;

    if args.len() != 3 {
        let e = E::from_char(input, '(');
        return Err(nom::Err::Failure(E::add_context(input, err_msg_args_incomplete, e)));
    }

    let res = FilterCondition::GeoLowerThan {
        point: [args[0].into(), args[1].into()],
        radius: args[2].into(),
    };
    Ok((input, res))
}

/// primary        = (WS* ~ "("  expression ")" ~ WS*) | condition | to | geoRadius
fn parse_primary<'a, E: FPError<'a>>(input: Span<'a>) -> IResult<Span, FilterCondition, E> {
    alt((
        delimited(ws(char('(')), |c| parse_expression(c), ws(char(')'))),
        |c| parse_condition(c),
        |c| parse_to(c),
        |c| parse_geo_radius(c),
    ))(input)
}

/// expression     = or
pub fn parse_expression<'a, E: FPError<'a>>(input: Span<'a>) -> IResult<Span, FilterCondition, E> {
    parse_or(input)
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Create a raw [Token]. You must specify the string that appear BEFORE your element followed by your element
    pub fn rtok<'a>(before: &'a str, value: &'a str) -> Token<'a> {
        // if the string is empty we still need to return 1 for the line number
        let lines = before.is_empty().then(|| 1).unwrap_or_else(|| before.lines().count());
        let offset = before.chars().count();
        unsafe { Span::new_from_raw_offset(offset, lines as u32, value, ()) }.into()
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
            let result = Fc::parse::<Error<Span>>(input);

            assert!(
                result.is_ok(),
                "Filter `{:?}` was supposed to be parsed but failed with the following error: `{}`",
                expected,
                result.unwrap_err()
            );
            let filter = result.unwrap();
            assert_eq!(filter, expected, "Filter `{}` failed.", input);
        }
    }
}
