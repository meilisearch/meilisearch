#![allow(unused_imports)]
//! BNF grammar:
//!
//! ```text
//! expression     = or
//! or             = and (~ "OR" ~ and)
//! and            = not (~ "AND" not)*
//! not            = ("NOT" | "!") not | primary
//! primary        = (WS* ~ "("  expression ")" ~ WS*) | condition | to | geoRadius
//! to             = value value TO value
//! condition      = value ("==" | ">" ...) value
//! value          = WS* ~ ( word | singleQuoted | doubleQuoted) ~ WS*
//! singleQuoted   = "'" .* all but quotes "'"
//! doubleQuoted   = "\"" (word | spaces)* "\""
//! word           = (alphanumeric | _ | - | .)+
//! geoRadius      = WS* ~ "_geoRadius(float ~ "," ~ float ~ "," float)
//! ```

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterCondition<'a> {
    Operator { fid: Token<'a>, op: Operator<'a> },
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
            Operator { fid, op } => match op.negate() {
                (op, None) => Operator { fid, op },
                (a, Some(b)) => {
                    Or(Operator { fid: fid.clone(), op: a }.into(), Operator { fid, op: b }.into())
                }
            },
            Or(a, b) => And(a.negate().into(), b.negate().into()),
            And(a, b) => Or(a.negate().into(), b.negate().into()),
            Empty => Empty,
            GeoLowerThan { point, radius } => GeoGreaterThan { point, radius },
            GeoGreaterThan { point, radius } => GeoLowerThan { point, radius },
        }
    }

    pub fn parse(input: &'a str) -> IResult<Span, Self> {
        let span = Span::new(input);
        parse_expression(span)
    }
}

use std::collections::HashSet;
use std::fmt::Debug;
use std::result::Result as StdResult;

use nom::branch::alt;
use nom::bytes::complete::{tag, take_till, take_while1};
use nom::character::complete::{char, multispace0};
use nom::combinator::map;
use nom::error::{ContextError, ErrorKind, ParseError, VerboseError};
use nom::multi::{many0, separated_list1};
use nom::number::complete::recognize_float;
use nom::sequence::{delimited, preceded, tuple};
use nom::IResult;
use nom_locate::LocatedSpan;

use self::Operator::*;

pub enum FilterError {
    AttributeNotFilterable(String),
}

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

type Span<'a> = LocatedSpan<&'a str>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator<'a> {
    GreaterThan(Token<'a>),
    GreaterThanOrEqual(Token<'a>),
    Equal(Token<'a>),
    NotEqual(Token<'a>),
    LowerThan(Token<'a>),
    LowerThanOrEqual(Token<'a>),
    Between { from: Token<'a>, to: Token<'a> },
}

impl<'a> Operator<'a> {
    /// This method can return two operations in case it must express
    /// an OR operation for the between case (i.e. `TO`).
    pub fn negate(self) -> (Self, Option<Self>) {
        match self {
            GreaterThan(n) => (LowerThanOrEqual(n), None),
            GreaterThanOrEqual(n) => (LowerThan(n), None),
            Equal(s) => (NotEqual(s), None),
            NotEqual(s) => (Equal(s), None),
            LowerThan(n) => (GreaterThanOrEqual(n), None),
            LowerThanOrEqual(n) => (GreaterThan(n), None),
            Between { from, to } => (LowerThan(from), Some(GreaterThan(to))),
        }
    }
}

pub trait FilterParserError<'a>:
    nom::error::ParseError<&'a str> + ContextError<&'a str> + std::fmt::Debug
{
}

impl<'a> FilterParserError<'a> for VerboseError<&'a str> {}

/// and            = not (~ "AND" not)*
fn parse_or(input: Span) -> IResult<Span, FilterCondition> {
    let (input, lhs) = parse_and(input)?;
    let (input, ors) = many0(preceded(ws(tag("OR")), |c| parse_and(c)))(input)?;

    let expr = ors
        .into_iter()
        .fold(lhs, |acc, branch| FilterCondition::Or(Box::new(acc), Box::new(branch)));
    Ok((input, expr))
}

fn parse_and(input: Span) -> IResult<Span, FilterCondition> {
    let (input, lhs) = parse_not(input)?;
    let (input, ors) = many0(preceded(ws(tag("AND")), |c| parse_not(c)))(input)?;
    let expr = ors
        .into_iter()
        .fold(lhs, |acc, branch| FilterCondition::And(Box::new(acc), Box::new(branch)));
    Ok((input, expr))
}

/// not            = ("NOT" | "!") not | primary
fn parse_not(input: Span) -> IResult<Span, FilterCondition> {
    alt((map(preceded(alt((tag("!"), tag("NOT"))), |c| parse_not(c)), |e| e.negate()), |c| {
        parse_primary(c)
    }))(input)
}

fn ws<'a, O>(
    inner: impl FnMut(Span<'a>) -> IResult<Span, O>,
) -> impl FnMut(Span<'a>) -> IResult<Span, O> {
    delimited(multispace0, inner, multispace0)
}

/// condition      = value ("==" | ">" ...) value
fn parse_condition(input: Span) -> IResult<Span, FilterCondition> {
    let operator = alt((tag("<="), tag(">="), tag("!="), tag("<"), tag(">"), tag("=")));
    let (input, (key, op, value)) =
        tuple((|c| parse_value(c), operator, |c| parse_value(c)))(input)?;

    let fid = key.into();

    // TODO
    match *op.fragment() {
        "=" => {
            let k = FilterCondition::Operator { fid, op: Equal(value.into()) };
            Ok((input, k))
        }
        "!=" => {
            let k = FilterCondition::Operator { fid, op: NotEqual(value.into()) };
            Ok((input, k))
        }
        ">" | "<" | "<=" | ">=" => {
            let k = match *op.fragment() {
                ">" => FilterCondition::Operator { fid, op: GreaterThan(value.into()) },
                "<" => FilterCondition::Operator { fid, op: LowerThan(value.into()) },
                "<=" => FilterCondition::Operator { fid, op: LowerThanOrEqual(value.into()) },
                ">=" => FilterCondition::Operator { fid, op: GreaterThanOrEqual(value.into()) },
                _ => unreachable!(),
            };
            Ok((input, k))
        }
        _ => unreachable!(),
    }
}

/// to             = value value TO value
fn parse_to(input: Span) -> IResult<Span, FilterCondition> {
    let (input, (key, from, _, to)) =
        tuple((ws(|c| parse_value(c)), ws(|c| parse_value(c)), tag("TO"), ws(|c| parse_value(c))))(
            input,
        )?;

    Ok((
        input,
        FilterCondition::Operator {
            fid: key.into(),
            op: Between { from: from.into(), to: to.into() },
        },
    ))
}

/// geoRadius      = WS* ~ "_geoRadius(float ~ "," ~ float ~ "," float)
fn parse_geo_radius(input: Span) -> IResult<Span, FilterCondition> {
    let err_msg_args_incomplete = "_geoRadius. The `_geoRadius` filter expect three arguments: `_geoRadius(latitude, longitude, radius)`";
    /*
    TODO
    let err_msg_latitude_invalid =
        "_geoRadius. Latitude must be contained between -90 and 90 degrees.";

    let err_msg_longitude_invalid =
        "_geoRadius. Longitude must be contained between -180 and 180 degrees.";
    */

    let parsed = preceded::<_, _, _, _, _, _>(
        // TODO: forbid spaces between _geoRadius and parenthesis
        ws(tag("_geoRadius")),
        delimited(char('('), separated_list1(tag(","), ws(|c| recognize_float(c))), char(')')),
    )(input);

    let (input, args): (Span, Vec<Span>) = match parsed {
        Ok(e) => e,
        Err(_e) => {
            return Err(nom::Err::Failure(nom::error::Error::add_context(
                input,
                err_msg_args_incomplete,
                nom::error::Error::from_char(input, '('),
            )));
        }
    };

    if args.len() != 3 {
        let e = nom::error::Error::from_char(input, '(');
        return Err(nom::Err::Failure(nom::error::Error::add_context(
            input,
            err_msg_args_incomplete,
            e,
        )));
    }

    let res = FilterCondition::GeoLowerThan {
        point: [args[0].into(), args[1].into()],
        radius: args[2].into(),
    };
    Ok((input, res))
}

/// primary        = (WS* ~ "("  expression ")" ~ WS*) | condition | to | geoRadius
fn parse_primary(input: Span) -> IResult<Span, FilterCondition> {
    alt((
        delimited(ws(char('(')), |c| parse_expression(c), ws(char(')'))),
        |c| parse_condition(c),
        |c| parse_to(c),
        |c| parse_geo_radius(c),
    ))(input)
}

/// value          = WS* ~ ( word | singleQuoted | doubleQuoted) ~ WS*
fn parse_value(input: Span) -> IResult<Span, Span> {
    // singleQuoted   = "'" .* all but quotes "'"
    let simple_quoted_key = |input| take_till(|c: char| c == '\'')(input);
    // doubleQuoted   = "\"" (word | spaces)* "\""
    let quoted_key = |input| take_till(|c: char| c == '"')(input);
    // word           = (alphanumeric | _ | - | .)+
    let word = |input| take_while1(is_key_component)(input);

    alt((
        ws(delimited(char('\''), simple_quoted_key, char('\''))),
        ws(delimited(char('"'), quoted_key, char('"'))),
        ws(word),
    ))(input)
}

fn is_key_component(c: char) -> bool {
    c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
}

/// expression     = or
pub fn parse_expression(input: Span) -> IResult<Span, FilterCondition> {
    parse_or(input)
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use maplit::hashset;

    use super::*;

    /// Create a raw [Token]. You must specify the string that appear BEFORE your element followed by your element
    fn rtok<'a>(before: &'a str, value: &'a str) -> Token<'a> {
        // if the string is empty we still need to return 1 for the line number
        let lines = before.is_empty().then(|| 1).unwrap_or_else(|| before.lines().count());
        let offset = before.chars().count();
        unsafe { Span::new_from_raw_offset(offset, lines as u32, value, ()) }.into()
    }

    #[test]
    fn parse() {
        use FilterCondition as Fc;

        // new_from_raw_offset is unsafe
        let test_case = [
            // simple test
            (
                "channel = Ponce",
                Fc::Operator {
                    fid: rtok("", "channel"),
                    op: Operator::Equal(rtok("channel = ", "Ponce")),
                },
            ),
            (
                "subscribers = 12",
                Fc::Operator {
                    fid: rtok("", "subscribers"),
                    op: Operator::Equal(rtok("subscribers = ", "12")),
                },
            ),
            // test all the quotes and simple quotes
            (
                "channel = 'Mister Mv'",
                Fc::Operator {
                    fid: rtok("", "channel"),
                    op: Operator::Equal(rtok("channel = '", "Mister Mv")),
                },
            ),
            (
                "channel = \"Mister Mv\"",
                Fc::Operator {
                    fid: rtok("", "channel"),
                    op: Operator::Equal(rtok("channel = \"", "Mister Mv")),
                },
            ),
            (
                "'dog race' = Borzoi",
                Fc::Operator {
                    fid: rtok("'", "dog race"),
                    op: Operator::Equal(rtok("'dog race' = ", "Borzoi")),
                },
            ),
            (
                "\"dog race\" = Chusky",
                Fc::Operator {
                    fid: rtok("\"", "dog race"),
                    op: Operator::Equal(rtok("\"dog race\" = ", "Chusky")),
                },
            ),
            (
                "\"dog race\" = \"Bernese Mountain\"",
                Fc::Operator {
                    fid: rtok("\"", "dog race"),
                    op: Operator::Equal(rtok("\"dog race\" = \"", "Bernese Mountain")),
                },
            ),
            (
                "'dog race' = 'Bernese Mountain'",
                Fc::Operator {
                    fid: rtok("'", "dog race"),
                    op: Operator::Equal(rtok("'dog race' = '", "Bernese Mountain")),
                },
            ),
            (
                "\"dog race\" = 'Bernese Mountain'",
                Fc::Operator {
                    fid: rtok("\"", "dog race"),
                    op: Operator::Equal(rtok("\"dog race\" = \"", "Bernese Mountain")),
                },
            ),
            // test all the operators
            (
                "channel != ponce",
                Fc::Operator {
                    fid: rtok("", "channel"),
                    op: Operator::NotEqual(rtok("channel != ", "ponce")),
                },
            ),
            (
                "NOT channel = ponce",
                Fc::Operator {
                    fid: rtok("NOT ", "channel"),
                    op: Operator::NotEqual(rtok("NOT channel = ", "ponce")),
                },
            ),
            (
                "subscribers < 1000",
                Fc::Operator {
                    fid: rtok("", "subscribers"),
                    op: Operator::LowerThan(rtok("subscribers < ", "1000")),
                },
            ),
            (
                "subscribers > 1000",
                Fc::Operator {
                    fid: rtok("", "subscribers"),
                    op: Operator::GreaterThan(rtok("subscribers > ", "1000")),
                },
            ),
            (
                "subscribers <= 1000",
                Fc::Operator {
                    fid: rtok("", "subscribers"),
                    op: Operator::LowerThanOrEqual(rtok("subscribers <= ", "1000")),
                },
            ),
            (
                "subscribers >= 1000",
                Fc::Operator {
                    fid: rtok("", "subscribers"),
                    op: Operator::GreaterThanOrEqual(rtok("subscribers >= ", "1000")),
                },
            ),
            (
                "NOT subscribers < 1000",
                Fc::Operator {
                    fid: rtok("NOT ", "subscribers"),
                    op: Operator::GreaterThanOrEqual(rtok("NOT subscribers < ", "1000")),
                },
            ),
            (
                "NOT subscribers > 1000",
                Fc::Operator {
                    fid: rtok("NOT ", "subscribers"),
                    op: Operator::LowerThanOrEqual(rtok("NOT subscribers > ", "1000")),
                },
            ),
            (
                "NOT subscribers <= 1000",
                Fc::Operator {
                    fid: rtok("NOT ", "subscribers"),
                    op: Operator::GreaterThan(rtok("NOT subscribers <= ", "1000")),
                },
            ),
            (
                "NOT subscribers >= 1000",
                Fc::Operator {
                    fid: rtok("NOT ", "subscribers"),
                    op: Operator::LowerThan(rtok("NOT subscribers >= ", "1000")),
                },
            ),
            (
                "subscribers 100 TO 1000",
                Fc::Operator {
                    fid: rtok("", "subscribers"),
                    op: Operator::Between {
                        from: rtok("subscribers ", "100"),
                        to: rtok("subscribers 100 TO ", "1000"),
                    },
                },
            ),
            (
                "NOT subscribers 100 TO 1000",
                Fc::Or(
                    Fc::Operator {
                        fid: rtok("NOT ", "subscribers"),
                        op: Operator::LowerThan(rtok("NOT subscribers ", "100")),
                    }
                    .into(),
                    Fc::Operator {
                        fid: rtok("NOT ", "subscribers"),
                        op: Operator::GreaterThan(rtok("NOT subscribers 100 TO ", "1000")),
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
                    Fc::Operator {
                        fid: rtok("", "channel"),
                        op: Operator::Equal(rtok("channel = ", "ponce")),
                    }
                    .into(),
                    Fc::Operator {
                        fid: rtok("channel = ponce AND '", "dog race"),
                        op: Operator::NotEqual(rtok(
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
                    Fc::Operator {
                        fid: rtok("", "channel"),
                        op: Operator::Equal(rtok("channel = ", "ponce")),
                    }
                    .into(),
                    Fc::Operator {
                        fid: rtok("channel = ponce OR '", "dog race"),
                        op: Operator::NotEqual(rtok(
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
                        Fc::Operator {
                            fid: rtok("", "channel"),
                            op: Operator::Equal(rtok("channel = ", "ponce")),
                        }
                        .into(),
                        Fc::Operator {
                            fid: rtok("channel = ponce AND '", "dog race"),
                            op: Operator::NotEqual(rtok(
                                "channel = ponce AND 'dog race' != '",
                                "bernese mountain",
                            )),
                        }
                        .into(),
                    )
                    .into(),
                    Fc::Operator {
                        fid: rtok(
                            "channel = ponce AND 'dog race' != 'bernese mountain' OR ",
                            "subscribers",
                        ),
                        op: Operator::GreaterThan(rtok(
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
                        Fc::Operator { fid: rtok("", "channel"), op: Operator::Equal(rtok("channel = ", "ponce")) }.into(),
                        Fc::Or(
                            Fc::Operator { fid: rtok("channel = ponce AND ( '", "dog race"), op: Operator::NotEqual(rtok("channel = ponce AND ( 'dog race' != '", "bernese mountain"))}.into(),
                            Fc::Operator { fid: rtok("channel = ponce AND ( 'dog race' != 'bernese mountain' OR ", "subscribers"), op: Operator::GreaterThan(rtok("channel = ponce AND ( 'dog race' != 'bernese mountain' OR subscribers > ", "1000")) }.into(),
                    ).into()),
            ),
            (
                "(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, 13, 14)",
                Fc::And(
                    Fc::Or(
                        Fc::And(
                            Fc::Operator { fid: rtok("(", "channel"), op: Operator::Equal(rtok("(channel = ", "ponce")) }.into(),
                            Fc::Operator { fid: rtok("(channel = ponce AND '", "dog race"), op: Operator::NotEqual(rtok("(channel = ponce AND 'dog race' != '", "bernese mountain")) }.into(),
                        ).into(),
                        Fc::Operator { fid: rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR ", "subscribers"), op: Operator::GreaterThan(rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > ", "1000")) }.into(),
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
            let filter = result.unwrap().1;
            assert_eq!(filter, expected, "Filter `{}` failed.", input);
        }
    }

    #[test]
    fn name() {
        use FilterCondition as Fc;

        // new_from_raw_offset is unsafe
        let test_case = [
            // simple test
            (
                "channel=Ponce",
                Fc::Operator {
                    fid: rtok("", "channel"),
                    op: Operator::Equal(rtok("channel = ", "Ponce")),
                },
            ),
        ];

        for (input, expected) in test_case {
            let result = Fc::parse(input);

            assert!(
                result.is_ok(),
                "Filter `{:?}` was supposed to be parsed but failed with the following error: `{}`",
                expected,
                result.unwrap_err()
            );
            let filter = result.unwrap().1;
            assert_eq!(filter, expected, "Filter `{}` failed.", input);
        }
    }
}
