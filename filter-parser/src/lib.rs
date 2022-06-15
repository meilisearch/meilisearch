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
use std::str::FromStr;

pub use condition::{parse_condition, parse_to, Condition};
use condition::{parse_exists, parse_not_exists};
use error::{cut_with_err, NomErrorExt};
pub use error::{Error, ErrorKind};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::{cut, eof, map, opt};
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
    Not(Box<Self>),
    Condition { fid: Token<'a>, op: Condition<'a> },
    In { fid: Token<'a>, els: Vec<Token<'a>> },
    Or(Vec<Self>),
    And(Vec<Self>),
    GeoLowerThan { point: [Token<'a>; 2], radius: Token<'a> },
}

impl<'a> FilterCondition<'a> {
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
fn ws<'a, O>(inner: impl FnMut(Span<'a>) -> IResult<O>) -> impl FnMut(Span<'a>) -> IResult<O> {
    delimited(multispace0, inner, multispace0)
}


/// value_list = (value ("," value)* ","?)?
fn parse_value_list<'a>(input: Span<'a>) -> IResult<Vec<Token<'a>>> {
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

/// in = value "IN" "[" value_list "]"
fn parse_in(input: Span) -> IResult<FilterCondition> {
    let (input, value) = parse_value(input)?;
    let (input, _) = ws(tag("IN"))(input)?;

    let mut els_parser = delimited(tag("["), parse_value_list, tag("]"));

    let (input, content) = els_parser(input)?;
    let filter = FilterCondition::In { fid: value, els: content };
    Ok((input, filter))
}
/// in = value "NOT" WS* "IN" "[" value_list "]"
fn parse_not_in(input: Span) -> IResult<FilterCondition> {
    let (input, value) = parse_value(input)?;
    let (input, _) = tag("NOT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = ws(tag("IN"))(input)?;

    let mut els_parser = delimited(tag("["), parse_value_list, tag("]"));

    let (input, content) = els_parser(input)?;
    let filter = FilterCondition::Not(Box::new(FilterCondition::In { fid: value, els: content }));
    Ok((input, filter))
}

/// or             = and ("OR" and)
fn parse_or(input: Span) -> IResult<FilterCondition> {
    let (input, first_filter) = parse_and(input)?;
    // if we found a `OR` then we MUST find something next
    let (input, mut ors) =
        many0(preceded(ws(tuple((tag("OR"), multispace1))), cut(parse_and)))(input)?;

    let filter = if ors.is_empty() {
        first_filter
    } else {
        ors.insert(0, first_filter);
        FilterCondition::Or(ors)
    };

    Ok((input, filter))
}

/// and            = not ("AND" not)*
fn parse_and(input: Span) -> IResult<FilterCondition> {
    let (input, first_filter) = parse_not(input)?;
    // if we found a `AND` then we MUST find something next
    let (input, mut ands) =
        many0(preceded(ws(tuple((tag("AND"), multispace1))), cut(parse_not)))(input)?;

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
fn parse_not(input: Span) -> IResult<FilterCondition> {
    alt((
        map(preceded(ws(tuple((tag("NOT"), multispace1))), cut(parse_not)), |e| {
            FilterCondition::Not(Box::new(e))
        }),
        parse_primary,
    ))(input)
}

/// geoRadius      = WS* "_geoRadius(float WS* "," WS* float WS* "," WS* float)
/// If we parse `_geoRadius` we MUST parse the rest of the expression.
fn parse_geo_radius(input: Span) -> IResult<FilterCondition> {
    // we want to allow space BEFORE the _geoRadius but not after
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

/// primary        = (WS* "(" WS* expression WS* ")" WS*) | geoRadius | condition | exists | not_exists | to
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
        parse_in,
        parse_not_in,
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

/// filter     = expression EOF
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
                "colour IN[]",
                Fc::In { 
                    fid: rtok("", "colour"), 
                    els: vec![] 
                }
            ),
            (
                "colour IN[green]",
                Fc::In { 
                    fid: rtok("", "colour"), 
                    els: vec![rtok("colour IN[", "green")] 
                }
            ),
            (
                "colour IN[green,]",
                Fc::In { 
                    fid: rtok("", "colour"), 
                    els: vec![rtok("colour IN[", "green")] 
                }
            ),
            (
                "colour IN[green,blue]",
                Fc::In { 
                    fid: rtok("", "colour"), 
                    els: vec![
                        rtok("colour IN[", "green"),
                        rtok("colour IN[green, ", "blue"),
                    ] 
                }
            ),
            (
                "colour NOT IN[green,blue]",
                Fc::Not(Box::new(Fc::In { 
                    fid: rtok("", "colour"), 
                    els: vec![
                        rtok("colour NOT IN[", "green"),
                        rtok("colour NOT IN[green, ", "blue"),
                    ] 
                }))
            ),
            (
                " colour IN [  green , blue , ]",
                Fc::In { 
                    fid: rtok(" ", "colour"), 
                    els: vec![
                        rtok("colour IN [  ", "green"),
                        rtok("colour IN [  green , ", "blue"),
                    ] 
                }
            ),
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
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "channel"),
                    op: Condition::Equal(rtok("NOT channel = ", "ponce")),
                })),
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
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::LowerThan(rtok("NOT subscribers < ", "1000")),
                })),
            ),
            (
                "NOT subscribers > 1000",
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::GreaterThan(rtok("NOT subscribers > ", "1000")),
                })),
            ),
            (
                "NOT subscribers <= 1000",
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::LowerThanOrEqual(rtok("NOT subscribers <= ", "1000")),
                })),
            ),
            (
                "NOT subscribers >= 1000",
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::GreaterThanOrEqual(rtok("NOT subscribers >= ", "1000")),
                })),
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
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::Exists,
                })),
            ),
            (
                "subscribers NOT EXISTS",
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::Exists,
                })),
            ),
            (
                "NOT subscribers NOT EXISTS",
                Fc::Not(Box::new(Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::Exists,
                })))),
            ),
            (
                "subscribers NOT   EXISTS",
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("", "subscribers"),
                    op: Condition::Exists,
                })),
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
                Fc::Not(Box::new(Fc::Condition {
                    fid: rtok("NOT ", "subscribers"),
                    op: Condition::Between {
                        from: rtok("NOT subscribers ", "100"),
                        to: rtok("NOT subscribers 100 TO ", "1000"),
                    },
                })),
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
                Fc::Not(Box::new(Fc::GeoLowerThan {
                    point: [rtok("NOT _geoRadius(", "12"), rtok("NOT _geoRadius(12, ", "13")],
                    radius: rtok("NOT _geoRadius(12, 13, ", "14"),
                })),
            ),
            // test simple `or` and `and`
            (
                "channel = ponce AND 'dog race' != 'bernese mountain'",
                Fc::And(vec![
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
                ]),
            ),
            (
                "channel = ponce OR 'dog race' != 'bernese mountain'",
                Fc::Or(vec![
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
                ]),
            ),
            (
                "channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000",
                Fc::Or(vec![
                    Fc::And(vec![
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
                    ])
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
                ]),
            ),
            // test parenthesis
            (
                    "channel = ponce AND ( 'dog race' != 'bernese mountain' OR subscribers > 1000 )",
                    Fc::And(vec![
                        Fc::Condition { fid: rtok("", "channel"), op: Condition::Equal(rtok("channel = ", "ponce")) }.into(),
                        Fc::Or(vec![
                          Fc::Condition { fid: rtok("channel = ponce AND ( '", "dog race"), op: Condition::NotEqual(rtok("channel = ponce AND ( 'dog race' != '", "bernese mountain"))}.into(),
                            Fc::Condition { fid: rtok("channel = ponce AND ( 'dog race' != 'bernese mountain' OR ", "subscribers"), op: Condition::GreaterThan(rtok("channel = ponce AND ( 'dog race' != 'bernese mountain' OR subscribers > ", "1000")) }.into(),]
                    ).into()]),
            ),
            (
                "(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, 13, 14)",
                Fc::And(vec![
                    Fc::Or(vec![
                        Fc::And(vec![
                            Fc::Condition { fid: rtok("(", "channel"), op: Condition::Equal(rtok("(channel = ", "ponce")) }.into(),
                            Fc::Condition { fid: rtok("(channel = ponce AND '", "dog race"), op: Condition::NotEqual(rtok("(channel = ponce AND 'dog race' != '", "bernese mountain")) }.into(),
                        ]).into(),
                        Fc::Condition { fid: rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR ", "subscribers"), op: Condition::GreaterThan(rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > ", "1000")) }.into(),
                    ]).into(),
                    Fc::GeoLowerThan { point: [rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(", "12"), rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, ", "13")], radius: rtok("(channel = ponce AND 'dog race' != 'bernese mountain' OR subscribers > 1000) AND _geoRadius(12, 13, ", "14") }.into()
                ])
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
            ("channel = Ponce OR", "Found unexpected characters at the end of the filter: `OR`. You probably forgot an `OR` or an `AND` rule."),
            ("_geoRadius", "The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`."),
            ("_geoRadius = 12", "The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`."),
            ("_geoPoint(12, 13, 14)", "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance) built-in rule to filter on `_geo` coordinates."),
            ("position <= _geoPoint(12, 13, 14)", "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance) built-in rule to filter on `_geo` coordinates."),
            ("position <= _geoRadius(12, 13, 14)", "The `_geoRadius` filter is an operation and can't be used as a value."),
            ("channel = 'ponce", "Expression `\\'ponce` is missing the following closing delimiter: `'`."),
            ("channel = \"ponce", "Expression `\\\"ponce` is missing the following closing delimiter: `\"`."),
            ("channel = mv OR (followers >= 1000", "Expression `(followers >= 1000` is missing the following closing delimiter: `)`."),
            ("channel = mv OR followers >= 1000)", "Found unexpected characters at the end of the filter: `)`. You probably forgot an `OR` or an `AND` rule."),
            ("colour NOT EXIST", "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `colour NOT EXIST`."),
            ("subscribers 100 TO1000", "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `subscribers 100 TO1000`."),
            ("channel = ponce ORdog != 'bernese mountain'", "Found unexpected characters at the end of the filter: `ORdog != \\'bernese mountain\\'`. You probably forgot an `OR` or an `AND` rule."),
            ("channel = ponce AND'dog' != 'bernese mountain'", "Found unexpected characters at the end of the filter: `AND\\'dog\\' != \\'bernese mountain\\'`. You probably forgot an `OR` or an `AND` rule."),
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
        assert!(filter.token_at_depth(1).is_some());
        assert!(filter.token_at_depth(2).is_none());

        let filter = FilterCondition::parse("(account_ids=1 OR (account_ids=2 AND account_ids=3) OR (account_ids=4 AND account_ids=5) OR account_ids=6)").unwrap().unwrap();
        assert!(filter.token_at_depth(2).is_some());
        assert!(filter.token_at_depth(3).is_none());

        let filter = FilterCondition::parse("account_ids=1 OR account_ids=2 AND account_ids=3 OR account_ids=4 AND account_ids=5 OR account_ids=6").unwrap().unwrap();
        assert!(filter.token_at_depth(2).is_some());
        assert!(filter.token_at_depth(3).is_none());
    }
}
