use std::fmt::Display;

use nom::error::{self, ParseError};
use nom::Parser;

use crate::{IResult, Span};

pub trait NomErrorExt<E> {
    fn is_failure(&self) -> bool;
    fn map_err<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E>;
    fn map_fail<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E>;
}

impl<E> NomErrorExt<E> for nom::Err<E> {
    fn is_failure(&self) -> bool {
        matches!(self, Self::Failure(_))
    }

    fn map_err<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E> {
        match self {
            e @ Self::Failure(_) => e,
            e => e.map(op),
        }
    }

    fn map_fail<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E> {
        match self {
            e @ Self::Error(_) => e,
            e => e.map(op),
        }
    }
}

/// cut a parser and map the error
pub fn cut_with_err<'a, O>(
    mut parser: impl FnMut(Span<'a>) -> IResult<O>,
    mut with: impl FnMut(Error<'a>) -> Error<'a>,
) -> impl FnMut(Span<'a>) -> IResult<O> {
    move |input| match parser.parse(input) {
        Err(nom::Err::Error(e)) => Err(nom::Err::Failure(with(e))),
        rest => rest,
    }
}

#[derive(Debug)]
pub struct Error<'a> {
    context: Span<'a>,
    kind: ErrorKind<'a>,
}

#[derive(Debug)]
pub enum ExpectedValueKind {
    ReservedKeyword,
    Other,
}

#[derive(Debug)]
pub enum ErrorKind<'a> {
    ReservedGeo(&'a str),
    Geo,
    MisusedGeo,
    InvalidPrimary,
    ExpectedEof,
    ExpectedValue(ExpectedValueKind),
    MalformedValue,
    InOpeningBracket,
    InClosingBracket,
    NonFiniteFloat,
    InExpectedValue(ExpectedValueKind),
    ReservedKeyword(String),
    MissingClosingDelimiter(char),
    Char(char),
    InternalError(error::ErrorKind),
    DepthLimitReached,
    External(String),
}

impl<'a> Error<'a> {
    pub fn kind(&self) -> &ErrorKind<'a> {
        &self.kind
    }

    pub fn context(&self) -> &Span<'a> {
        &self.context
    }

    pub fn new_from_kind(context: Span<'a>, kind: ErrorKind<'a>) -> Self {
        Self { context, kind }
    }

    pub fn new_from_external(context: Span<'a>, error: impl std::error::Error) -> Self {
        Self::new_from_kind(context, ErrorKind::External(error.to_string()))
    }

    pub fn char(self) -> char {
        match self.kind {
            ErrorKind::Char(c) => c,
            error => panic!("Internal filter parser error: {:?}", error),
        }
    }
}

impl<'a> ParseError<Span<'a>> for Error<'a> {
    fn from_error_kind(input: Span<'a>, kind: error::ErrorKind) -> Self {
        let kind = match kind {
            error::ErrorKind::Eof => ErrorKind::ExpectedEof,
            kind => ErrorKind::InternalError(kind),
        };
        Self { context: input, kind }
    }

    fn append(_input: Span<'a>, _kind: error::ErrorKind, other: Self) -> Self {
        other
    }

    fn from_char(input: Span<'a>, c: char) -> Self {
        Self { context: input, kind: ErrorKind::Char(c) }
    }
}

impl<'a> Display for Error<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let input = self.context.fragment();
        // When printing our error message we want to escape all `\n` to be sure we keep our format with the
        // first line being the diagnostic and the second line being the incriminated filter.
        let escaped_input = input.escape_debug();

        match &self.kind {
            ErrorKind::ExpectedValue(_) if input.trim().is_empty() => {
                writeln!(f, "Was expecting a value but instead got nothing.")?
            }
            ErrorKind::ExpectedValue(ExpectedValueKind::ReservedKeyword) => {
                writeln!(f, "Was expecting a value but instead got `{escaped_input}`, which is a reserved keyword. To use `{escaped_input}` as a field name or a value, surround it by quotes.")?
            }
            ErrorKind::ExpectedValue(ExpectedValueKind::Other) => {
                writeln!(f, "Was expecting a value but instead got `{}`.", escaped_input)?
            }
            ErrorKind::MalformedValue => {
                writeln!(f, "Malformed value: `{}`.", escaped_input)?
            }
            ErrorKind::MissingClosingDelimiter(c) => {
                writeln!(f, "Expression `{}` is missing the following closing delimiter: `{}`.", escaped_input, c)?
            }
            ErrorKind::InvalidPrimary if input.trim().is_empty() => {
                writeln!(f, "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` but instead got nothing.")?
            }
            ErrorKind::InvalidPrimary => {
                writeln!(f, "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, or `_geoRadius` at `{}`.", escaped_input)?
            }
            ErrorKind::ExpectedEof => {
                writeln!(f, "Found unexpected characters at the end of the filter: `{}`. You probably forgot an `OR` or an `AND` rule.", escaped_input)?
            }
            ErrorKind::Geo => {
                writeln!(f, "The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`.")?
            }
            ErrorKind::ReservedGeo(name) => {
                writeln!(f, "`{}` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance) built-in rule to filter on `_geo` coordinates.", name.escape_debug())?
            }
            ErrorKind::MisusedGeo => {
                writeln!(f, "The `_geoRadius` filter is an operation and can't be used as a value.")?
            }
            ErrorKind::ReservedKeyword(word) => {
                writeln!(f, "`{word}` is a reserved keyword and thus cannot be used as a field name unless it is put inside quotes. Use \"{word}\" or \'{word}\' instead.")?
            }
            ErrorKind::InOpeningBracket => {
                writeln!(f, "Expected `[` after `IN` keyword.")?
            }
            ErrorKind::InClosingBracket => {
                writeln!(f, "Expected matching `]` after the list of field names given to `IN[`")?
            }
            ErrorKind::NonFiniteFloat => {
                writeln!(f, "Non finite floats are not supported")?
            }
            ErrorKind::InExpectedValue(ExpectedValueKind::ReservedKeyword) => {
                writeln!(f, "Expected only comma-separated field names inside `IN[..]` but instead found `{escaped_input}`, which is a keyword. To use `{escaped_input}` as a field name or a value, surround it by quotes.")?
            }
            ErrorKind::InExpectedValue(ExpectedValueKind::Other) => {
                writeln!(f, "Expected only comma-separated field names inside `IN[..]` but instead found `{escaped_input}`.")?
            }
            ErrorKind::Char(c) => {
                panic!("Tried to display a char error with `{}`", c)
            }
            ErrorKind::DepthLimitReached => writeln!(
                f,
                "The filter exceeded the maximum depth limit. Try rewriting the filter so that it contains fewer nested conditions."
            )?,
            ErrorKind::InternalError(kind) => writeln!(
                f,
                "Encountered an internal `{:?}` error while parsing your filter. Please fill an issue", kind
            )?,
            ErrorKind::External(ref error) => writeln!(f, "{}", error)?,
        }
        let base_column = self.context.get_utf8_column();
        let size = self.context.fragment().chars().count();

        write!(f, "{}:{} {}", base_column, base_column + size, self.context.extra)
    }
}
