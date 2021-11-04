use std::fmt::Display;

use nom::{Parser, error::{self, ParseError}};

use crate::{IResult, Span};

pub trait ExtendNomError<E> {
    fn is_failure(&self) -> bool;
    fn map_err<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E>;
    fn map_fail<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E>;
}

impl<E> ExtendNomError<E> for nom::Err<E> {
    fn is_failure(&self) -> bool {
        matches!(self, Self::Failure(_))
    }

    fn map_err<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E> {
        match self {
            e @ Self::Failure(_) => e,
            e => e.map(|e| op(e)),
        }
    }

    fn map_fail<O: FnOnce(E) -> E>(self, op: O) -> nom::Err<E> {
        match self {
            e @ Self::Error(_) => e,
            e => e.map(|e| op(e)),
        }
    }
}

/// cut a parser and map the error
pub fn cut_with_err<'a, O>(mut parser: impl FnMut(Span<'a>) -> IResult<O>, mut with: impl FnMut(Error<'a>) -> Error<'a>) -> impl FnMut(Span<'a>) -> IResult<O> {
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
pub enum ErrorKind<'a> {
    ReservedGeo(&'a str),
    Geo,
    MisusedGeo,
    InvalidPrimary,
    ReservedKeyword,
    ExpectedEof,
    ExpectedValue,
    MissingClosingDelimiter(char),
    UnexpectedInput(Vec<&'a str>),
    Context(&'a str),
    Char(char),
    Unreachable,
}

impl<'a> Error<'a> {
    pub fn kind(context: Span<'a>, kind: ErrorKind<'a>) -> Self {
        Self { context, kind }
    }
    pub fn char(self) -> char {
        match self.kind {
            ErrorKind::Char(c) => c,
            _ => panic!("Internal filter parser error"),
            }
        }
}

impl<'a> ParseError<Span<'a>> for Error<'a> {
    fn from_error_kind(input: Span<'a>, kind: error::ErrorKind) -> Self {
        let kind = match kind {
            error::ErrorKind::Eof => ErrorKind::ExpectedEof,
            error::ErrorKind::Tag => ErrorKind::UnexpectedInput(Vec::new()),
            error::ErrorKind::MapRes => todo!(),
            error::ErrorKind::MapOpt => todo!(),
            error::ErrorKind::Alt => todo!(),
            error::ErrorKind::IsNot => todo!(),
            error::ErrorKind::IsA => todo!(),
            error::ErrorKind::SeparatedList => todo!(),
            error::ErrorKind::SeparatedNonEmptyList => todo!(),
            error::ErrorKind::Many0 => todo!(),
            error::ErrorKind::Many1 => todo!(),
            error::ErrorKind::ManyTill => todo!(),
            error::ErrorKind::Count => todo!(),
            error::ErrorKind::TakeUntil => todo!(),
            error::ErrorKind::LengthValue => todo!(),
            error::ErrorKind::TagClosure => todo!(),
            error::ErrorKind::Alpha => todo!(),
            error::ErrorKind::Digit => todo!(),
            error::ErrorKind::HexDigit => todo!(),
            error::ErrorKind::OctDigit => todo!(),
            error::ErrorKind::AlphaNumeric => todo!(),
            error::ErrorKind::Space => todo!(),
            error::ErrorKind::MultiSpace => todo!(),
            error::ErrorKind::LengthValueFn => todo!(),
            error::ErrorKind::Switch => todo!(),
            error::ErrorKind::TagBits => todo!(),
            error::ErrorKind::OneOf => todo!(),
            error::ErrorKind::NoneOf => todo!(),
            error::ErrorKind::Char => todo!(),
            error::ErrorKind::CrLf => todo!(),
            error::ErrorKind::RegexpMatch => todo!(),
            error::ErrorKind::RegexpMatches => todo!(),
            error::ErrorKind::RegexpFind => todo!(),
            error::ErrorKind::RegexpCapture => todo!(),
            error::ErrorKind::RegexpCaptures => todo!(),
            error::ErrorKind::TakeWhile1 => ErrorKind::Unreachable,
            error::ErrorKind::Complete => todo!(),
            error::ErrorKind::Fix => todo!(),
            error::ErrorKind::Escaped => todo!(),
            error::ErrorKind::EscapedTransform => todo!(),
            error::ErrorKind::NonEmpty => todo!(),
            error::ErrorKind::ManyMN => todo!(),
            error::ErrorKind::Not => todo!(),
            error::ErrorKind::Permutation => todo!(),
            error::ErrorKind::Verify => todo!(),
            error::ErrorKind::TakeTill1 => todo!(),
            error::ErrorKind::TakeWhileMN => todo!(),
            error::ErrorKind::TooLarge => todo!(),
            error::ErrorKind::Many0Count => todo!(),
            error::ErrorKind::Many1Count => todo!(),
            error::ErrorKind::Float => todo!(),
            error::ErrorKind::Satisfy => todo!(),
            error::ErrorKind::Fail => todo!(),
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

        match self.kind {
            ErrorKind::ExpectedValue if input.trim().is_empty() => {
                writeln!(f, "Was expecting a value but instead got nothing.")?
            }
            ErrorKind::MissingClosingDelimiter(c) => { 
                writeln!(f, "Expression `{}` is missing the following closing delimiter: `{}`.", input, c)?
            }
            ErrorKind::ExpectedValue => {
                writeln!(f, "Was expecting a value but instead got `{}`.", input)?
            }
            ErrorKind::InvalidPrimary if input.trim().is_empty() => {
                writeln!(f, "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO` or `_geoRadius` but instead got nothing.")?
            }
            ErrorKind::InvalidPrimary => {
                writeln!(f, "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `TO` or `_geoRadius` at `{}`.", input)?
            }
            ErrorKind::ExpectedEof => {
                writeln!(f, "Found unexpected characters at the end of the filter: `{}`. You probably forgot an `OR` or an `AND` rule.", input)?
            }
            ErrorKind::Geo => {
                writeln!(f, "The `_geoRadius` filter expects three arguments: `_geoRadius(latitude, longitude, radius)`.")?
            }
            ErrorKind::ReservedGeo(name) => {
                writeln!(f, "`{}` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance) built-in rule to filter on `_geo` coordinates.", name)?
            }
            ErrorKind::MisusedGeo => {
                writeln!(f, "The `_geoRadius` filter is an operation and can't be used as a value.")?
            }
            ErrorKind::Char(c) => {
                panic!("Tried to display a char error with `{}`", c)
            }
            ErrorKind::ReservedKeyword => writeln!(f, "reserved keyword")?,
            ErrorKind::UnexpectedInput(ref v) => writeln!(f, "Unexpected input found `{}`, vec: `{:?}`", input, v)?,
            ErrorKind::Context(_) => todo!(),
            ErrorKind::Unreachable => writeln!(
                f,
                "Encountered an internal error while parsing your filter. Please fill an issue"
            )?,
        }
        write!(
            f,
            "{}:{} in `{}`.",
            self.context.location_line(),
            self.context.get_utf8_column(),
            self.context.extra,
        )
    }
}
