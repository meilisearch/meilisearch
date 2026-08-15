use crate::SpanView;
use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::character::complete::multispace0;
use nom::combinator::cut;
use nom::number::complete::recognize_float;
use nom::sequence::terminated;
use nom::{IResult, Input, Parser};

#[derive(Debug, Clone, Copy)]
pub struct Token<'a> {
    span: SpanView<'a>,
    kind: TokenKind,
}

struct ParseOutput<'a> {
    parsed_token: Token<'a>,
    remaining_input: SpanView<'a>,
}

impl<'a> Token<'a> {
    /// Parses the first token of an input span, returning the parsed token and the remaining input.
    pub fn parse_next(input: SpanView<'a>) -> ParseOutput<'a> {
        let input = ignore_whitespace(input);
        // unwrap: parsing tokens can never fail
        let (remaining_input, parsed_token) = parse_token().parse(input).unwrap();
        ParseOutput { parsed_token, remaining_input }
    }

    /// Iterates over all parsed tokens from input until reaching EOF.
    pub fn iter_tokens(input: SpanView<'a>) -> impl Iterator<Item = Token<'a>> + 'a {
        let input = std::cell::Cell::new(input);
        std::iter::from_fn(move || {
            let token = (|| {
                let ParseOutput { parsed_token, remaining_input } = Self::parse_next(input.get());
                input.set(remaining_input);
                parsed_token
            })();
            Some(token)
        })
        .take_while_inclusive(|token| !matches!(token.kind, TokenKind::Eof))
    }
}

fn ignore_whitespace(input: SpanView<'_>) -> SpanView<'_> {
    match nom::character::complete::multispace0::<_, ()>(input) {
        Ok((rest, _)) => rest,
        Err(_) => input,
    }
}

fn is_value_component(c: char) -> bool {
    c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
}

fn parse_token<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    parse_all_syntax()
        .or(parse_keywords())
        .or(parse_reserved_fields())
        .or(parse_eof)
        .or(parse_value)
        .or(parse_illegal)
}

fn parse_illegal<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let (remaining, illegal) = input.take_split(1);
    Ok((remaining, Token { span: illegal, kind: TokenKind::IllegalCharacter }))
}

fn parse_keywords<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    alt((
        parse_keyword("OR", TokenKind::Or),
        parse_keyword("AND", TokenKind::And),
        parse_keyword("NOT", TokenKind::Not),
        parse_keyword("NULL", TokenKind::Null),
        parse_keyword("EXISTS", TokenKind::Exists),
        parse_keyword("IS", TokenKind::Is),
        parse_keyword("TO", TokenKind::To),
        parse_keyword("IN", TokenKind::In),
    ))
}

fn parse_all_syntax<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    alt((
        // start with two-characters syntax to ensure we don't parse them as separate tokens
        parse_syntax("!=", TokenKind::Different),
        parse_syntax(">=", TokenKind::GreaterOrEqual),
        parse_syntax("<=", TokenKind::LowerOrEqual),
        // single-char tokens
        parse_syntax("(", TokenKind::LeftParens),
        parse_syntax(")", TokenKind::RightParens),
        parse_syntax("[", TokenKind::LeftSquareBracket),
        parse_syntax("]", TokenKind::RightSquareBracket),
        parse_syntax(",", TokenKind::Comma),
        parse_syntax("=", TokenKind::Equal),
        parse_syntax(">", TokenKind::GreaterThan),
        parse_syntax("<", TokenKind::LowerThan),
    ))
}

fn parse_reserved_fields<'a>(
) -> impl Parser<SpanView<'a>, Output = Token<'a>, Error = nom::error::Error<SpanView<'a>>> {
    alt((
        parse_reserved_field("_geoRadius", TokenKind::GeoRadius),
        parse_reserved_field("_geoBoundingBox", TokenKind::GeoBoundingBox),
        parse_reserved_field("_geoPolygon", TokenKind::GeoPolygon),
        parse_reserved_field("_vectors", TokenKind::Vectors),
        parse_reserved_field("_foreign", TokenKind::Foreign),
    ))
}

/// Parse the specified keyword, which should NOT be followed by a value component
fn parse_keyword<'a>(
    keyword: &str,
    keyword_kind: TokenKind,
) -> impl Fn(SpanView<'_>) -> IResult<SpanView<'_>, Token<'_>> + '_ {
    move |input| {
        let (rest, parsed) = nom::bytes::complete::tag(keyword)(input)?;
        if let Some(next) = rest.iter_elements().next() {
            if is_value_component(next) {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        }
        Ok((rest, Token { span: parsed, kind: keyword_kind }))
    }
}

/// Parse the specified syntax character, which can be followed by anything
fn parse_syntax<'a>(
    c: &str,
    kind: TokenKind,
) -> impl Fn(SpanView<'_>) -> IResult<SpanView<'_>, Token<'_>> + '_ {
    move |input| {
        let (rest, parsed) = nom::bytes::complete::tag(c)(input)?;
        Ok((rest, Token { span: parsed, kind }))
    }
}

/// Parse the specified reserved field name, which can be NOT be followed by a value component except dot
fn parse_reserved_field<'a>(
    field: &str,
    kind: TokenKind,
) -> impl Fn(SpanView<'_>) -> IResult<SpanView<'_>, Token<'_>> + '_ {
    wip::fixme!("parse_float");
    move |input| {
        let (rest, parsed) = nom::bytes::complete::tag(field)(input)?;
        if let Some(next) = rest.iter_elements().next() {
            if next != '.' && is_value_component(next) {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
        }
        Ok((rest, Token { span: parsed, kind }))
    }
}

fn parse_eof<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let (rest, parsed) = nom::combinator::eof(input)?;
    Ok((rest, Token { span: parsed, kind: TokenKind::Eof }))
}

fn parse_value<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let value = nom::combinator::map(take_while1(is_value_component), |span| Token {
        span,
        kind: TokenKind::Value,
    });

    let float_value =
        nom::combinator::map(recognize_float, |span| Token { span, kind: TokenKind::FloatValue });

    let (rest, parsed) =
        terminated(alt((float_value, value, parse_quoted_value)), multispace0).parse(input)?;
    Ok((rest, parsed))
}

/// Inspired by <https://github.com/rust-bakery/nom/blob/main/examples/string.rs>, *sans* the output as String
/// because we're solely interested in emitting tokens at this stage
fn parse_quoted_value<'a>(input: SpanView<'a>) -> IResult<SpanView<'a>, Token<'a>> {
    let not_double_quote_slash = nom::bytes::streaming::is_not("\"\\");
    let not_single_quote_slash = nom::bytes::streaming::is_not("'\\");
    let parse_escaped_double_quote = alt((tag("\\\""), tag("\\\\")));
    let parse_escaped_single_quote = alt((tag("\\'"), tag("\\\\")));

    let parse_double_quote_fragment = alt((
        nom::combinator::map(not_double_quote_slash, |_| TokenKind::Value),
        nom::combinator::map(parse_escaped_double_quote, |_| TokenKind::Value),
        nom::combinator::map(tag("\\"), |_| TokenKind::IllegalDoubleQuoted),
    ));

    let parse_single_quote_fragment = alt((
        nom::combinator::map(not_single_quote_slash, |_| TokenKind::Value),
        nom::combinator::map(parse_escaped_single_quote, |_| TokenKind::Value),
        nom::combinator::map(tag("\\"), |_| TokenKind::IllegalSingleQuoted),
    ));

    let parse_single_quoted_value = nom::multi::fold(
        0..,
        parse_single_quote_fragment,
        || TokenKind::Value,
        |previous, kind| match (previous, kind) {
            (TokenKind::IllegalSingleQuoted, _) => TokenKind::IllegalSingleQuoted,
            (_, TokenKind::IllegalSingleQuoted) => TokenKind::IllegalSingleQuoted,
            (_, _) => kind,
        },
    );

    let parse_double_quoted_value = nom::multi::fold(
        0..,
        parse_double_quote_fragment,
        || TokenKind::Value,
        |previous, kind: TokenKind| match (previous, kind) {
            (TokenKind::IllegalDoubleQuoted, _) => TokenKind::IllegalDoubleQuoted,
            (_, TokenKind::IllegalDoubleQuoted) => TokenKind::IllegalDoubleQuoted,
            (_, _) => kind,
        },
    );

    let (rest, (first, token_kind, third)) = alt((
        (tag("'"), parse_single_quoted_value, cut(tag("'"))),
        (tag(r#"""#), parse_double_quoted_value, tag(r#"""#)),
    ))
    .parse(input)?;

    let parsed = SpanView::earliest_end(input, third);

    Ok((rest, Token { span: parsed, kind: token_kind }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    /// field name, directly or single or double quoted
    Value,
    /// Single quoted value containing illegal characters
    IllegalSingleQuoted,
    /// Double quoted value containing illegal characters
    IllegalDoubleQuoted,
    /// Illegal character
    IllegalCharacter,
    /// number
    FloatValue,
    /// (
    LeftParens,
    /// )
    RightParens,
    /// [
    LeftSquareBracket,
    /// ]
    RightSquareBracket,
    /// OR
    Or,
    /// AND
    And,
    /// NOT
    Not,
    /// IN
    In,
    /// EXISTS
    Exists,
    /// IS
    Is,
    /// NULL
    Null,
    /// TO
    To,
    /// _geoRadius
    GeoRadius,
    /// _geoBoundingBox
    GeoBoundingBox,
    /// _geoPolygon
    GeoPolygon,
    /// _vectors
    Vectors,
    /// _foreign
    Foreign,
    /// ,
    Comma,
    /// =
    Equal,
    /// !=
    Different,
    /// >
    GreaterThan,
    /// >=
    GreaterOrEqual,
    /// <
    LowerThan,
    /// <=
    LowerOrEqual,
    /// End of input
    Eof,
}

#[cfg(test)]
mod test {
    use super::Token;
    use crate::{FilterSource, Link, SpanView};

    #[test]
    fn list_tokens() {
        let source = FilterSource {
            label: "".into(),
            source:
                r#"(doggo.name = Intel AND doggo.age > 12) OR "with spaces" != "some \"  \\ value" "#
                    .into(),
            previous_link: Link::And,
            next_down: 0,
        };
        let view = SpanView::from_entire_source(0, &source);

        for token in Token::iter_tokens(view) {
            dbg!(token);
        }
    }

    #[test]
    fn fail_tokens() {
        let source = FilterSource {
            label: "".into(),
            source: "illegal {}".into(),
            previous_link: Link::And,
            next_down: 0,
        };
        let view = SpanView::from_entire_source(0, &source);

        for token in Token::iter_tokens(view).take(10) {
            dbg!(token);
        }
    }
}
