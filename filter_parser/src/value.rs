use nom::branch::alt;
use nom::bytes::complete::{take_till, take_while, take_while1};
use nom::character::complete::{char, multispace0};
use nom::combinator::cut;
use nom::sequence::{delimited, terminated};

use crate::error::ExtendNomError;
use crate::{parse_geo_point, parse_geo_radius, Error, ErrorKind, IResult, Span, Token};

/// value          = WS* ~ ( word | singleQuoted | doubleQuoted) ~ WS*
pub fn parse_value(input: Span) -> IResult<Token> {
    // before anything we want to check if the user is misusing a geo expression
    let err = parse_geo_point(input).unwrap_err();
    if err.is_failure() {
        return Err(err);
    }
    match parse_geo_radius(input) {
        Ok(_) => return Err(nom::Err::Failure(Error::kind(input, ErrorKind::MisusedGeo))),
        // if we encountered a failure it means the user badly wrote a _geoRadius filter.
        // But instead of showing him how to fix his syntax we are going to tell him he should not use this filter as a value.
        Err(e) if e.is_failure() => {
            return Err(nom::Err::Failure(Error::kind(input, ErrorKind::MisusedGeo)))
        }
        _ => (),
    }

    // singleQuoted   = "'" .* all but quotes "'"
    let simple_quoted = |input| take_till(|c: char| c == '\'')(input);
    // doubleQuoted   = "\"" (word | spaces)* "\""
    let double_quoted = |input| take_till(|c: char| c == '"')(input);
    // word           = (alphanumeric | _ | - | .)+
    let word = |input| take_while1(is_key_component)(input);

    // we want to remove the space before entering the alt because if we don't,
    // when we create the errors from the output of the alt we have spaces everywhere
    let (input, _) = take_while(char::is_whitespace)(input)?;

    terminated(
        alt((
            delimited(char('\''), simple_quoted, cut(char('\''))),
            delimited(char('"'), double_quoted, cut(char('"'))),
            word,
        )),
        multispace0,
    )(input)
    .map(|(s, t)| (s, t.into()))
    // if we found nothing in the alt it means the user did not input any value
    .map_err(|e| e.map_err(|_| Error::kind(input, ErrorKind::ExpectedValue)))
    // if we found encountered a failure it means the user really tried to input a value, but had an unmatched quote
    .map_err(|e| e.map_fail(|c| Error::kind(input, ErrorKind::MissingClosingDelimiter(c.char()))))
}

fn is_key_component(c: char) -> bool {
    c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::tests::rtok;

    #[test]
    fn name() {
        let test_case = [
            ("channel", rtok("", "channel")),
            (".private", rtok("", ".private")),
            ("I-love-kebab", rtok("", "I-love-kebab")),
            ("but_snakes_is_also_good", rtok("", "but_snakes_is_also_good")),
            ("parens(", rtok("", "parens")),
            ("parens)", rtok("", "parens")),
            ("not!", rtok("", "not")),
            ("    channel", rtok("    ", "channel")),
            ("channel     ", rtok("", "channel")),
            ("    channel     ", rtok("    ", "channel")),
            ("'channel'", rtok("'", "channel")),
            ("\"channel\"", rtok("\"", "channel")),
            ("'cha)nnel'", rtok("'", "cha)nnel")),
            ("'cha\"nnel'", rtok("'", "cha\"nnel")),
            ("\"cha'nnel\"", rtok("\"", "cha'nnel")),
            ("\" some spaces \"", rtok("\"", " some spaces ")),
            ("\"cha'nnel\"", rtok("'", "cha'nnel")),
            ("\"cha'nnel\"", rtok("'", "cha'nnel")),
        ];

        for (input, expected) in test_case {
            let input = Span::new_extra(input, input);
            let result = parse_value(input);

            assert!(
                result.is_ok(),
                "Filter `{:?}` was supposed to be parsed but failed with the following error: `{}`",
                expected,
                result.unwrap_err()
            );
            let value = result.unwrap().1;
            assert_eq!(value, expected, "Filter `{}` failed.", input);
        }
    }
}
