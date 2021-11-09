use nom::branch::alt;
use nom::bytes::complete::{take_till, take_while, take_while1};
use nom::character::complete::{char, multispace0};
use nom::combinator::cut;
use nom::sequence::{delimited, terminated};

use crate::error::NomErrorExt;
use crate::{parse_geo_point, parse_geo_radius, Error, ErrorKind, IResult, Span, Token};

/// value          = WS* ~ ( word | singleQuoted | doubleQuoted) ~ WS*
pub fn parse_value(input: Span) -> IResult<Token> {
    // to get better diagnostic message we are going to strip the left whitespaces from the input right now
    let (input, _) = take_while(char::is_whitespace)(input)?;

    // then, we want to check if the user is misusing a geo expression
    // This expression can’t finish without error.
    // We want to return an error in case of failure.
    if let Err(err) = parse_geo_point(input) {
        if err.is_failure() {
            return Err(err);
        }
    }
    match parse_geo_radius(input) {
        Ok(_) => return Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::MisusedGeo))),
        // if we encountered a failure it means the user badly wrote a _geoRadius filter.
        // But instead of showing him how to fix his syntax we are going to tell him he should not use this filter as a value.
        Err(e) if e.is_failure() => {
            return Err(nom::Err::Failure(Error::new_from_kind(input, ErrorKind::MisusedGeo)))
        }
        _ => (),
    }

    // singleQuoted   = "'" .* all but quotes "'"
    let simple_quoted = take_till(|c: char| c == '\'');
    // doubleQuoted   = "\"" (word | spaces)* "\""
    let double_quoted = take_till(|c: char| c == '"');
    // word           = (alphanumeric | _ | - | .)+
    let word = take_while1(is_value_component);

    // this parser is only used when an error is encountered and it parse the
    // largest string possible that do not contain any “language” syntax.
    // If we try to parse `name = 🦀 AND language = rust` we want to return an
    // error saying we could not parse `🦀`. Not that no value were found or that
    // we could note parse `🦀 AND language = rust`.
    // we want to remove the space before entering the alt because if we don't,
    // when we create the errors from the output of the alt we have spaces everywhere
    let error_word = take_till::<_, _, Error>(is_syntax_component);

    terminated(
        alt((
            delimited(char('\''), cut(simple_quoted), cut(char('\''))),
            delimited(char('"'), cut(double_quoted), cut(char('"'))),
            word,
        )),
        multispace0,
    )(input)
    .map(|(s, t)| (s, t.into()))
    // if we found nothing in the alt it means the user specified something that was not recognized as a value
    .map_err(|e: nom::Err<Error>| {
        e.map_err(|_| Error::new_from_kind(error_word(input).unwrap().1, ErrorKind::ExpectedValue))
    })
    // if we found encountered a failure it means the user really tried to input a value, but had an unmatched quote
    .map_err(|e| {
        e.map_fail(|c| Error::new_from_kind(input, ErrorKind::MissingClosingDelimiter(c.char())))
    })
}

fn is_value_component(c: char) -> bool {
    c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
}

fn is_syntax_component(c: char) -> bool {
    c.is_whitespace() || ['(', ')', '=', '<', '>'].contains(&c)
}

#[cfg(test)]
pub mod test {
    use nom::Finish;

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
            ("I'm tamo", rtok("'m tamo", "I")),
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

    #[test]
    fn diagnostic() {
        let test_case = [
            ("🦀", "🦀"),
            ("     🦀", "🦀"),
            ("🦀 AND crab = truc", "🦀"),
            ("🦀_in_name", "🦀_in_name"),
            (" (name = ...", ""),
        ];

        for (input, expected) in test_case {
            let input = Span::new_extra(input, input);
            let result = parse_value(input);

            assert!(
                result.is_err(),
                "Filter `{}` wasn’t supposed to be parsed but it did with the following result: `{:?}`",
                expected,
                result.unwrap()
            );
            // get the inner string referenced in the error
            let value = *result.finish().unwrap_err().context().fragment();
            assert_eq!(value, expected, "Filter `{}` was supposed to fail with the following value: `{}`, but it failed with: `{}`.", input, expected, value);
        }
    }
}
