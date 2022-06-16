use nom::branch::alt;
use nom::bytes::complete::{take_till, take_while, take_while1};
use nom::character::complete::{char, multispace0};
use nom::combinator::cut;
use nom::sequence::{delimited, terminated};
use nom::{InputIter, InputLength, InputTake, Slice};

use crate::error::NomErrorExt;
use crate::{parse_geo_point, parse_geo_radius, Error, ErrorKind, IResult, Span, Token};

/// This function goes through all characters in the [Span] if it finds any escaped character (`\`).
/// It generates a new string with all `\` removed from the [Span].
fn unescape(buf: Span, char_to_escape: char) -> String {
    let to_escape = format!("\\{}", char_to_escape);
    buf.replace(&to_escape, &char_to_escape.to_string())
}

/// Parse a value in quote. If it encounter an escaped quote it'll unescape it.
fn quoted_by(quote: char, input: Span) -> IResult<Token> {
    // empty fields / values are valid in json
    if input.is_empty() {
        return Ok((input.slice(input.input_len()..), input.into()));
    }

    let mut escaped = false;
    let mut i = input.iter_indices();

    while let Some((idx, c)) = i.next() {
        if c == quote {
            let (rem, output) = input.take_split(idx);
            return Ok((rem, Token::new(output, escaped.then(|| unescape(output, quote)))));
        } else if c == '\\' {
            if let Some((_, c)) = i.next() {
                escaped |= c == quote;
            } else {
                return Err(nom::Err::Error(Error::new_from_kind(
                    input,
                    ErrorKind::MalformedValue,
                )));
            }
        }
        // if it was preceeded by a `\` or if it was anything else we can continue to advance
    }

    Ok((
        input.slice(input.input_len()..),
        Token::new(input, escaped.then(|| unescape(input, quote))),
    ))
}

/// value          = WS* ( word | singleQuoted | doubleQuoted) WS+
pub fn parse_value<'a>(input: Span<'a>) -> IResult<Token<'a>> {
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

    // word           = (alphanumeric | _ | - | .)+
    let word = |input: Span<'a>| -> IResult<Token<'a>> {
        take_while1(is_value_component)(input).map(|(s, t)| (s, t.into()))
    };

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
            delimited(char('\''), cut(|input| quoted_by('\'', input)), cut(char('\''))),
            delimited(char('"'), cut(|input| quoted_by('"', input)), cut(char('"'))),
            word,
        )),
        multispace0,
    )(input)
    // if we found nothing in the alt it means the user specified something that was not recognized as a value
    .map_err(|e: nom::Err<Error>| {
        e.map_err(|_| Error::new_from_kind(error_word(input).unwrap().1, ErrorKind::ExpectedValue))
    })
    .map_err(|e| {
        e.map_fail(|failure| {
            // if we found encountered a char failure it means the user had an unmatched quote
            if matches!(failure.kind(), ErrorKind::Char(_)) {
                Error::new_from_kind(input, ErrorKind::MissingClosingDelimiter(failure.char()))
            } else {
                // else we let the failure untouched
                failure
            }
        })
    })
}

fn is_value_component(c: char) -> bool {
    c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
}

fn is_syntax_component(c: char) -> bool {
    c.is_whitespace() || ['(', ')', '=', '<', '>', '!'].contains(&c)
}

#[cfg(test)]
pub mod test {
    use nom::Finish;

    use super::*;
    use crate::tests::rtok;

    #[test]
    fn test_span() {
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
            ("\"I'm \\\"super\\\" tamo\"", rtok("\"", "I'm \\\"super\\\" tamo")),
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
            let token = result.unwrap().1;
            assert_eq!(token, expected, "Filter `{}` failed.", input);
        }
    }

    #[test]
    fn test_escape_inside_double_quote() {
        // (input, remaining, expected output token, output value)
        let test_case = [
            ("aaaa", "", rtok("", "aaaa"), "aaaa"),
            (r#"aa"aa"#, r#""aa"#, rtok("", "aa"), "aa"),
            (r#"aa\"aa"#, r#""#, rtok("", r#"aa\"aa"#), r#"aa"aa"#),
            (r#"aa\\\aa"#, r#""#, rtok("", r#"aa\\\aa"#), r#"aa\\\aa"#),
            (r#"aa\\"\aa"#, r#""\aa"#, rtok("", r#"aa\\"#), r#"aa\\"#),
            (r#"aa\\\"\aa"#, r#""#, rtok("", r#"aa\\\"\aa"#), r#"aa\\"\aa"#),
            (r#"\"\""#, r#""#, rtok("", r#"\"\""#), r#""""#),
        ];

        for (input, remaining, expected_tok, expected_val) in test_case {
            let span = Span::new_extra(input, "");
            let result = quoted_by('"', span);
            assert!(result.is_ok());

            let (rem, output) = result.unwrap();
            assert_eq!(rem.to_string(), remaining);
            assert_eq!(output, expected_tok);
            assert_eq!(output.value(), expected_val.to_string());
        }
    }

    #[test]
    fn test_unescape() {
        // double quote
        assert_eq!(
            unescape(Span::new_extra(r#"Hello \"World\""#, ""), '"'),
            r#"Hello "World""#.to_string()
        );
        assert_eq!(
            unescape(Span::new_extra(r#"Hello \\\"World\\\""#, ""), '"'),
            r#"Hello \\"World\\""#.to_string()
        );
        // simple quote
        assert_eq!(
            unescape(Span::new_extra(r#"Hello \'World\'"#, ""), '\''),
            r#"Hello 'World'"#.to_string()
        );
        assert_eq!(
            unescape(Span::new_extra(r#"Hello \\\'World\\\'"#, ""), '\''),
            r#"Hello \\'World\\'"#.to_string()
        );
    }

    #[test]
    fn test_value() {
        let test_case = [
            // (input, expected value, if a string was generated to hold the new value)
            ("channel", "channel", false),
            // All the base test, no escaped string should be generated
            (".private", ".private", false),
            ("I-love-kebab", "I-love-kebab", false),
            ("but_snakes_is_also_good", "but_snakes_is_also_good", false),
            ("parens(", "parens", false),
            ("parens)", "parens", false),
            ("not!", "not", false),
            ("    channel", "channel", false),
            ("channel     ", "channel", false),
            ("    channel     ", "channel", false),
            ("'channel'", "channel", false),
            ("\"channel\"", "channel", false),
            ("'cha)nnel'", "cha)nnel", false),
            ("'cha\"nnel'", "cha\"nnel", false),
            ("\"cha'nnel\"", "cha'nnel", false),
            ("\" some spaces \"", " some spaces ", false),
            ("\"cha'nnel\"", "cha'nnel", false),
            ("\"cha'nnel\"", "cha'nnel", false),
            ("I'm tamo", "I", false),
            // escaped thing but not quote
            (r#""\\""#, r#"\\"#, false),
            (r#""\\\\\\""#, r#"\\\\\\"#, false),
            (r#""aa\\aa""#, r#"aa\\aa"#, false),
            // with double quote
            (r#""Hello \"world\"""#, r#"Hello "world""#, true),
            (r#""Hello \\\"world\\\"""#, r#"Hello \\"world\\""#, true),
            (r#""I'm \"super\" tamo""#, r#"I'm "super" tamo"#, true),
            (r#""\"\"""#, r#""""#, true),
            // with simple quote
            (r#"'Hello \'world\''"#, r#"Hello 'world'"#, true),
            (r#"'Hello \\\'world\\\''"#, r#"Hello \\'world\\'"#, true),
            (r#"'I\'m "super" tamo'"#, r#"I'm "super" tamo"#, true),
            (r#"'\'\''"#, r#"''"#, true),
        ];

        for (input, expected, escaped) in test_case {
            let input = Span::new_extra(input, input);
            let result = parse_value(input);

            assert!(
                result.is_ok(),
                "Filter `{:?}` was supposed to be parsed but failed with the following error: `{}`",
                expected,
                result.unwrap_err()
            );
            let token = result.unwrap().1;
            assert_eq!(
                token.value.is_some(),
                escaped,
                "Filter `{}` was not supposed to be escaped",
                input
            );
            assert_eq!(token.value(), expected, "Filter `{}` failed.", input);
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
