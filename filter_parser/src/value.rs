use nom::branch::alt;
use nom::bytes::complete::{take_till, take_while1};
use nom::character::complete::char;
use nom::sequence::delimited;
use nom::IResult;

use crate::{ws, FPError, Span, Token};

/// value          = WS* ~ ( word | singleQuoted | doubleQuoted) ~ WS*
pub fn parse_value<'a, E: FPError<'a>>(input: Span<'a>) -> IResult<Span<'a>, Token, E> {
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
    .map(|(s, t)| (s, t.into()))
}

fn is_key_component(c: char) -> bool {
    c.is_alphanumeric() || ['_', '-', '.'].contains(&c)
}

#[cfg(test)]
pub mod tests {
    use nom::error::Error;

    use super::*;
    use crate::tests::rtok;

    #[test]
    fn name() {
        let test_case = [
            ("channel", rtok("", "channel")),
            (".private", rtok("", ".private")),
            ("I-love-kebab", rtok("", "I-love-kebab")),
            ("but_snakes_are_also_good", rtok("", "but_snakes_are_also_good")),
            ("parens(", rtok("", "parens")),
            ("parens)", rtok("", "parens")),
            ("not!", rtok("", "not")),
            ("    channel", rtok("    ", "channel")),
            ("channel     ", rtok("", "channel")),
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
            let result = parse_value::<Error<Span>>(input);

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
