use unicode_linebreak::{linebreaks, BreakClass, break_property};

fn can_be_broken(c: char) -> bool {
    use BreakClass::*;

    match break_property(c as u32) {
          Ideographic
        | Alphabetic
        | Numeric
        | CombiningMark
        | WordJoiner
        | NonBreakingGlue
        | OpenPunctuation
        | Symbol
        | EmojiBase
        | EmojiModifier
        | HangulLJamo
        | HangulVJamo
        | HangulTJamo
        | RegionalIndicator
        | Quotation => false,
        _ => true,
    }
}

fn extract_token(s: &str) -> &str {
    let end = s.char_indices().rev()
        .take_while(|(_, c)| can_be_broken(*c))
        .last()
        .map(|(i, _)| i)
        .unwrap_or(s.len());

    &s[..end]
}

pub fn break_string(s: &str) -> impl Iterator<Item = &str> {
    let mut prev = 0;
    linebreaks(&s).map(move |(i, _)| {
        let s = &s[prev..i];
        prev = i;
        extract_token(s)
    })
    .filter(|s| !s.is_empty())
}
