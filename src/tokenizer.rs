use slice_group_by::StrGroupBy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    Word,
    Space,
}

pub fn simple_tokenizer(text: &str) -> impl Iterator<Item=(TokenType, &str)> {
    text
        .linear_group_by_key(|c| c.is_alphanumeric())
        .map(|s| {
            let first = s.chars().next().unwrap();
            let type_ = if first.is_alphanumeric() { TokenType::Word } else { TokenType::Space };
            (type_, s)
        })
}

pub fn only_token((t, w): (TokenType, &str)) -> Option<&str> {
    if t == TokenType::Word { Some(w) } else { None }
}
