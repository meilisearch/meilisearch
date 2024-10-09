use charabia::{SeparatorKind, Token, TokenKind};

pub enum SimpleTokenKind {
    Separator(SeparatorKind),
    NotSeparator,
}

impl SimpleTokenKind {
    pub fn new(token: &&Token<'_>) -> Self {
        match token.kind {
            TokenKind::Separator(separaor_kind) => Self::Separator(separaor_kind),
            _ => Self::NotSeparator,
        }
    }
}
