#[derive(Debug, PartialEq, Eq)]
pub enum QueryWord<'a> {
    Free(&'a str),
    Quoted(&'a str),
}

pub fn alphanumeric_quoted_tokens(string: &str) -> impl Iterator<Item = QueryWord> {
    use QueryWord::{Quoted, Free};

    enum State {
        Free(usize),
        Quoted(usize),
        Fused,
    }

    impl State {
        fn is_quoted(&self) -> bool {
            match self { State::Quoted(_) => true, _ => false }
        }

        fn replace_by(&mut self, state: State) -> State {
            std::mem::replace(self, state)
        }
    }

    let mut state = State::Free(0);
    let mut string_chars = string.char_indices();
    std::iter::from_fn(move || {
        loop {
            let (i, afteri, c) = match string_chars.next() {
                Some((i, c)) => (i, i + c.len_utf8(), c),
                None => return match state.replace_by(State::Fused) {
                    State::Free(s) => if !string[s..].is_empty() {
                        Some(Free(&string[s..]))
                    } else {
                        None
                    },
                    State::Quoted(s) => Some(Quoted(&string[s..])),
                    State::Fused => None,
                },
            };

            if c == '"' {
                match state.replace_by(State::Free(afteri)) {
                    State::Quoted(s) => return Some(Quoted(&string[s..i])),
                    State::Free(s) => {
                        state = State::Quoted(afteri);
                        if i > s { return Some(Free(&string[s..i])) }
                    },
                    State::Fused => return None,
                }
            }
            else if !state.is_quoted() && !c.is_alphanumeric() {
                match state.replace_by(State::Free(afteri)) {
                    State::Free(s) if i > s => return Some(Free(&string[s..i])),
                    _ => state = State::Free(afteri),
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_quoted_string() {
        use QueryWord::Quoted;

        let mut iter = alphanumeric_quoted_tokens("\"hello\"");
        assert_eq!(iter.next(), Some(Quoted("hello")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn one_pending_quoted_string() {
        use QueryWord::Quoted;

        let mut iter = alphanumeric_quoted_tokens("\"hello");
        assert_eq!(iter.next(), Some(Quoted("hello")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn one_non_quoted_string() {
        use QueryWord::Free;

        let mut iter = alphanumeric_quoted_tokens("hello");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn quoted_directly_followed_by_free_strings() {
        use QueryWord::{Quoted, Free};

        let mut iter = alphanumeric_quoted_tokens("\"hello\"world");
        assert_eq!(iter.next(), Some(Quoted("hello")));
        assert_eq!(iter.next(), Some(Free("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn free_directly_followed_by_quoted_strings() {
        use QueryWord::{Quoted, Free};

        let mut iter = alphanumeric_quoted_tokens("hello\"world\"");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Quoted("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn free_followed_by_quoted_strings() {
        use QueryWord::{Quoted, Free};

        let mut iter = alphanumeric_quoted_tokens("hello \"world\"");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Quoted("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multiple_spaces_separated_strings() {
        use QueryWord::Free;

        let mut iter = alphanumeric_quoted_tokens("hello    world   ");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Free("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multi_interleaved_quoted_free_strings() {
        use QueryWord::{Quoted, Free};

        let mut iter = alphanumeric_quoted_tokens("hello \"world\" coucou \"monde\"");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Quoted("world")));
        assert_eq!(iter.next(), Some(Free("coucou")));
        assert_eq!(iter.next(), Some(Quoted("monde")));
        assert_eq!(iter.next(), None);
    }
}
