use std::str;
use crate::tokenizer::{simple_tokenizer, TokenType};

#[derive(Debug)]
enum State {
    Free,
    Quoted,
}

impl State {
    fn swap(&mut self) {
        match self {
            State::Quoted => *self = State::Free,
            State::Free => *self = State::Quoted,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryToken<'a> {
    Free(&'a str),
    Quoted(&'a str),
}

pub struct QueryTokens<'a> {
    state: State,
    iter: Box<dyn Iterator<Item=(TokenType, &'a str)> + 'a>,
}

impl QueryTokens<'_> {
    pub fn new(query: &str) -> QueryTokens {
        QueryTokens {
            state: State::Free,
            iter: Box::new(simple_tokenizer(query)),
        }
    }
}

impl<'a> Iterator for QueryTokens<'a> {
    type Item = QueryToken<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next()? {
                (TokenType::Other, "\"") => self.state.swap(),
                (TokenType::Word, token) => {
                    let token = match self.state {
                        State::Quoted => QueryToken::Quoted(token),
                        State::Free => QueryToken::Free(token),
                    };
                    return Some(token);
                },
                (_, _) => (),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use QueryToken::{Quoted, Free};

    #[test]
    fn empty() {
        let mut iter = QueryTokens::new("");
        assert_eq!(iter.next(), None);

        let mut iter = QueryTokens::new(" ");
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn one_quoted_string() {
        let mut iter = QueryTokens::new("\"hello\"");
        assert_eq!(iter.next(), Some(Quoted("hello")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn one_pending_quoted_string() {
        let mut iter = QueryTokens::new("\"hello");
        assert_eq!(iter.next(), Some(Quoted("hello")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn one_non_quoted_string() {
        let mut iter = QueryTokens::new("hello");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn quoted_directly_followed_by_free_strings() {
        let mut iter = QueryTokens::new("\"hello\"world");
        assert_eq!(iter.next(), Some(Quoted("hello")));
        assert_eq!(iter.next(), Some(Free("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn free_directly_followed_by_quoted_strings() {
        let mut iter = QueryTokens::new("hello\"world\"");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Quoted("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn free_followed_by_quoted_strings() {
        let mut iter = QueryTokens::new("hello \"world\"");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Quoted("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multiple_spaces_separated_strings() {
        let mut iter = QueryTokens::new("hello    world   ");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Free("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multi_interleaved_quoted_free_strings() {
        let mut iter = QueryTokens::new("hello \"world\" coucou \"monde\"");
        assert_eq!(iter.next(), Some(Free("hello")));
        assert_eq!(iter.next(), Some(Quoted("world")));
        assert_eq!(iter.next(), Some(Free("coucou")));
        assert_eq!(iter.next(), Some(Quoted("monde")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multi_quoted_strings() {
        let mut iter = QueryTokens::new("\"hello world\" coucou \"monde est beau\"");
        assert_eq!(iter.next(), Some(Quoted("hello")));
        assert_eq!(iter.next(), Some(Quoted("world")));
        assert_eq!(iter.next(), Some(Free("coucou")));
        assert_eq!(iter.next(), Some(Quoted("monde")));
        assert_eq!(iter.next(), Some(Quoted("est")));
        assert_eq!(iter.next(), Some(Quoted("beau")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn chinese() {
        let mut iter = QueryTokens::new("汽车男生");
        assert_eq!(iter.next(), Some(Free("汽车")));
        assert_eq!(iter.next(), Some(Free("男生")));
        assert_eq!(iter.next(), None);
    }
}
